import base64
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import urllib.parse


ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "scripts/release/guard.py"
SPEC = importlib.util.spec_from_file_location("sbt_release_guard", HELPER)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

SYNTHETIC = {
    "SONATYPE_USERNAME": "synthetic-publisher-user",
    "SONATYPE_PASSWORD": 'synthetic-"password\\line\nΩ/end',
    "PGP_PASSPHRASE": "synthetic-signing-passphrase",
    "PGP_SECRET": "DUMMY-KEY-START\nsynthetic-private-material\nDUMMY-KEY-END",
}


class Stream:
    def __init__(self):
        self.buffer = io.BytesIO()

    def flush(self):
        pass


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="spark-release-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.logs = self.directory / "logs"

    def environment(self):
        # Never forward host publishing credentials into a fake process.
        return {"PATH": os.defpath, **SYNTHETIC, "RELEASE_LOG_DIR": str(self.logs)}

    def capture(self, script, status=0):
        out, err = Stream(), Stream()
        with mock.patch.object(guard.sys, "stdout", out), mock.patch.object(guard.sys, "stderr", err):
            result = guard.capture(
                [sys.executable, "-c", script + f"\nraise SystemExit({status})"],
                self.environment(), guard.secret_forms(self.environment()), self.logs,
            )
        return result, out.buffer.getvalue(), err.buffer.getvalue()

    def test_artifacts_and_console_redact_raw_json_unicode_and_basic_credentials(self):
        script = """
import base64, json, os, sys, urllib.parse
password = os.environ['SONATYPE_PASSWORD']
forms = [password, json.dumps(password), json.dumps(password, ensure_ascii=False),
         urllib.parse.quote(password, safe=''),
         base64.b64encode((os.environ['SONATYPE_USERNAME'] + ':' + password).encode()).decode(),
         os.environ['SONATYPE_USERNAME'], os.environ['PGP_PASSPHRASE'], os.environ['PGP_SECRET']]
print('before stdout')
for value in forms: print('stdout ' + value)
print('after stdout')
print('before stderr', file=sys.stderr)
for value in forms: print('stderr ' + value, file=sys.stderr)
print('after stderr', file=sys.stderr)
"""
        status, stdout, stderr = self.capture(script)
        self.assertEqual(0, status)
        self.assertEqual(stdout, (self.logs / "stdout.log").read_bytes())
        self.assertEqual(stderr, (self.logs / "stderr.log").read_bytes())
        self.assertIn(b"before stdout", stdout)
        self.assertIn(b"after stderr", stderr)
        for stream in (stdout, stderr):
            self.assertIn(b"[REDACTED]", stream)
            for value in SYNTHETIC.values():
                self.assertNotIn(value.encode(), stream)
            password = SYNTHETIC["SONATYPE_PASSWORD"]
            self.assertNotIn(json.dumps(password).encode(), stream)
            self.assertNotIn(json.dumps(password, ensure_ascii=False).encode(), stream)
            self.assertNotIn(urllib.parse.quote(password, safe="").encode(), stream)
            combined = SYNTHETIC["SONATYPE_USERNAME"] + ":" + password
            self.assertNotIn(base64.b64encode(combined.encode()), stream)

    def test_split_chunks_on_both_streams_are_buffered_until_complete(self):
        script = """
import os, time
for fd, key in ((1, 'SONATYPE_PASSWORD'), (2, 'PGP_PASSPHRASE')):
    value = os.environ[key].encode()
    os.write(fd, value[:8])
    time.sleep(.03)
    os.write(fd, value[8:] + b'\\n')
"""
        status, stdout, stderr = self.capture(script)
        self.assertEqual(0, status)
        self.assertEqual(b"[REDACTED]\n", stdout)
        self.assertEqual(b"[REDACTED]\n", stderr)

    def test_unknown_authorization_and_credential_properties_are_redacted(self):
        raw = b"Authorization: Basic not-in-environment\n-Dpgp.password=unknown-value\n"
        self.assertEqual(b"Authorization: [REDACTED]\n-Dpgp.password=[REDACTED]\n", guard.redact(raw, []))

    def test_numeric_xml_entities_and_url_encoded_xml_are_redacted(self):
        forms = guard.secret_forms({"TOKEN": "alpha&beta<tail"})
        for value in ("alpha&#38;beta&#60;tail", "alpha&#x26;beta&#x3C;tail"):
            for encoded in (value, urllib.parse.quote(value, safe="")):
                with self.subTest(encoded=encoded):
                    self.assertEqual(b"[REDACTED]", guard.redact(encoded.encode(), forms))

    def test_factored_literals_preserve_overlap_prefixes_and_percent_case(self):
        forms = [b"aba", b"abab", b"bab", b"a%2Fb"]
        self.assertEqual(b"[REDACTED]", guard.redact(b"ababa", forms))
        self.assertEqual(b"[REDACTED] ABA", guard.redact(b"a%2fb ABA", forms))
        self.assertEqual(b"[REDACTED]", guard.redact(b"a%2fbc", [b"a%2Fb", b"a%2fbc"]))
        credentials = guard.secret_forms({"SONATYPE_USERNAME": "user%2F", "SONATYPE_PASSWORD": "USER/tail"})
        self.assertEqual(b"[REDACTED]", guard.redact(b"USER%2Ftail", credentials))
        self.assertEqual(b"[REDACTED]", guard.redact(b"a" * 2000, [b"a" * 2000]))

    def test_completed_status_is_preserved_on_success_and_failure(self):
        for code in (0, 7, 19):
            with self.subTest(status=code):
                result, _, _ = self.capture("print('completed')", status=code)
                self.assertEqual(code, result)

    def test_multiple_invocations_append_sanitized_streams(self):
        self.capture("print('first phase')")
        self.capture("print('second phase')")
        self.assertEqual(b"first phase\nsecond phase\n", (self.logs / "stdout.log").read_bytes())

    def test_overflow_withholds_every_stream_and_process_finishes_normally(self):
        marker = self.directory / "finished"
        script = f"""
import os
from pathlib import Path
os.write(1, b'A' * 10000)
os.write(2, os.environ['PGP_PASSPHRASE'].encode() + b'B' * 10000)
Path({str(marker)!r}).write_text('finished after overflow')
"""
        with mock.patch.object(guard, "LIMIT", 256):
            status, stdout, stderr = self.capture(script, status=13)
        self.assertEqual(13, status)
        self.assertTrue(marker.exists())
        self.assertEqual(guard.OVERFLOW_LOG, stdout)
        self.assertEqual(guard.OVERFLOW_LOG, stderr)
        self.assertEqual(stdout, (self.logs / "stdout.log").read_bytes())
        self.assertEqual(stderr, (self.logs / "stderr.log").read_bytes())

    def test_redaction_failure_after_exit_withholds_output_and_preserves_status(self):
        for code in (0, 11):
            with self.subTest(status=code):
                with mock.patch.object(guard, "redact", side_effect=ValueError(SYNTHETIC["SONATYPE_PASSWORD"])):
                    status, stdout, stderr = self.capture("print('output to withhold')", status=code)
                self.assertEqual(code, status)
                self.assertEqual(guard.FAILED_LOG, stdout)
                self.assertEqual(guard.FAILED_LOG, stderr)
                self.assertNotIn(SYNTHETIC["SONATYPE_PASSWORD"].encode(), stdout + stderr)

    def test_log_write_failure_does_not_replace_completed_status(self):
        self.logs.write_text("not a directory")
        status, stdout, stderr = self.capture("print('safe captured text')", status=23)
        self.assertEqual(23, status)
        self.assertIn(b"safe captured text", stdout)
        self.assertNotIn(SYNTHETIC["SONATYPE_PASSWORD"].encode(), stdout + stderr)

    def test_selector_failure_after_exit_preserves_completed_status(self):
        start = subprocess.Popen

        def completed_process(*args, **kwargs):
            process = start(*args, **kwargs)
            process.wait(timeout=5)
            return process

        for code in (0, 11):
            with self.subTest(status=code):
                with mock.patch.object(guard.subprocess, "Popen", side_effect=completed_process), \
                     mock.patch.object(guard.selectors.DefaultSelector, "select", side_effect=OSError("read failure")):
                    status, stdout, stderr = self.capture("print('output to withhold')", status=code)
                self.assertEqual(code, status)
                self.assertEqual(guard.FAILED_LOG, stdout)
                self.assertEqual(guard.FAILED_LOG, stderr)

    def test_unexpected_post_exit_writer_failure_still_preserves_status(self):
        with mock.patch.object(guard, "write_logs", side_effect=RuntimeError(SYNTHETIC["SONATYPE_PASSWORD"])):
            status, stdout, stderr = self.capture("print('safe')", status=17)
        self.assertEqual(17, status)
        self.assertEqual(b"", stdout + stderr)

    def test_inherited_pipe_grace_retains_complete_stderr_and_withholds_stdout(self):
        pid_file = self.directory / "group"
        script = f"""
import os, subprocess, sys
from pathlib import Path
Path({str(pid_file)!r}).write_text(str(os.getpid()))
subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'], stderr=subprocess.DEVNULL)
os.write(1, os.environ['PGP_PASSPHRASE'].encode()[:8])
os.write(2, b'complete stderr\\n')
"""
        try:
            started = time.monotonic()
            status, stdout, stderr = self.capture(script, status=3)
            self.assertLess(time.monotonic() - started, 4)
            self.assertEqual(3, status)
            self.assertEqual(guard.INCOMPLETE_LOG, stdout)
            self.assertEqual(b"complete stderr\n", stderr)
        finally:
            if pid_file.exists():
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(int(pid_file.read_text()), signal.SIGKILL)

    def test_cancellation_stops_process_group_and_retains_safe_diagnostics(self):
        pids = self.directory / "pids"
        script = f"""
import os, subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)'])
Path({str(pids)!r}).write_text(str(os.getpid()) + ',' + str(child.pid))
os.write(1, os.environ['PGP_PASSPHRASE'].encode()[:8])
time.sleep(30)
"""
        wrapper = subprocess.Popen(
            [sys.executable, str(HELPER), "run", sys.executable, "-c", script],
            env=self.environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 5
            while not pids.exists() and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(pids.exists())
            # Ensure the descendant has installed its ignore-SIGTERM handler.
            time.sleep(.1)
            wrapper.send_signal(signal.SIGTERM)
            stdout, stderr = wrapper.communicate(timeout=5)
            self.assertEqual(143, wrapper.returncode)
            self.assertEqual(guard.CANCELLED_LOG, stdout)
            self.assertEqual(guard.CANCELLED_LOG, stderr)
            self.assertEqual(stdout, (self.logs / "stdout.log").read_bytes())
            parent, child = map(int, pids.read_text().split(','))
            for pid in (parent, child):
                stat = Path(f"/proc/{pid}/stat")
                if stat.exists():
                    self.assertEqual("Z", stat.read_text().split()[2])
        finally:
            if wrapper.poll() is None:
                wrapper.kill()
                wrapper.communicate()
            if pids.exists():
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(int(pids.read_text().split(',')[0]), signal.SIGKILL)

    def test_startup_failure_is_closed_and_does_not_echo_exception_values(self):
        missing = self.directory / SYNTHETIC["PGP_PASSPHRASE"]
        result = subprocess.run(
            [sys.executable, str(HELPER), "run", str(missing)], env=self.environment(),
            capture_output=True, timeout=5,
        )
        self.assertEqual(2, result.returncode)
        self.assertNotIn(SYNTHETIC["PGP_PASSPHRASE"].encode(), result.stdout + result.stderr)


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="spark-release-integration-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        self.logs = self.directory / "retained logs"
        self.trace = self.directory / "trace.jsonl"

    def executable(self, name, source):
        path = self.bin / name
        path.write_text(f"#!{sys.executable}\n" + source)
        path.chmod(0o755)
        return path

    def environment(self):
        return {
            "PATH": str(self.bin) + os.pathsep + os.defpath,
            "RELEASE_LOG_DIR": str(self.logs), "RELEASE_GUARD": str(HELPER),
            "FAKE_TRACE": str(self.trace), **SYNTHETIC,
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
        }

    def fake_commands(self):
        self.executable("java", "import sys; print('openjdk version \"17.0.1\"', file=sys.stderr)\n")
        self.executable("git", """
import json, os, sys
with open(os.environ['FAKE_TRACE'], 'a') as stream: stream.write(json.dumps(['git', *sys.argv[1:]]) + '\\n')
if sys.argv[1:2] == ['describe']: print('v9.9.9')
""")
        return self.executable("fake sbt", """
import json, os, sys
with open(os.environ['FAKE_TRACE'], 'a') as stream: stream.write(json.dumps(['sbt', *sys.argv[1:]]) + '\\n')
print('stdout credential: ' + os.environ['SONATYPE_PASSWORD'])
print('stderr passphrase: ' + os.environ['PGP_PASSPHRASE'], file=sys.stderr)
raise SystemExit(int(os.environ.get('FAKE_SBT_STATUS', '0')))
""")

    def test_actual_make_recipes_keep_publication_tasks_and_literal_skip_test_argument(self):
        sbt = self.fake_commands()
        for target, skip in (("release", ""), ("release-dry-run", "true"), ("release", "1")):
            with self.subTest(target=target, skip=skip):
                self.trace.write_text("")
                result = subprocess.run(
                    ["make", "-f", str(ROOT / "Makefile"), target, f"SBT_BIN={sbt}", f"RELEASE_SKIP_TESTS={skip}"],
                    cwd=self.directory, env=self.environment(), capture_output=True, timeout=10,
                )
                self.assertEqual(0, result.returncode, result.stderr.decode())
                trace = [json.loads(line) for line in self.trace.read_text().splitlines()]
                self.assertIn(["git", "checkout", "v9.9.9"], trace)
                args = ["+publishSigned", "sonaUpload"]
                if target == "release":
                    args.append("sonaRelease")
                if skip:
                    args.insert(0, "set ThisBuild / test := {}")
                self.assertEqual(["sbt", *args], trace[-1])
                for captured in (result.stdout, result.stderr, (self.logs / "stdout.log").read_bytes(), (self.logs / "stderr.log").read_bytes()):
                    self.assertNotIn(SYNTHETIC["SONATYPE_PASSWORD"].encode(), captured)
                    self.assertNotIn(SYNTHETIC["PGP_PASSPHRASE"].encode(), captured)

    def test_make_failure_still_stops_the_workflow_after_sbt_failure(self):
        sbt = self.fake_commands()
        env = self.environment()
        env["FAKE_SBT_STATUS"] = "8"
        result = subprocess.run(
            ["make", "-f", str(ROOT / "Makefile"), "release", f"SBT_BIN={sbt}"],
            cwd=self.directory, env=env, capture_output=True, timeout=10,
        )
        self.assertEqual(2, result.returncode)  # GNU Make preserves failure semantics.
        self.assertIn(b"Error 8", result.stderr)

    def test_make_recipes_accept_sbt_command_options(self):
        sbt = self.fake_commands()
        for target in ("release", "release-dry-run"):
            with self.subTest(target=target):
                self.trace.write_text("")
                result = subprocess.run(
                    ["make", "-f", str(ROOT / "Makefile"), target,
                     f"SBT_BIN={shlex.quote(str(sbt))} -batch", "RELEASE_SKIP_TESTS=true"],
                    cwd=self.directory, env=self.environment(), capture_output=True, timeout=10,
                )
                self.assertEqual(0, result.returncode, result.stderr.decode())
                trace = [json.loads(line) for line in self.trace.read_text().splitlines()]
                args = ["-batch", "set ThisBuild / test := {}", "+publishSigned", "sonaUpload"]
                if target == "release":
                    args.append("sonaRelease")
                self.assertEqual(["sbt", *args], trace[-1])

    def test_policy_accepts_guarded_sbt_without_maven_pom_or_gpg_properties(self):
        selected = self.directory / "selected"
        selected.mkdir()
        (selected / "Makefile").write_bytes((ROOT / "Makefile").read_bytes())
        guard.check_policy(selected)

    def test_policy_rejects_old_raw_capture_or_rebound_helper(self):
        selected = self.directory / "selected"
        selected.mkdir()
        original = (ROOT / "Makefile").read_text()
        for unsafe in (
            original.replace('python3 "$(RELEASE_GUARD)" run "$(SBT_BIN)"', '"$(SBT_BIN)"'),
            original.replace('"$${SBT_ARGS[@]}"', '"$${SBT_ARGS[@]}" > >(tee raw.log)'),
            original.replace("RELEASE_GUARD ?=", "RELEASE_GUARD :="),
        ):
            with self.subTest(target=unsafe[:20]):
                (selected / "Makefile").write_text(unsafe)
                with self.assertRaises(guard.Unsafe):
                    guard.check_policy(selected)

    def test_workflow_preserves_helper_before_checkouts_and_checks_before_credentials(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        preserve = workflow.index("name: Preserve release capture helper")
        target_checkout = workflow.index("name: Checkout Code One Commit Before")
        branch_checkout = workflow.index("name: Checkout release branch")
        check = workflow.index("name: Check selected release log capture")
        credentials = workflow.index("gpg-private-key:")
        self.assertLess(preserve, target_checkout)
        self.assertLess(branch_checkout, check)
        self.assertLess(check, credentials)
        self.assertIn('cp scripts/release/guard.py "$guard_dir/guard.py"', workflow)
        self.assertIn('run: python3 "$RELEASE_GUARD" check', workflow)
        self.assertIn("if: always()", workflow)
        self.assertIn("path: /tmp/spark-connector-release-logs/*.log", workflow)
        self.assertEqual(2, workflow.count('echo "::add-mask::${SONATYPE_PASSWORD}"'))
        self.assertIn("git push origin scylla-4.x --follow-tags -v", workflow)

    def test_retained_helper_survives_real_target_checkout_and_rejects_legacy_recipe(self):
        checkout = self.directory / "checkout"
        checkout.mkdir()
        env = self.environment()
        # This fixture uses real local Git with no host config or remote; no release is run.
        git = "/usr/bin/git"
        subprocess.run([git, "init", "-q", str(checkout)], env=env, check=True)
        subprocess.run([git, "-C", str(checkout), "config", "user.name", "Release Fixture"], env=env, check=True)
        subprocess.run([git, "-C", str(checkout), "config", "user.email", "fixture@example.test"], env=env, check=True)
        (checkout / "Makefile").write_text("release:\n\t./sbt/sbt > >(tee raw.log)\nrelease-dry-run:\n\t./sbt/sbt > >(tee raw.log)\n")
        subprocess.run([git, "-C", str(checkout), "add", "Makefile"], env=env, check=True)
        subprocess.run([git, "-C", str(checkout), "commit", "-qm", "legacy target"], env=env, check=True)
        subprocess.run([git, "-C", str(checkout), "tag", "legacy"], env=env, check=True)
        (checkout / "Makefile").write_bytes((ROOT / "Makefile").read_bytes())
        folder = checkout / "scripts/release"
        folder.mkdir(parents=True)
        (folder / "guard.py").write_bytes(HELPER.read_bytes())
        subprocess.run([git, "-C", str(checkout), "add", "."], env=env, check=True)
        subprocess.run([git, "-C", str(checkout), "commit", "-qm", "guarded workflow checkout"], env=env, check=True)
        retained = self.directory / "current retained guard.py"
        retained.write_bytes(HELPER.read_bytes())
        subprocess.run([git, "-C", str(checkout), "checkout", "-q", "legacy"], env=env, check=True)
        self.assertFalse((folder / "guard.py").exists())
        result = subprocess.run([sys.executable, str(retained), "check"], cwd=checkout, env=env, capture_output=True)
        self.assertEqual(2, result.returncode)
        self.assertIn(b"release recipe", result.stderr)
        subprocess.run([git, "-C", str(checkout), "checkout", "-q", "-"], env=env, check=True)
        # The retained copy remains authoritative even if target helper changes.
        (folder / "guard.py").write_text("raise SystemExit(99)\n")
        result = subprocess.run([sys.executable, str(retained), "check"], cwd=checkout, env=env, capture_output=True)
        self.assertEqual(0, result.returncode)


if __name__ == "__main__":
    unittest.main()
