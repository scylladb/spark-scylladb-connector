"""Exercise release signing with disposable protected keys and real Git objects."""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import signing


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class SigningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="repo-signing-fixture-")
        cls.addClassCleanup(cls.fixture.cleanup)
        cls.directory = Path(cls.fixture.name)
        cls.keyring = cls.directory / "gnupg"
        cls.keyring.mkdir(mode=0o700)
        cls.addClassCleanup(subprocess.run,
                            ["gpgconf", "--homedir", str(cls.keyring), "--kill", "gpg-agent"],
                            capture_output=True, check=True, timeout=60)
        cls.password = "test-only-release-passphrase"
        def gpg(*args):
            result = subprocess.run(
                ["gpg", "--no-options", "--homedir", str(cls.keyring), "--batch",
                 "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                input=cls.password + "\n", capture_output=True, text=True, check=True, timeout=60,
            )
            return result.stdout
        gpg("--quick-generate-key", "Release Test <release@example.invalid>", "rsa2048", "cert", "1d")
        cls.primary = [line.split(":")[9] for line in
                       gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][0]
        gpg("--quick-add-key", cls.primary, "rsa2048", "sign", "1d")
        cls.subkey = [line.split(":")[9] for line in
                      gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][1]
        cls.public = cls.directory / "public.asc"
        cls.public.write_text(gpg("--armor", "--export", cls.primary))
        cls.private = cls.directory / "subkey.asc"
        cls.private.write_text(gpg("--armor", "--export-secret-subkeys", cls.primary))
        cls.full = cls.directory / "full.asc"
        cls.full.write_text(gpg("--armor", "--export-secret-keys", cls.primary))

    @contextmanager
    def repository(self, full=False, password=None):
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="repo-signing-test-") as temp:
            directory = Path(temp)
            keyring = directory / "gnupg"
            keyring.mkdir(mode=0o700)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "RELEASE_GPG_", "RELEASE_SIGNING_"))
                   and key not in ("MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE")}
            env.update(GNUPGHOME=str(keyring), RUNNER_TEMP=str(directory),
                       GITHUB_ENV=str(directory / "github-env"), GIT_CONFIG_NOSYSTEM="1",
                       GIT_CONFIG_GLOBAL=str(directory / "initial-gitconfig"),
                       RELEASE_GPG_PASSPHRASE=self.password if password is None else password)
            (directory / "initial-gitconfig").write_text("")
            try:
                subprocess.run(["gpg", "--no-options", "--homedir", str(keyring), "--batch",
                                "--pinentry-mode", "loopback", "--passphrase-fd", "0", "--import",
                                str(self.full if full else self.private)], input=self.password + "\n",
                               capture_output=True, text=True, env=env, check=True, timeout=60)
                with patch.dict(os.environ, env, clear=True), \
                        patch.multiple(signing, PRIMARY=self.primary, SIGNING=self.subkey,
                                       PUBLIC_KEY=self.public):
                    repo = directory / "source"
                    signing.run(["git", "init", "--quiet", str(repo)])
                    os.chdir(repo)
                    signing.run(["git", "-c", "user.name=Initial", "-c",
                                 "user.email=initial@example.invalid", "-c", "commit.gpgsign=false",
                                 "commit", "--quiet", "--allow-empty", "-m", "Initial"])
                    try:
                        yield directory
                    finally:
                        signing.cleanup()
            finally:
                os.chdir(original)
                subprocess.run(["gpgconf", "--homedir", str(keyring), "--kill", "gpg-agent"],
                               capture_output=True, check=True, timeout=60)

    def configured(self):
        values = signing.configure()
        os.environ.update(values)
        self.assertNotIn("RELEASE_GPG_PASSPHRASE_FILE", values)
        self.assertFalse((Path(values["RELEASE_SIGNING_DIR"]) / "passphrase").exists())
        self.assertNotIn(self.password, Path(os.environ["GITHUB_ENV"]).read_text())
        return values

    def release(self):
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
        # make release-prepare relies on tag.gpgSign for its annotated tag.
        signing.run(["git", "tag", "-a", "-m", "Release 1.0", "1.0"])
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Next development"])

    def test_signed_release_and_unchanged_checkout_config(self):
        with self.repository() as directory:
            config = Path(signing.run(["git", "rev-parse", "--git-path", "config"]).strip())
            before = config.read_bytes()
            values = self.configured()
            self.assertNotIn("RELEASE_MAVEN_OPTS", values)
            self.release()
            signing.verify_release("1.0")
            self.assertEqual(config.read_bytes(), before)
            signing.cleanup()
            self.assertFalse(Path(values["RELEASE_SIGNING_DIR"]).exists())

    def test_empty_passphrase_fails_before_configuration(self):
        with self.repository(password="") as directory:
            with self.assertRaisesRegex(signing.SigningError, "nonempty"):
                signing.configure()
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_local_git_settings_cannot_disable_release_signing(self):
        with self.repository():
            for key, value in (("commit.gpgsign", "false"), ("tag.gpgSign", "false"),
                               ("user.signingkey", "0" * 40), ("gpg.program", "false")):
                signing.run(["git", "config", "--local", key, value])
            config = Path(signing.run(["git", "rev-parse", "--git-path", "config"]).strip())
            before = config.read_bytes()
            os.environ.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="remote.example.url",
                              GIT_CONFIG_VALUE_0="https://example.invalid/repo")
            self.configured()
            self.release()
            signing.verify_release("1.0")
            self.assertEqual(signing.run(["git", "config", "remote.example.url"]).strip(),
                             "https://example.invalid/repo")
            self.assertEqual(config.read_bytes(), before)

    def test_wrong_passphrase_fails_with_fresh_agent(self):
        with self.repository(password="wrong-test-passphrase") as directory:
            with self.assertRaises(signing.SigningError):
                signing.configure()
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_primary_secret_rejected(self):
        with self.repository(full=True):
            with self.assertRaisesRegex(signing.SigningError, "signing-private.asc"):
                signing.configure()

    def test_unsigned_release_commit_rejected(self):
        with self.repository():
            self.configured()
            self.release()
            signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                         "--allow-empty", "-m", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0")

    def test_unsigned_release_tag_rejected(self):
        with self.repository():
            self.configured()
            self.release()
            signing.run(["git", "tag", "-d", "1.0"])
            signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m", "Unsigned",
                         "1.0", "HEAD~1"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0")

    def test_tag_at_unrelated_commit_rejected(self):
        with self.repository():
            self.configured()
            signing.run(["git", "tag", "-m", "Old commit", "1.0"])
            signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
            with self.assertRaisesRegex(signing.SigningError, "newly signed"):
                signing.verify_release("1.0")

    def test_public_only_tag_and_commit_verification(self):
        with self.repository():
            self.configured()
            self.release()
            signing.verify_tag("1.0")
            signing.verify_commit("HEAD")
            signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                         "--allow-empty", "-m", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_commit("HEAD")
            with self.assertRaises(signing.SigningError):
                signing.verify_tag("1.0", signing.run(["git", "rev-parse", "HEAD"]).strip())

    def test_detached_signature_reuse_and_tamper_rejection(self):
        with self.repository():
            self.configured()
            artifact = Path("package.tar.gz")
            artifact.write_bytes(b"release package")
            signing.sign_files([str(artifact)])
            signature = Path(str(artifact) + ".asc").read_bytes()
            signing.sign_files([str(artifact)])
            self.assertEqual(Path(str(artifact) + ".asc").read_bytes(), signature)
            artifact.write_bytes(b"changed package")
            with self.assertRaises(signing.SigningError):
                signing.sign_files([str(artifact)])

    def test_import_from_protected_files_and_local_exec_restores_config(self):
        with self.repository() as directory:
            config = Path(signing.run(["git", "rev-parse", "--git-path", "config"]).strip())
            before = config.read_bytes()
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = str(self.private)
            with patch("sys.argv", ["signing.py", "exec", "git", "commit", "--quiet",
                                    "--allow-empty", "-m", "Signed by isolated exec"]):
                self.assertEqual(signing.main(), 0)
            self.assertEqual(config.read_bytes(), before)
            self.assertFalse(list(directory.glob("release-signing-*")))
            signing.verify_commit("HEAD")

    def test_git_wrapper_reads_current_passphrase_after_agent_reset(self):
        with self.repository():
            self.configured()
            signing.run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = "incorrect-fixture-passphrase"
            with self.assertRaises(signing.SigningError):
                signing.run(["git", "commit", "--allow-empty", "-qm", "Must fail"])
            os.environ["RELEASE_GPG_PASSPHRASE"] = self.password
            signing.run(["git", "commit", "--allow-empty", "-qm", "Correct passphrase"])
            signing.verify_commit("HEAD")

    def test_other_signer_and_ambiguous_signatures_rejected(self):
        valid = ("[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Publisher\n"
                 "[GNUPG:] VALIDSIG " + signing.SIGNING
                 + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY)
        signing.verify_status(valid)
        for status in [valid.replace(signing.SIGNING, "0" * 40),
                       valid.replace(signing.PRIMARY, "0" * 40), valid + "\n" + valid,
                       valid.splitlines()[-1]] + [valid + "\n[GNUPG:] " + record for record in
                       ("EXPKEYSIG", "REVKEYSIG", "EXPSIG", "BADSIG", "ERRSIG")]:
            with self.assertRaises(signing.SigningError):
                signing.verify_status(status)

    def test_default_gnupg_home_and_agent_cleanup(self):
        with self.repository() as directory:
            home = os.environ.pop("GNUPGHOME")
            original_run = signing.run
            def run(command, env=None):
                if command == ["gpgconf", "--list-dirs", "homedir"]:
                    return home + "\n"
                return original_run(command, env)
            with patch.object(signing, "run", side_effect=run):
                self.configured()
            self.assertEqual(os.environ["GNUPGHOME"], home)
            with patch.object(signing, "run", wraps=original_run) as calls:
                signing.cleanup()
            calls.assert_any_call(["gpgconf", "--homedir", home, "--kill", "gpg-agent"])

    def test_cleanup_removes_files_when_agent_shutdown_fails(self):
        with self.repository():
            values = self.configured()
            directory = Path(values["RELEASE_SIGNING_DIR"])
            with patch.object(signing, "run", side_effect=signing.SigningError("Shutdown failed")):
                with self.assertRaisesRegex(signing.SigningError, "Shutdown failed"):
                    signing.cleanup()
            self.assertFalse(directory.exists())

    def test_exec_preserves_unrelated_git_config_changes(self):
        with self.repository():
            with patch("sys.argv", ["signing.py", "exec", "git", "config", "--local",
                                    "remote.example.url", "https://example.invalid/repo"]):
                self.assertEqual(signing.main(), 0)
            self.assertEqual(signing.run(["git", "config", "--local", "remote.example.url"]).strip(),
                             "https://example.invalid/repo")

    def test_wrapper_rejects_passphrase_options_before_spawning(self):
        for option in ("--passphrase", "--passphrase-file", "--passphrase-fd=0"):
            with patch.object(signing.subprocess, "run") as spawn:
                with self.assertRaisesRegex(signing.SigningError, "forbidden"):
                    signing.credential_gpg([option, "secret"])
                spawn.assert_not_called()


@unittest.skipUnless(shutil.which("java"), "Java required for the SBT signing-policy check")
class TargetCheckTests(unittest.TestCase):
    def test_effective_settings_reject_inactive_definitions_and_overrides(self):
        root = Path(__file__).resolve().parents[2]
        settings = [
            "Global / PgpKeys.useGpg := true",
            "Global / PgpKeys.pgpSelectPassphrase := None",
            'Global / PgpKeys.pgpSigningKey := Some("' + signing.SIGNING + '!")',
            'Global / PgpKeys.gpgCommand := sys.env.getOrElse("RELEASE_GPG_WRAPPER", "gpg")',
        ]
        source = "\n\n".join(settings)
        project = '\n\nlazy val child = project.in(file("child"))\n'
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="repo-signing-sbt-") as temp:
            directory = Path(temp)
            (directory / "sbt").symlink_to(root / "sbt", target_is_directory=True)
            (directory / "project").mkdir()
            shutil.copyfile(root / "project/build.properties", directory / "project/build.properties")
            (directory / "project/plugins.sbt").write_text(
                'addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")\n')
            try:
                os.chdir(directory)
                Path("build.sbt").write_text(source + project)
                signing.check_target()
                cases = [
                    (source.replace(settings[3], "// " + settings[3]), "gpgCommand"),
                    ('val documentation = """\n' + source + '\n"""', "signing setting"),
                    (source + "\n\nGlobal / PgpKeys.useGpg := false", "useGpg"),
                    (source + '\n\nchild / PgpKeys.pgpSigningKey := Some("wrong")', "pgpSigningKey"),
                    (source.replace(settings[1], ""), "pgpSelectPassphrase"),
                ]
                for build, diagnostic in cases:
                    with self.subTest(diagnostic=diagnostic, build=build):
                        Path("build.sbt").write_text(build + project)
                        with self.assertRaisesRegex(signing.SigningError, diagnostic):
                            signing.check_target()
            finally:
                os.chdir(original)


class FixtureFailureTests(unittest.TestCase):
    def test_failed_setup_and_agent_shutdown_still_remove_fixture(self):
        class FailingFixture(SigningTests):
            def test_fixture_body(self):
                self.fail("Failed class setup must prevent the test body")

        with patch.object(subprocess, "run", side_effect=RuntimeError("Fixture GPG failure")) as run:
            result = unittest.TestResult()
            unittest.TestSuite([FailingFixture("test_fixture_body")]).run(result)
        self.assertEqual(len(result.errors), 2)
        self.assertFalse(FailingFixture.directory.exists())
        self.assertTrue(any(call.args[0][0] == "gpgconf" for call in run.call_args_list))
        self.assertTrue(all(call.kwargs["timeout"] == 60 for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
