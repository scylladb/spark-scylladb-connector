#!/usr/bin/env python3
"""Capture SBT release output without putting raw credentials in logs or artifacts.

Output is buffered until SBT exits (64 MiB maximum), then sanitized for both the
console and log artifacts. Overflow is drained without stopping publication or
changing SBT's exit status. Incomplete streams receive fixed diagnostics.
"""

import base64
import heapq
import html
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import signal
import subprocess
import sys
import time
import urllib.parse

LIMIT = 64 * 1024 * 1024
DRAIN_GRACE = 1
OVERFLOW_LOG = b"SBT output exceeded the capture limit; invocation output withheld.\n"
INCOMPLETE_LOG = b"SBT output stream did not close; incomplete output withheld.\n"
CANCELLED_LOG = b"SBT was interrupted; incomplete output withheld.\n"
FAILED_LOG = b"Sanitized SBT output capture failed; invocation output withheld.\n"


class Unsafe(ValueError):
    pass


def check_policy(root):
    """Reject historical targets whose actual release recipes still write raw logs."""
    makefile = (root / "Makefile").read_text()
    for target in ("release", "release-dry-run"):
        match = re.search(r"^" + re.escape(target) + r":[^\n]*\n((?:[\t ][^\n]*\n|\n)+)",
                          makefile, re.M)
        if not match:
            raise Unsafe("release recipe is missing")
        recipe = match[1]
        calls = [line for line in recipe.splitlines() if "$(SBT_BIN)" in line]
        if len(calls) != 1 or not re.search(
            r'python3\s+"\$\(RELEASE_GUARD\)"\s+run\s+"\$\(SBT_BIN\)"', calls[0]
        ):
            raise Unsafe("release recipe must use the retained sanitized capture helper")
        if re.search(r"\btee\b|[12]?>\s*>?", recipe):
            raise Unsafe("release recipe must not write raw stdout or stderr")
        if "eval " in recipe:
            raise Unsafe("release recipe must pass SBT commands as arguments")
    # Environment must retain the workflow-checkout helper rather than rebind it
    # to a script which a target checkout can replace or remove.
    if not re.search(r'^RELEASE_GUARD\s*\?=', makefile, re.M):
        raise Unsafe("release target must preserve the supplied capture helper")
    if re.search(r'^\s*(?:override\s+)?(?:export\s+)?RELEASE_GUARD\s*(?::=|=|\+=)',
                 makefile, re.M):
        raise Unsafe("release target must not replace the supplied capture helper")


def secret_forms(env):
    values = {
        v
        for k, v in env.items()
        if v and re.search(r"PASSWORD|PASSPHRASE|TOKEN|SECRET|CREDENTIALS?|PRIVATE_KEY|AUTHORIZATION", k, re.I)
    }
    for user_key, password_key in (("SONATYPE_USERNAME", "SONATYPE_PASSWORD"),
                                    ("SONATYPE_TOKEN_USERNAME", "SONATYPE_TOKEN_PASSWORD")):
        user, password = env.get(user_key), env.get(password_key)
        if user:
            values.add(user)
        if user and password:
            values.add(user + ":" + password)
    # Include multiline components, escaped newlines, URL and both Base64 alphabets.
    values |= {line for value in values for line in value.splitlines() if line}
    values |= {value.replace("\n", "\\n").replace("\r", "\\r") for value in values}
    forms = set(values)
    for value in values:
        raw = value.encode()
        xml_text = value.replace("&", "&amp;").replace("<", "&lt;")
        forms.update(
            (
                base64.b64encode(raw).decode(),
                base64.urlsafe_b64encode(raw).decode(),
                json.dumps(value),
                json.dumps(value)[1:-1],
                json.dumps(value, ensure_ascii=False),
                json.dumps(value, ensure_ascii=False)[1:-1],
                json.dumps(value).replace("/", "\\/"),
                json.dumps(value)[1:-1].replace("/", "\\/"),
                json.dumps(value, ensure_ascii=False).replace("/", "\\/"),
                json.dumps(value, ensure_ascii=False)[1:-1].replace("/", "\\/"),
                html.escape(value, quote=True),
                shlex.quote(value),
                re.sub(r"([^\w@%+=:,./-])", r"\\\1", value),
                value.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'"),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote_plus(value, safe=""),
                urllib.parse.quote_plus(value, safe="*").replace("~", "%7E"),
            )
        )
        ansi_escapes = {"\a": "\\a", "\b": "\\b", "\x1b": "\\e", "\f": "\\f",
                        "\n": "\\n", "\r": "\\r", "\t": "\\t", "\v": "\\v",
                        "\\": "\\\\", "'": "\\'"}
        ansi = "".join(ansi_escapes.get(character, character) for character in value)
        octal = "".join(
            "\\%03o" % byte if byte < 32 or byte >= 127 else ansi_escapes.get(chr(byte), chr(byte))
            for byte in raw
        )
        for escaped in (ansi, octal):
            forms.update(("$'" + escaped + "'", escaped))
        # XML serializers differ in escaping >, attribute quotes, and whitespace.
        xml_variants = {xml_text}
        for named, entities in (("&amp;", ("&#38;", "&#x26;")),
                                ("&lt;", ("&#60;", "&#x3C;", "&#x3c;"))):
            xml_variants |= {
                variant.replace(named, entity)
                for variant in tuple(xml_variants) for entity in entities
            }
        for character, entities in (
            ("\t", ("&#9;", "&#09;", "&#x9;", "&#x09;")),
            ("\n", ("&#10;", "&#010;", "&#xA;", "&#x0A;")),
            ("\r", ("&#13;", "&#013;", "&#xD;", "&#x0D;")),
        ):
            if character in xml_text:
                xml_variants = {
                    variant.replace(character, entity)
                    for variant in xml_variants
                    for entity in (character, *entities)
                }
        for xml_variant in xml_variants:
            for greater in (">", "&gt;"):
                for double in ('"', "&quot;"):
                    for single in ("'", "&apos;", "&#39;", "&#x27;"):
                        forms.add(
                            xml_variant.replace(">", greater)
                            .replace('"', double)
                            .replace("'", single)
                        )
    for value in tuple(forms):
        forms.add(urllib.parse.quote(value, safe=""))
        forms.add(urllib.parse.quote(value))
        forms.add(value.rstrip("="))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


def literal_pattern(forms):
    """Factor shared prefixes so variants do not each scan the complete log."""
    root = {}
    for value in forms:
        node = root
        for character in value:
            node = node.setdefault(character, {})
        node[None] = {}

    def pattern(node):
        prefix = bytearray()
        while len(node) == 1 and None not in node:
            character, node = next(iter(node.items()))
            prefix.append(character)
        branches = [re.escape(bytes([character])) + pattern(child)
                    for character, child in node.items() if character is not None]
        suffix = b"(?:" + b"|".join(branches) + b")" if branches else b""
        if suffix and None in node:
            suffix += b"?"  # Prefer the longest literal at each starting position.
        return re.escape(bytes(prefix)) + suffix

    return pattern(root)


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    matches = [
        spans(
            rb"(?i)([\"'])((?:-D\s*|--?define[=\s]+)[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"((?:\\[\s\S]|(?!\1)[^\\])*)\1", 3,
        ),
        spans(
            rb"(?i)((?:-D\s*|--?define[=\s]+)[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"(\"(?:\\[\s\S]|[^\"\\])*\"|'(?:\\[\s\S]|[^'\\])*'|[^\s]+)", 2,
        ),
        spans(rb"(?im)((?:proxy-)?authorization[\"']?\s*[:=]\s*)([^\r\n]*)", 2),
    ]
    # Lookahead includes overlapping occurrences; only percent forms ignore case.
    for percent in (False, True):
        values = [value.lower() if percent else value
                  for value in forms if (b"%" in value) == percent]
        if values:
            matches.append(spans(b"(?=(" + literal_pattern(values) + b"))", 1, re.I if percent else 0))
    output = bytearray()
    start = end = 0
    for left, right in heapq.merge(*matches):
        if left > end:
            if end > start:
                output.extend(b"[REDACTED]")
            output.extend(data[end:left])
            start = left
        end = max(end, right)
    if end > start:
        output.extend(b"[REDACTED]")
    output.extend(data[end:])
    return bytes(output)


def write_logs(clean, logs):
    """Write only sanitized content or fixed diagnostics, tolerating broken outputs."""
    output_failed = False
    for name, data, stream in zip(("stdout", "stderr"), clean, (sys.stdout, sys.stderr)):
        try:
            logs.mkdir(parents=True, exist_ok=True)
            with (logs / (name + ".log")).open("ab") as output:
                output.write(data)
        except OSError:
            output_failed = True
        try:
            stream.buffer.write(data)
            stream.flush()
        except OSError:
            output_failed = True
            # Prevent a failed shutdown flush from replacing SBT's exit status.
            try:
                with open(os.devnull, "wb") as sink:
                    os.dup2(sink.fileno(), stream.fileno())
            except OSError:
                pass
    if output_failed:
        try:
            os.write(2, b"Sanitized release output could not be fully written; values withheld.\n")
        except OSError:
            pass


def capture(command, env, forms, logs):
    """Never spool raw output or sanitize a stream cut off before EOF."""
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    )
    buffers = [bytearray(), bytearray()]
    complete = [False, False]
    overflow = False
    logs_started = False
    status = None
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, 0)
            selector.register(process.stderr, selectors.EVENT_READ, 1)
            exited_at = None
            while selector.get_map():
                if process.poll() is not None:
                    if exited_at is None:
                        exited_at = time.monotonic()
                    elif time.monotonic() - exited_at >= DRAIN_GRACE:
                        break
                for key, _ in selector.select(timeout=0.1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        complete[key.data] = True
                        selector.unregister(key.fileobj)
                    elif not overflow:
                        if sum(map(len, buffers)) + len(chunk) > LIMIT:
                            overflow = True
                            buffers = [bytearray(), bytearray()]
                            continue
                        buffers[key.data].extend(chunk)
        status = process.wait()
        # Sanitize both streams completely before opening artifacts or emitting output.
        clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
            redact(bytes(buffer), forms) if ended else INCOMPLETE_LOG
            for buffer, ended in zip(buffers, complete)
        ]
        logs_started = True
        write_logs(clean, logs)
        return status if status >= 0 else 128 - status
    except BaseException as error:
        if status is None and not isinstance(error, (SystemExit, KeyboardInterrupt)):
            status = process.poll()
        if status is not None and not isinstance(error, (SystemExit, KeyboardInterrupt)):
            # Publishing already completed. Withhold output on a sanitization/write
            # failure without turning success into a failed release inviting retries.
            try:
                write_logs([FAILED_LOG, FAILED_LOG], logs)
            except Exception:
                try:
                    os.write(2, FAILED_LOG)
                except OSError:
                    pass
            return status if status >= 0 else 128 - status
        signum = signal.SIGTERM
        if isinstance(error, SystemExit) and isinstance(error.code, int):
            signum = error.code - 128
        if signum not in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            signum = signal.SIGTERM
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=DRAIN_GRACE)
        except subprocess.TimeoutExpired:
            pass
        # The SBT parent may exit before a descendant responds to cancellation.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        clean = [FAILED_LOG, FAILED_LOG]
        if isinstance(error, (SystemExit, KeyboardInterrupt)):
            try:
                clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
                    redact(bytes(buffer), forms) if ended else CANCELLED_LOG
                    for buffer, ended in zip(buffers, complete)
                ]
            except Exception:
                pass
        if not logs_started:
            write_logs(clean, logs)
        raise
    finally:
        process.stdout.close()
        process.stderr.close()


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def main():
    try:
        if sys.argv[1:] == ["check"]:
            check_policy(Path.cwd())
            print("SBT release log safety checks passed.")
            return 0
        if len(sys.argv) < 3 or sys.argv[1] != "run":
            raise Unsafe("expected check or run COMMAND ...")
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        # SBT_BIN historically accepts executable options. Keep literal paths
        # (including spaces) intact; otherwise parse shell quoting without eval.
        command = [sys.argv[2]] if Path(sys.argv[2]).is_file() else shlex.split(sys.argv[2])
        return capture(
            command + sys.argv[3:], os.environ, secret_forms(os.environ),
            Path(os.environ.get("RELEASE_LOG_DIR", "/tmp/spark-connector-release-logs")),
        )
    except Unsafe as error:
        print(f"Release log safety check failed: {error}; values withheld.", file=sys.stderr)
        return 2
    except Exception:
        print("Sanitized SBT release capture failed; values withheld.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
