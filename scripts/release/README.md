# SBT release logs

The release recipes send SBT stdout and stderr through `guard.py`. It buffers
output until SBT exits, then redacts publishing credentials and PGP secrets
before writing either console output or the retained `stdout.log`/`stderr.log`.
Known raw, combined Sonatype, Base64, URL, JSON, XML and shell-escaped forms are
removed. Existing GitHub console masks remain in place.

The shared capture limit is 64 MiB. Overflow discards that invocation's output,
records fixed diagnostics for both streams, and drains SBT until it finishes.
It does not interrupt publication or replace SBT's exit status. Streams left
open by a descendant receive a one-second grace period after SBT exits; complete
streams retain sanitized output, while incomplete ones receive fixed diagnostics.
Cancellation stops the process group and preserves only safe output/diagnostics.
If sanitization or log writing fails after SBT has completed, fixed diagnostics
replace the unavailable output while its known exit status is preserved. Startup
failures still fail the invocation without claiming publication succeeded.

The workflow retains the current helper outside the repository before checking
out a release target. Its pre-credential check requires the selected Makefile's
release and dry-run recipes to use that helper instead of raw capture. Historical
targets without guarded recipes fail before importing PGP credentials. SBT
publication tasks, skip-test behavior, tag preparation and SCM pushes are unchanged.

`python3 -m unittest discover -s scripts/release -p 'test_*.py' -v` exercises capture
with synthetic credentials and fake SBT/git commands. These checks never import a
PGP key, sign an artifact or publish to a registry.
