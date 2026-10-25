const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync, writeFileSync, mkdtempSync, rmSync} = require('node:fs');
const {tmpdir} = require('node:os');
const {join} = require('node:path');
const {spawnSync} = require('node:child_process');

const workflow = readFileSync('.github/workflows/release.yml', 'utf8');
const step = workflow.split('      - name: Create GitHub release\n')[1];
const script = step.split('        run: |\n')[1]
  .split('\n').map(line => line.replace(/^          /, '')).join('\n');

function release(mode) {
  const dir = mkdtempSync(join(tmpdir(), 'spark-release-notes-'));
  try {
    writeFileSync(join(dir, 'git'), '#!/bin/sh\necho v4.1.4\n', {mode: 0o755});
    writeFileSync(join(dir, 'gh'), `#!/bin/sh
printf '%s\\n' "$*" >> "$CALLS"
case "$*" in
  'api repos/scylladb/spark-scylladb-connector/git/ref/tags/v4.1.4')
    [ "$MODE" != missing-tag ] ;;
  'release view v4.1.4 --json isDraft')
    case "$MODE" in
      published) echo '{"isDraft":false}' ;;
      draft) echo '{"isDraft":true}' ;;
      *) exit 1 ;;
    esac ;;
  'release create v4.1.4 --title v4.1.4 --generate-notes --verify-tag')
    [ "$MODE" != create-error ] ;;
  *) exit 99 ;;
esac
`, {mode: 0o755});
    const calls = join(dir, 'calls');
    const result = spawnSync('bash', ['-c', script], {encoding: 'utf8',
      env: {...process.env, PATH: `${dir}:${process.env.PATH}`, MODE: mode,
        CALLS: calls, GITHUB_REPOSITORY: 'scylladb/spark-scylladb-connector'}});
    return {...result, calls: readFileSync(calls, 'utf8')};
  } finally {
    rmSync(dir, {recursive: true, force: true});
  }
}

test('new releases verify the remote tag and generate filtered notes after publishing', () => {
  const result = release('new');
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.calls, /api .*git\/ref\/tags\/v4\.1\.4/);
  assert.match(result.calls, /release create .*--generate-notes --verify-tag/);
  assert.match(step, /if: inputs\.dry-run == false/);
  assert.ok(workflow.indexOf('- name: Perform release\n') < workflow.indexOf('- name: Push changes to SCM\n'));
  assert.ok(workflow.indexOf('- name: Push changes to SCM\n') < workflow.indexOf('- name: Create GitHub release\n'));
  const config = readFileSync('.github/release.yml', 'utf8');
  assert.match(config, /changelog:\n  exclude:\n    labels:\n      - omit-from-release-notes/);
  assert.doesNotMatch(config, /authors:|chore|dependencies/);
});

test('published releases are rerun-safe and preserved', () => {
  const result = release('published');
  assert.equal(result.status, 0, result.stderr);
  assert.doesNotMatch(result.calls, /release create/);
});

test('draft releases, missing authoritative tags and creation failures fail visibly', () => {
  for (const mode of ['draft', 'missing-tag', 'create-error']) {
    const result = release(mode);
    assert.notEqual(result.status, 0, mode);
    if (mode !== 'create-error') assert.doesNotMatch(result.calls, /release create/);
  }
});
