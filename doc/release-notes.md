# GitHub release notes

The manual Release workflow creates a GitHub release with generated notes after
Maven publication and the required SCM push succeed. Signing, release-blocker
checks and dry-run behavior still apply. A rerun preserves a published GitHub
release; an existing draft fails visibly so a maintainer can publish or remove
it before retrying. The release tag must exist on GitHub. Historical release
handling and package publication gates are unchanged.

Generated notes exclude PRs carrying `omit-from-release-notes`. Pinned
`actions/labeler` uses the single filename union in `.github/labeler.yml` with
`sync-labels: true`. Mixtures of root documentation, Markdown guides in `doc/`,
module `src/test` and `src/it` files, named test helpers, issue/PR templates and
named CI test/development workflows qualify. Published `test-support/src/main`,
production sources, SBT build/dependency/version files, packaging and release
helpers, release/classification configuration and workflows, and unknown paths
remain visible. Authors, titles, `chore` and `dependencies` labels do not exclude
PRs. A manual omission label can be removed when a PR does not qualify.

Labeling runs on PR creation, updates, reopening, editing and closing, including
merges. Runs for the same PR are serialized, including manual backfills. The
workflow checks out repository tooling at the event revision, never PR head
code. Manual runs must select the repository default branch (`scylla-4.x`); a
precondition rejects other selected refs before checkout. The same independent
step confirms manual targets through the PR API and trusts matching PR events.
Only confirmed PRs can reach label cleanup; ordinary issues and failed target
lookups receive no label writes. Confirmation survives later read or setup
failures. This check documents supported operation; it cannot protect against replacing the workflow itself.

A focused guard reads the same canonical YAML as labeler. It checks a complete
listing of distinct filenames against the reported count, both old and new
rename paths, and stable head/base/count metadata around enumeration. It skips
labeler for production-origin renames, removes the omission label, and verifies
visible state. After labeler, it collects the snapshot again and checks the
actual logical omission label, including case-preserving metadata. Failed or
unexpectedly skipped prerequisites and verification failures attempt independent
label removal without requiring checkout or installed helper dependencies.
Failures remain visible; unrelated labels are preserved. Cleanup is best effort:
API outages, runner termination or cancellation can prevent it. Before/after
reads and workflow serialization do not provide atomicity or race freedom.

The marker controls GitHub release-note presentation. Local Jira add/remove
callers exclude this whole logical label, including case variants, while other
label and PR events retain their routes. Default `GITHUB_TOKEN` labeling does
not trigger those event workflows; manual label changes can. Existing Jira
copies are not cleaned, and other synchronization paths may restore old metadata.

## Before publishing new notes

Publication remains asynchronous. There is no automated release/classifier
synchronization in this change, including later iterations; no follow-up is
scheduled. Before starting a non-dry release:

1. Record the intended `planned_tag` and an existing source endpoint. For the
   normal `scylla-4.x` release, resolve and record the remote branch commit:

   ```bash
   repo=scylladb/spark-scylladb-connector
   comparison_end=$(gh api "repos/$repo/git/ref/heads/scylla-4.x" --jq '.object.sha')
   ```

   The workflow creates the new tag during `release-prepare` and pushes it after
   Maven publication; that future tag is unavailable for these checks. For a
   historical rerun, use its existing remote target tag as `comparison_end`.
   Choose an existing previous release tag as `previous_tag`, then review the
   full GitHub comparison `previous_tag...comparison_end` and associated merged
   PRs, including PRs already carrying the omission label. If there is no prior
   baseline, review the full history through `comparison_end`. Do not identify
   PRs only from a filtered preview. Split truncated comparisons into smaller
   ranges until all relevant changes are accounted for.
2. Backfill every relevant merged PR through **Actions → Classify release notes
   → Run workflow**, selecting the default branch and its `pull-request` input.
   For example:

   ```sh
   gh workflow run label-release-notes.yml --ref scylla-4.x -f pull-request=123
   ```

3. Wait for each run to succeed. Read its final `Verified` log and compare its
   head, base and file count with the current PR metadata. Inspect the changed
   files, including old rename paths, and the actual labels. For example:

   ```sh
   gh api repos/scylladb/spark-scylladb-connector/pulls/123 \
     --jq '{head:.head.sha,base:.base.sha,files:.changed_files,labels:[.labels[].name]}'
   ```

   Confirm support-only PRs have the marker and runtime, mixed or unverified PRs
   do not. A pending/failed run, changed revision or unexpected label requires
   correction and a successful rerun before proceeding. Recheck if anything
   changes after verification.
4. Preview notes against that existing endpoint before the new tag exists. Set
   `planned_tag` to the intended release tag. The [generate-notes API](https://docs.github.com/en/rest/releases/releases#generate-release-notes-content-for-a-release)
   accepts a future tag with `target_commitish` and returns notes without saving
   a tag or release:

   ```bash
   gh api --method POST "repos/$repo/releases/generate-notes" \
     -f "tag_name=$planned_tag" -f "target_commitish=$comparison_end" --jq '.body'
   ```

   For an existing historical tag, GitHub uses that tag and ignores
   `target_commitish`. Leave `previous_tag_name` unspecified, matching the
   Release workflow's automatic baseline. Inspect the preview's full comparison
   baseline as well as its filtered PR list. If GitHub selected a different
   baseline from step 1, repeat the full comparison to `comparison_end` and
   classification checks for that range before previewing again. Preserve
   already published release bodies. Immediately before starting a normal
   release, recheck that the remote `scylla-4.x` commit still equals the recorded
   `comparison_end`; repeat the checks if it moved. Then start the intended
   release with the existing signing, blocker and Maven gates. This manual
   procedure does not lock the branch or synchronize publication with labeling.

If these manual checks are missed, published notes can retain omissions or
unwanted support entries. Published release bodies and curated `CHANGES.txt`
are preserved; filtering applies when GitHub generates notes for a new release.

Run focused checks with `npm ci --ignore-scripts --prefix .github` followed by
`node --test .github/scripts/*.test.cjs`. The controlled API/workflow tests do
not publish a release or write to Jira.
