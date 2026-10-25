# GitHub release notes

The manual Release workflow creates a GitHub release with generated notes after
Maven publication and the required SCM push succeed. Signing, release-blocker
checks and dry-run behavior still apply. A rerun preserves a published GitHub
release; an existing draft fails visibly so a maintainer can publish or remove
it before retrying. The release tag must exist on GitHub. Historical release
handling and package publication gates are unchanged.
