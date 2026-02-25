# CI/CD Pipeline

## Overview

The project uses GitHub Actions for continuous integration and release automation. Workflows are defined in `.github/workflows/`.

## Workflows

### Integration Tests (`integration-tests.yml`)

Runs on every push and PR to `scylla-4.x`.

**Jobs:**

1. **Compile** -- Compiles all modules with the target Scala version
2. **Lint** -- Runs scalafix checks (`make lint`)
3. **Test Matrix** -- Runs integration tests across a matrix of database types/versions:
   - ScyllaDB: LATEST, LTS-LATEST
   - Cassandra: 3-LATEST, 4-LATEST, 5-LATEST

Each test configuration installs CCM (Cassandra Cluster Manager), downloads the database image, and runs both unit and integration tests.

**Manual trigger:** The workflow can be triggered manually with custom database and Scala version inputs via the Actions tab.

### Release (`release.yml`)

Manual-only workflow for publishing to Maven Central.

**Steps:**
1. Removes `-SNAPSHOT` from version, creates a git tag
2. Builds and signs the artifact with GPG
3. Publishes to Maven Central via Sonatype
4. Bumps version for next development iteration
5. Pushes tag and version commits

**Options:**
- `dry-run`: Build and sign without publishing to Maven Central
- `skip-tests`: Skip test execution during release
- `target-tag`: Re-release a previously tagged version

## Running Locally

### Prerequisites

- Java 17 (Temurin recommended)
- Python 3.10+ (for CCM)
- CCM installed (`make install-cassandra-ccm` or `make install-scylla-ccm`)

### Common Commands

```bash
make help                          # Show all available targets
make compile                       # Compile all modules
make test-unit                     # Run unit tests
make lint                          # Check code with scalafix
make lint-fix                      # Auto-fix scalafix issues
make test-integration-cassandra    # Run integration tests with Cassandra
make test-integration-scylla       # Run integration tests with ScyllaDB
```

### Version Resolution

The CI uses semantic version aliases (e.g., `LATEST`, `LTS-LATEST`, `4-LATEST`) that resolve to actual versions at build time. You can use these locally too:

```bash
CASSANDRA_VERSION=4-LATEST make test-integration-cassandra
SCYLLA_VERSION=LATEST make test-integration-scylla
```

Or specify exact versions:

```bash
CASSANDRA_VERSION=4.1.7 make test-integration-cassandra
SCYLLA_VERSION=2024.2.1 make test-integration-scylla
```

## Caching

CI uses several cache layers for performance:

| Cache | Key includes | Purpose |
|-------|-------------|---------|
| SBT/Maven | OS, build file hashes | Dependency JARs |
| Compiled classes | OS, commit SHA, build files | Avoid recompilation across jobs |
| CCM repository | OS, database version | Database images for integration tests |

## Debugging CI Failures

1. Check the failing job's logs in the GitHub Actions tab
2. Look for the test report annotations on the PR (via JUnit reports)
3. To reproduce locally, run the same `make` target with matching environment variables
4. For CCM issues, check that the database version resolves correctly: `make resolve-cassandra-version` or `make resolve-scylla-version`
