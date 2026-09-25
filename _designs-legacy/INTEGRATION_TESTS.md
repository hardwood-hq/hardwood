# Integration Tests

**Status:** Completed

## Overview

Tests are split across the two Maven test phases by what they need in order to run. A unit
test needs nothing beyond the JVM and a source checkout, and runs in the `test` phase under
Surefire. An integration test needs something the build has to provide first, and runs in
the `integration-test` phase under Failsafe.

The split is carried entirely by the standard Maven naming convention — `*Test` for
Surefire, `*IT` for Failsafe — so the standard flags behave as documented. `-DskipITs`
skips the tests with an external prerequisite and leaves the unit suite intact;
`-DskipTests`, and therefore `-Dquick`, skips both.

## What Counts as an Integration Test

Three prerequisites put a test in the `integration-test` phase:

- **A Docker daemon.** Anything that starts a Testcontainers container, which here means
  anything reaching S3 through the s3proxy container built by `S3ProxyContainers`.
- **A compiled native binary.** The `cli` tests that spawn the GraalVM binary as a
  subprocess.
- **The packaged artifact.** The `core` decompressor ITs, which run against the built JAR
  rather than `target/classes` so that multi-release class selection is exercised the way
  a consumer sees it.

Everything else is a unit test. Reading a fixture from `src/test/resources` is not an
external prerequisite.

Two sets of tests meet one of those prerequisites and are still named `*Test`, because
they are separated by module rather than by phase:

- `hardwood-integration-test` runs against `hardwood-core` resolved as an installed
  artifact, on the Java 21 baseline the JAR claims to support, across a matrix of
  libdeflate and Vector API availability. Nothing else in the reactor builds it and CI
  invokes it on its own, so the phase split has nothing to arbitrate. It needs no daemon
  and no binary, and `-DskipITs` is not meant to skip it.
- `performance-testing/end-to-end` reaches S3 through a container in
  `FlatS3PerformanceTest`. The module builds only under `-Pperformance-test`, and
  `performance.yml` picks the benchmarks to run with `-Dtest=` filters, which Surefire
  honours and Failsafe does not. Under that profile `-DskipITs` therefore leaves that one
  container-backed test running.

## Build Wiring

Failsafe's `integration-test` and `verify` goals are bound once, in the parent POM, so
every module runs its ITs without repeating the binding. Modules needing more than the
defaults contribute only a `<configuration>`: `core` points `classesDirectory` at the built
JAR, and `cli` passes `native.image.path` so the native ITs can locate the binary.

The native binary is the one prerequisite an ordinary build cannot satisfy, since only the
`native` profile produces it. Those ITs carry the JUnit tag `native`, which the
`failsafe.excludedGroups` property excludes by default; the `native` profile clears that
property, and does nothing else.

The tag therefore only ever withholds tests, and never narrows a run. `./mvnw verify` runs
every IT that can run without a binary, and `./mvnw -Dnative verify` produces the binary and
runs every IT there is, so adding `-Dnative` never subtracts coverage. Gating this way also
leaves `-DskipITs` free to mean what Maven says it means, and ties the native ITs to the
condition that actually governs them rather than to a property a caller might pass for an
unrelated reason. A build wanting a narrower slice asks for it directly, with
`-Dfailsafe.excludedGroups=native` or `-Dit.test=...`.

## The Native Test Layer

The JVM `*CommandTest` and `*S3CommandIT` classes carry the primary behavioural coverage for
every CLI command. The three native ITs deliberately do not repeat it. They exist to catch
what only appears once the image is built — missing reflection registrations, unreachable
classpath resources, broken AWS SDK wiring, codec native libraries that fail to load:

- `NativeBinarySmokeIT` runs the binary against a local Parquet file and pins its reported
  build version against the JVM it was compiled from.
- `NativeBinaryS3SmokeIT` runs it against a file served by an S3 proxy container.
- `NativeCompressionCodecIT` reads one fixture per supported compression codec, covering
  `LZ4` and `LZ4_RAW` separately because they use different decompressors.

The ITs themselves run on the JVM and drive the binary through `ProcessBuilder`, which makes
configuration one-directional: file arguments are resolved as classpath resources and passed
as absolute paths, and settings reach the subprocess as environment variables. The JVM S3
tests configure AWS through `System.setProperty()` instead, which works only because they
run the CLI in the test's own JVM.

## Running the Native Build Check Locally

The `native-build-check` CI job can be run with [act](https://github.com/nektos/act)
(`brew install act`), which executes workflow jobs in Docker:

```bash
act pull_request -j native-build-check \
  --container-architecture linux/amd64 \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest \
  --container-options "-v $HOME/.m2:/root/.m2 -v /tmp/act-certs.pem:/tmp/act-certs.pem" \
  --env NODE_EXTRA_CA_CERTS=/tmp/act-certs.pem \
  --env TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal
```

- `catthehacker/ubuntu:act-latest` replaces `act`'s default minimal image with one that has
  the Node.js that JavaScript-based actions need; real runners ship it pre-installed.
- `TESTCONTAINERS_HOST_OVERRIDE` makes Testcontainers reach mapped ports via
  `host.docker.internal` rather than the Docker bridge IP, which is unreachable from inside
  the `act` container on macOS.
- Mounting `~/.m2` reuses the local Maven cache.

On macOS the container also needs the system CA bundle to trust corporate or self-signed
certificates, exported beforehand to the path mounted above:

```bash
security find-certificate -a -p /Library/Keychains/System.keychain > /tmp/act-certs.pem
security find-certificate -a -p ~/Library/Keychains/login.keychain-db >> /tmp/act-certs.pem
```
