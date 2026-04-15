# Native Binary Integration Tests

**Status:** Completed

## Overview

The CLI native binary is tested via the Quarkus integration-test infrastructure using
`@QuarkusMainIntegrationTest`. When the `native` Maven profile is active, these tests run
the compiled native executable as a subprocess and assert on its exit code and output.

## Test Layers

### JVM tests (`@QuarkusMainTest`)

All existing `*CommandTest` classes annotated with `@QuarkusMainTest` continue to execute
during the `test` phase against the JVM build of the application. They serve as fast
feedback during development and remain unaffected by this change.

### Native integration tests (`@QuarkusMainIntegrationTest`)

A parallel set of `*CommandIT` classes, annotated with `@QuarkusMainIntegrationTest`, runs
during the `integration-test` phase via the Maven Failsafe plugin. Each IT class mirrors
its `*CommandTest` counterpart:

- It implements the same `*CommandContract` interface, delegating to the same assertions.
- Any additional tests present on the corresponding `*CommandTest` class are also included.
- Non-S3 variants are annotated with `@WithTestResource(QuietLoggingTestResource.class)`.
- S3-backed variants extend `AbstractS3CommandIT` and are annotated with
  `@WithTestResource(S3MockTestResource.class)`.

The test class does not run inside the native binary; it runs in the JVM and uses
`QuarkusMainLauncher` to spawn the native executable as a subprocess. File path arguments
are resolved as classpath resources in the JVM test process and passed as absolute paths,
so they are accessible to the native subprocess via the local filesystem.

`AbstractS3CommandIT` exists separately from `AbstractS3CommandTest` because the two test
layers require fundamentally different AWS configuration mechanisms. `AbstractS3CommandTest`
configures AWS via `System.setProperty()` in a static initializer, which works because
`@QuarkusMainTest` runs the CLI in the same JVM process as the test — system properties
set on the test JVM are immediately visible to the CLI code. `@QuarkusMainIntegrationTest`
runs the native binary as a separate OS process, so `System.setProperty()` in the test JVM
has no effect on it. All configuration must be injected as `-D` flags on the subprocess
command line before it starts, which is only possible through
`QuarkusTestResourceLifecycleManager.start()`. `AbstractS3CommandIT` therefore holds only
the S3 URL constants; container lifecycle and property injection are fully delegated to
`S3MockTestResource`.

## Test Resources

Properties returned by a `QuarkusTestResourceLifecycleManager.start()` are passed by
Quarkus as `-D` system-property flags on the native binary command line. This is the only
mechanism for injecting configuration into the native subprocess from the test layer.

### `QuietLoggingTestResource`

The Quarkus integration-test extension unconditionally adds
`-Dquarkus.log.category."io.quarkus".level=INFO` to the native binary command, which
overrides the `%prod.quarkus.log.level=ERROR` setting baked into the binary and causes
startup/shutdown messages to appear in captured stdout. `QuietLoggingTestResource` returns
`quarkus.log.console.enable=false`, disabling the console log handler at the handler level
so no log output reaches stdout regardless of category levels.

### `S3MockTestResource`

`S3MockTestResource` launches an S3Mock container, uploads all test Parquet files, and
returns a `Map<String, String>` containing `quarkus.log.console.enable=false` (for the
same reason as above) plus the AWS connection properties: `aws.accessKeyId`,
`aws.secretAccessKey`, `aws.region`, `aws.endpointUrl`, `aws.pathStyle`,
`aws.configFile`, and `aws.sharedCredentialsFile`. The AWS SDK embedded in the native
image reads them at runtime, so no code changes are required in the CLI itself.

## Build Changes

The Maven Failsafe plugin is added to the `cli` module's main `<build>` section with the
`integration-test` and `verify` goals. Integration tests are skipped by default
(`skipITs=true`). The existing `native` profile sets `skipITs=false`, enabling the tests
only during a native build.

## Running the Native Build Check Locally

The `native-build-check` CI job can be run locally using [act](https://github.com/nektos/act),
which executes GitHub Actions workflow jobs inside Docker containers.

Install via Homebrew:

```bash
brew install act
```

On macOS, the container needs access to the system CA bundle to trust corporate or
self-signed certificates when pulling dependencies:

```bash
security find-certificate -a -p /Library/Keychains/System.keychain > /tmp/act-certs.pem
security find-certificate -a -p ~/Library/Keychains/login.keychain-db >> /tmp/act-certs.pem
```

Then run the job:

```bash
act pull_request -j native-build-check --matrix os:ubuntu-latest \
  --container-architecture linux/amd64 \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest \
  --container-options "-v /Users/brandonbrown/.m2:/root/.m2 -v /tmp/act-certs.pem:/tmp/act-certs.pem" \
  --env NODE_EXTRA_CA_CERTS=/tmp/act-certs.pem \
  --env TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal
```

- `~/.m2` mount reuses the local Maven cache, avoiding a full dependency re-download on each run.
- `-P ubuntu-latest=catthehacker/ubuntu:act-latest` replaces `act`'s default minimal container image with one that includes Node.js, which is required by JavaScript-based GitHub Actions (`actions/upload-artifact`, `actions/download-artifact`, `actions/setup-java` cleanup). Real GitHub Actions runners include Node.js pre-installed.
- `NODE_EXTRA_CA_CERTS` makes Node.js trust the exported CA bundle.
- `TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal` tells Testcontainers to reach mapped container ports via `host.docker.internal` instead of the Docker bridge IP (`172.17.0.1`), which is unreachable from inside the `act` container on macOS with Docker Desktop.
