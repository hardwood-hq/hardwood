# Native CLI Build Details

## Building the native CLI

### Prerequisites

A local GraalVM (Java 25+) is required to build a native binary for your own platform. Install via [SDKMAN](https://sdkman.io/):

```bash
sdk install java 25.0.2-graalce
```

### Local build

Build the native binary for the `cli` module and its dependencies:

```bash
./mvnw -Dnative package -pl cli -am
```

The resulting binary is at `cli/target/hardwood-cli`. Run it directly (e.g. `cli/target/hardwood-cli --help`); see the [CLI reference](docs/content/reference/cli.md) for command usage.

### Linux binary without a local GraalVM (containerized)

To build a Linux binary on a non-Linux host — e.g. for a Docker image on macOS — use Quarkus' containerized native build, which runs GraalVM inside the Mandrel builder container:

```bash
./mvnw -Dnative -Dquarkus.native.container-build=true package -pl cli -am
```

This requires Docker to be running. The build always produces a Linux ELF binary; running it directly on macOS fails with `exec format error`.

### Building the Docker image

`cli/build-cli-docker.sh` builds the container image. It produces (or reuses) the full native dist — the Linux binary, completion script, and codec libraries — building it in a container so it targets Linux regardless of the host OS, then builds the image:

```bash
cd cli
./build-cli-docker.sh              # reuse an existing dist, tag :local
./build-cli-docker.sh -f           # force a rebuild of the dist
./build-cli-docker.sh v1.0.0       # custom tag
```

See the [CLI reference](docs/content/reference/cli.md#docker) for running the published image.

### Troubleshooting: missing `error-prone-checks` artifact

The QA profile wires in a build-only annotation-processor module, `dev.hardwood:hardwood-error-prone-checks`. On a clean tree, a native build of `cli` alone can fail with:

```
Could not find artifact dev.hardwood:hardwood-error-prone-checks:jar:1.0.0-SNAPSHOT
```

Build that module alongside the CLI:

```bash
./mvnw -Dnative package -pl cli,error-prone-checks -am
```

## How the native build works

The CLI module uses [Quarkus](https://quarkus.io/) with `quarkus-picocli` and GraalVM/Mandrel native image. Several non-obvious pieces are required to make all compression codecs work correctly in a native binary.

### Compression codec native libraries

All compression codecs (Snappy, ZSTD, LZ4, Brotli) ship their native code as JNI libraries inside their JARs. In a standard JVM application, each library extracts itself from the JAR at runtime via `Class.getResourceAsStream()`. This extraction mechanism does not work in a GraalVM native image.

The solution differs by codec:

- **ZSTD, Snappy, LZ4** — Native libraries are unpacked from their JARs during the Maven `prepare-package` phase (`maven-dependency-plugin`) and bundled in a `lib/` directory alongside the binary. At startup, `NativeImageStartup` fires a Quarkus `StartupEvent` which calls `NativeLibraryLoader` to load each library via `System.load(absolutePath)` before any decompression occurs. For ZSTD, `zstd-jni`'s `Native.assumeLoaded()` is also called to prevent the library's own loader from attempting a duplicate load. Snappy is handled the same way — its loader may have already run at image build time (and failed), so directly calling `System.load()` at runtime bypasses its cached failure state entirely.

- **Brotli** — `brotli4j`'s loader (`Brotli4jLoader.ensureAvailability()`) is invoked explicitly at decompression time rather than in a static initializer, so it never runs at build time. Its loading strategy — extracting a classpath resource to a temp file and loading that — works in native images provided the resource is embedded in the binary. The `resource-config.json` under `cli/src/main/resources/META-INF/native-image/` instructs GraalVM to embed the brotli native libraries as image resources.

- **libdeflate (GZIP acceleration)** — libdeflate uses the Java 22+ Foreign Function & Memory (FFM) API, which relies on runtime downcall handles that cannot be created inside a native image. `LibdeflateLoader` detects the native image context via the `org.graalvm.nativeimage.imagecode` system property and returns `isAvailable() = false`, dead-code-eliminating the entire FFM path. The `--initialize-at-build-time` directive in `core`'s `native-image.properties` ensures GraalVM constant-folds this check at image build time.

### Build arguments (application.properties)

| Argument | Reason |
|---|---|
| `-march=compatibility` | Produces a binary targeting a generic x86\_64/arm64 baseline rather than the build machine's specific CPU generation. Without this, the binary may crash with `SIGILL` on older hardware. |
| `--gc=serial` | Replaces the default G1 garbage collector with the serial GC, removing GC infrastructure code from the binary. Appropriate for a short-lived CLI process and meaningfully reduces binary size. |
| `-J--enable-native-access=ALL-UNNAMED` | Passed to the JVM _running the Mandrel build process_ (not the native image itself). Required because GraalVM's image builder uses native access internally on JDK 21+. |
| `--initialize-at-run-time=...YamlConfiguration` | Prevents log4j's YAML configuration class from initializing at image build time, where it would attempt to load SnakeYAML and fail. |

### Logging dependencies

`netty-buffer` (an optional dependency of `brotli4j`) is declared explicitly at compile scope so that GraalVM can resolve the `ByteBufUtil` reference in `brotli4j`'s `DirectDecompress` class during image analysis.

## Testing the native binary

Automated coverage of the native binary is provided by the Quarkus integration-test infrastructure; see [_designs/NATIVE_INTEGRATION_TESTS.md](_designs/NATIVE_INTEGRATION_TESTS.md). The ITs run against the compiled native executable during `./mvnw -Pnative -pl cli verify`.

For ad-hoc manual testing of the native binary against S3, see the [Manual S3 testing](TESTING.md#manual-s3-testing) recipe in [TESTING.md](TESTING.md).
