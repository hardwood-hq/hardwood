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

- **ZSTD, Snappy, LZ4** — Native libraries are unpacked from their JARs during the Maven `prepare-package` phase (`maven-dependency-plugin`) and embedded as GraalVM image resources in `target/classes/native/{os}-{arch}/`. At startup, `NativeImageStartup` fires a Quarkus `StartupEvent` which calls `NativeLibraryLoader` to load each library via `System.load(absolutePath)` before any decompression occurs. For ZSTD, `zstd-jni`'s `Native.assumeLoaded()` is also called to prevent the library's own loader from attempting a duplicate load. Snappy is handled the same way — its loader may have already run at image build time (and failed), so directly calling `System.load()` at runtime bypasses its cached failure state entirely.

  #### Embedded libraries (native binary only)

  In the native binary, zstd, snappy, and lz4 native libraries are embedded as
  GraalVM image resources and extracted to a flat cache directly in
  `java.io.tmpdir` on first run. The external `lib/` directory (via
  `HARDWOOD_LIB_PATH` or next to the executable) is the primary loading path. If
  unavailable, embedded resources are extracted from the binary as a fallback.

  **Cache location:** files are placed directly in `java.io.tmpdir`, falling back
  to `~/.hardwood/`. Each cache file is named
  `hardwood-<version>-<shortHash>-<library><ext>` (e.g.
  `hardwood-1.1.0-SNAPSHOT-<shortHash>-libzstd-jni-1.5.7-9.so`), where `<shortHash>`
  is the first 8 bytes of the SHA-256 digest encoded as Base64 URL without padding
  (for brevity). The full SHA-256 of the embedded bytes is still verified on every
  cache hit.

  **Loading priority:**
  1. External `lib/` directory (via `HARDWOOD_LIB_PATH` or next to executable)
  2. An existing copy in `~/.hardwood/` — its presence signals a previous run could
     not use `java.io.tmpdir` (e.g. mounted `noexec`), so it is used without
     re-attempting tmp
  3. Embedded resources extracted directly to `java.io.tmpdir`
  4. Embedded resources extracted to `~/.hardwood/` (fallback when the tmp attempt
     fails)

  **Load-failure fallback:** if a library extracted to `java.io.tmpdir` cannot be
  loaded (for example `/tmp` is mounted `noexec`), the extracted file and its lock
  file are deleted and extraction + load is retried in `~/.hardwood/`.

  **Concurrency:** a per-library `.lck` file (named like the cache file with a
  `.lck` extension) serializes concurrent extraction across multiple processes.
  The lock file is deleted as soon as extraction completes — it exists only to
  prevent two processes from extracting the same library at once. Extracted
  libraries are verified by SHA-256 hash on every cache hit; corrupted files are
  automatically replaced.

  This means the native binary works as a standalone executable — no `lib/`
  directory or environment variable required.

- **Brotli** — managed by `NativeLibraryLoader` like ZSTD/Snappy/LZ4: the `brotli4j`
  native library (`libbrotli.so`) is embedded as a GraalVM image resource, extracted to
  the same flat cache, and loaded at startup alongside the other codecs. The loader sets
  the `brotli4j.library.path` system property to the extracted file, so brotli4j's
  `Brotli4jLoader.ensureAvailability()` (invoked lazily from core's `BrotliDecompressor`
  at decompression time) calls `System.load` on the already-loaded file and skips its own
  temp-file extraction.

- **libdeflate (GZIP acceleration)** — libdeflate uses the Java 22+ Foreign Function & Memory (FFM) API, which relies on runtime downcall handles that cannot be created inside a native image. `LibdeflateLoader` detects the native image context via the `org.graalvm.nativeimage.imagecode` system property and returns `isAvailable() = false`, dead-code-eliminating the entire FFM path. The `--initialize-at-build-time` directive in `core`'s `native-image.properties` ensures GraalVM constant-folds this check at image build time.

### Build arguments (`native-maven-plugin` `buildArgs` in `cli/pom.xml`)

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
