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

### Building a Linux binary

`native-maven-plugin` always targets the host platform, so a Linux ELF binary requires a Linux host with a GraalVM JDK. On macOS the same command produces a Mach-O binary, which a Linux container cannot execute (`exec format error`).

There is no cross-compilation step in the build. The per-platform binaries published with a release are produced by the `package` job in [.github/workflows/release-cli.yml](.github/workflows/release-cli.yml), which runs the build once per runner OS in a matrix.

To obtain a Linux binary from a non-Linux host, either run the build on a Linux machine (a container with a GraalVM JDK will do — the repository's own dev container ships Temurin, which cannot build native images), or download the Linux dist from a release.

### Building the Docker image

`cli/build-cli-docker.sh` builds the container image. It produces (or reuses) the full native dist — the Linux binary, completion script, and codec libraries — then builds the image from it. Since the dist has to be a Linux one, the script runs on Linux only and refuses to start elsewhere:

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

The CLI module uses the [aesh](https://aeshell.github.io/) command framework and GraalVM/Mandrel native image. Several non-obvious pieces are required to make all compression codecs work correctly in a native binary.

### Compression codec native libraries

All compression codecs (Snappy, ZSTD, LZ4, Brotli) ship their native code as JNI libraries inside their JARs. In a standard JVM application, each library extracts itself from the JAR at runtime via `Class.getResourceAsStream()`. This extraction mechanism does not work in a GraalVM native image.

The solution differs by codec:

- **ZSTD, Snappy, LZ4, Brotli** — Native libraries are unpacked from their JARs during the Maven `generate-resources` phase (`maven-dependency-plugin`) and staged in `process-resources` as `target/classes/native/{os}-{arch}/{libzstd-jni,libsnappyjava,liblz4-java,libbrotli}.{so,dylib,dll}`, which `resource-config.json` embeds in the binary. The staging step renames each library to that version-free base name and fails the build if any of the four is missing. At startup, `Main.run()` calls `NativeLibraryLoader`, which extracts each library into a cache directory and loads it via `System.load(absolutePath)` before dispatching the command, so every library is in place before any decompression occurs. Each codec's own loader is then pointed at the loaded file so it does not attempt its own (native-image-incompatible) extraction: `zstd-jni`'s `Native.assumeLoaded()`, the `org.xerial.snappy.lib.path`/`lib.name` properties for Snappy (whose loader may have already run and failed at image build time), and the `brotli4j.library.path` property for Brotli.

  **Cache directories:** `<java.io.tmpdir>/hardwood-<user>/`, then `~/.hardwood/`. A directory is created with `rwx------` permissions when missing, and used only when it is not a symbolic link, the current user can write to it, and neither group nor others can. Only the current user (or root) can therefore have placed a file in it, so a library cannot be swapped between its hash check and `System.load()`.

  **Cache files** are named `hardwood-<shortHash>-<library><ext>`, where `<shortHash>` is the first 8 bytes of the SHA-256 of the embedded bytes, Base64 URL-encoded without padding. The name carries no Hardwood version, so a release re-extracts only the libraries whose bytes changed. A file is reused only when its full SHA-256 matches; otherwise it is rewritten. Extraction writes a uniquely named temporary file and atomically renames it onto the cache file name, so concurrent processes never see a partial file and need no lock: every writer produces identical bytes.

  **Load-failure fallback:** a library that cannot be loaded from `java.io.tmpdir` (for example `/tmp` mounted `noexec`) is deleted and extracted to `~/.hardwood/` instead. A directory already holding the library is tried first on later runs, so such a system does not retry `/tmp` each time.

  **`HARDWOOD_LIB_PATH`** names a directory of native libraries to load instead of the embedded ones. A library is matched by its base name, with or without version suffix or `lib` prefix (e.g. `libzstd-jni-1.5.7-9.so`, `snappyjava.dll`); a library missing there is loaded from the embedded copy. The Docker image extracts the libraries once at image build time into `/usr/local/lib/hardwood/` and sets `HARDWOOD_LIB_PATH` to it, so containers need no writable, executable directory.

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

Automated coverage of the native binary is provided by Failsafe integration tests that spawn the compiled executable as a subprocess; see [_designs-legacy/INTEGRATION_TESTS.md](_designs-legacy/INTEGRATION_TESTS.md). They run during `./mvnw -Dnative -pl cli verify`.

For ad-hoc manual testing of the native binary against S3, see the [Manual S3 testing](TESTING.md#manual-s3-testing) recipe in [TESTING.md](TESTING.md).
