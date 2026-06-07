# Hardwood CLI — local build and quick usage

This README documents the local native build and how to run the CLI binary produced by a local build.
It includes guidance on building a Linux binary using Docker for distribution.

Prerequisites
- Java 25 (GraalVM) installed locally if you want a macOS native binary. Use SDKMAN to install GraalVM:

```bash
sdk install java 25.0.2-graalce
```

Local native build

Run the native build for the `cli` module and its dependencies:

```bash
./mvnw -Dnative package -pl cli -am
```

Building a Linux binary (containerized)

If you want to build a Linux x86_64 ELF binary (e.g., for Docker on macOS), use the containerized native build:

```bash
./mvnw -Dnative -Dquarkus.native.container-build=true package -pl cli -am
```

This requires Docker to be running and produces a Linux ELF binary that can be used in Docker images.

> **Note:** The containerized build always produces a Linux binary. Running it directly on macOS will fail with `exec format error`. Use it inside Docker or for inclusion in a Docker image.

Common build failure (missing build-only module)

The QA profile wires in a build-only annotation-processor module `dev.hardwood:hardwood-error-prone-checks`. If you see an error like

```
Could not find artifact dev.hardwood:hardwood-error-prone-checks:jar:1.0.0-SNAPSHOT
```

fix it by installing that module locally first:

```bash
./mvnw -pl error-prone-checks install -DskipTests
./mvnw -Dnative package -pl cli -am
```

Or build the checks module together with the CLI in one invocation:

```bash
./mvnw -Dnative package -pl cli,error-prone-checks -am
```

Where the built binary appears

The common local build output is:

```
cli/target/hardwood-cli
```

## Running the binary (examples)

From the repository root, run the locally built binary:

```bash
# help and version
cli/target/hardwood-cli --help
cli/target/hardwood-cli --version

# run info against an included test fixture
cli/target/hardwood-cli info -f core/src/test/resources/plain_uncompressed.parquet

# print rows (first 10)
cli/target/hardwood-cli print -n 10 -f core/src/test/resources/plain_uncompressed.parquet

# interactive TUI (run in a terminal)
cli/target/hardwood-cli dive -f core/src/test/resources/plain_uncompressed.parquet
```

## Building a Docker image

The repository contains a minimal Fedora-based Dockerfile at `cli/docker/Dockerfile`. To build a Docker image locally, use the convenience script in the cli directory:

```bash
cd cli
./build-cli-docker.sh
```

By default, the script uses an existing built binary if available. If you want to force a rebuild of the CLI binary, use the `-f` or `--force` flag:

```bash
./build-cli-docker.sh -f
```

You can also specify a custom image tag:

```bash
./build-cli-docker.sh v1.0.0              # uses existing binary
./build-cli-docker.sh --force v1.0.0      # forces rebuild
```

The script automatically builds a Linux native binary using the containerized native build (if needed). This requires Docker to be running.

After building, run the image:

```bash
docker run --rm hardwood/hardwood:local --help
```

Using the Docker image (run from the repo root)

```bash
# Mount a local directory and run a command 
docker run --rm -v "$(pwd)":/repo -w /repo hardwood/hardwood:local info -f core/src/test/resources/plain_uncompressed.parquet

# Run interactive TUI in a container
docker run --rm -it -v "$(pwd)":/repo -w /repo hardwood/hardwood:local dive -f core/src/test/resources/plain_uncompressed.parquet
```

```bash
# Start an interactive shell with the current directory mounted (tab completion is sourced automatically)
docker run --rm -it -v "$(pwd)":/data -w /data hardwood/hardwood:local
```

