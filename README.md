# Hardwood

_A parser for the Apache Parquet file format, optimized for minimal dependencies and great performance.
Available as a Java library and a [command-line tool](https://hardwood.dev/latest/cli/)._

Goals of the project are:

* Be light-weight: Implement the Parquet file format avoiding any 3rd party dependencies other than for compression algorithms (e.g. Snappy)
* Be correct: Support all Parquet files which are supported by the canonical [parquet-java](https://github.com/apache/parquet-java) library
* Be fast: Be as fast or faster as parquet-java
* Be complete: Add a Parquet file writer (after 1.0)

Latest version: 1.0.0.Beta1, 2026-04-02

## Documentation

Full documentation is available at **[hardwood.dev](https://hardwood.dev/)**.

## Quick Start

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
    <version>1.0.0.Beta1</version>
</dependency>
```

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.createRowReader()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        LocalDate birthDate = rowReader.getDate("birth_date");
        Instant createdAt = rowReader.getTimestamp("created_at");
    }
}
```

See the [Getting Started](https://hardwood.dev/latest/getting-started/) guide for detailed setup instructions.

---

## Repository Documentation

| Document | Purpose |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | High-level architecture and module layout. |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute: workflow, commit format, PR expectations. |
| [ROADMAP.md](ROADMAP.md) | Implementation status, roadmap, and milestones. |
| [NATIVE_BUILD.md](NATIVE_BUILD.md) | How the GraalVM native CLI build works. |
| [PERFORMANCE.md](PERFORMANCE.md) | Benchmark results and how to run performance tests. |
| [TESTING.md](TESTING.md) | Manual testing recipes (e.g. S3 via s3proxy). |
| [RELEASING.md](RELEASING.md) | Release process. |

---

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide — how to find work, the issue-first workflow, commit message format, and PR expectations.

- File bugs and feature requests on the [issue tracker](https://github.com/hardwood-hq/hardwood/issues).
- Ask questions or discuss ideas on [GitHub Discussions](https://github.com/hardwood-hq/hardwood/discussions).
- Looking for something to work on? Browse [`good first issue`](https://github.com/hardwood-hq/hardwood/labels/good%20first%20issue) and [`help wanted`](https://github.com/hardwood-hq/hardwood/labels/help%20wanted).

LLM-assisted contributions are welcome, but vibe coding — accepting AI-generated changes without understanding them — is not. The aspiration is a high-quality, maintainable, performant, safe codebase.

See [ROADMAP.md](ROADMAP.md) for the detailed implementation status, roadmap, and milestones.

---

## Build

This project requires **Java 25 or newer for building** (to create the multi-release JAR with Java 22+ FFM support). The resulting JAR runs on **Java 21+** (libdeflate support requires Java 22+).

**Docker must be running** for the build to succeed, as the test suite uses [Testcontainers](https://www.testcontainers.org/) to spin up services (e.g. S3 integration tests). If Docker is not available, the build will fail during the test phase for these tests.

It comes with the Apache [Maven wrapper](https://github.com/takari/maven-wrapper),
i.e. a Maven distribution will be downloaded automatically, if needed.

Run the following command to build this project:

```shell
./mvnw clean verify
```

On Windows, run the following command:

```shell
mvnw.cmd clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```shell
./mvnw clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```shell
./mvnw process-sources
```

### Building the Native CLI

The `hardwood` CLI can be compiled to a GraalVM native binary using the `-Dnative` flag.

#### Local GraalVM build

Requires GraalVM (Java 25+) installed locally. Install via [SDKMAN](https://sdkman.io/):

```shell
sdk install java 25.0.2-graalce
```

Then build:

```shell
./mvnw -Dnative package -pl cli -am
```

The resulting distribution is at `cli/target/hardwood-<version>/bin/hardwood`.

#### Linux — container build (no local GraalVM required)

Requires Docker. The build runs inside a Linux container and produces a Linux x86\_64 ELF binary:

```shell
./mvnw -Dnative -Dquarkus.native.container-build=true package -pl cli -am
```

> **Note:** The container build always produces a Linux binary. Running it on macOS will fail with `exec format error`. Use the local GraalVM build for macOS binaries.

See [NATIVE_BUILD.md](NATIVE_BUILD.md) for details on how the native build works (compression codec handling, build arguments).

### Building the Documentation

The documentation site can be previewed locally using Docker:

```shell
# Build the image (once, or after changing requirements.txt)
docker build -t hardwood-docs docs/

# Serve locally with hot reload — preview at http://127.0.0.1:8000
docker run --rm -p 8000:8000 -v "$(pwd):/repo" hardwood-docs

# Build static site (output in docs/site/)
docker run --rm -v "$(pwd):/repo" hardwood-docs build -f docs/mkdocs.yml
```

### Running Claude Code

A Docker Compose set-up is provided for running [Claude Code](https://claude.ai/claude-code) with all build dependencies (Java 25, Maven, `gh`) pre-installed.

```shell
GH_TOKEN=<your-token> docker compose run --rm claude
```

Set `GH_TOKEN` to a GitHub personal access token so that Claude Code can interact with issues and pull requests. The project directory is mounted into the container at `/workspace`, and Claude Code configuration is persisted in a named volume across sessions.

### Creating a Release

See [RELEASING.md](RELEASING.md).

### API Change Report

To generate an API change report comparing the current build against a previous release:

```shell
./mvnw package japicmp:cmp -pl :hardwood-core -DskipTests -Djapicmp.oldVersion=<PREVIOUS_VERSION>
```

The `package` phase is needed to build the current jar before comparing. The report is written to `core/target/japicmp/`. Internal packages (`dev.hardwood.internal`) are excluded. This is run automatically during releases.

## Performance

See [PERFORMANCE.md](PERFORMANCE.md) for benchmark results and instructions on running performance tests.

---


## License

This code base is available under the Apache License, version 2.

---

## Resources

- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Thrift Compact Protocol Spec](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Dremel Paper](https://research.google/pubs/pub36632/)
- [parquet-java Reference](https://github.com/apache/parquet-java)
- [parquet-testing Files](https://github.com/apache/parquet-testing)
- [arrow-testing Files](https://github.com/apache/arrow-testing)
