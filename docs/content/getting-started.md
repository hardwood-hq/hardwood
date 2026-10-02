<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Getting Started

Hardwood runs on Java 21 or newer; Java 25 is recommended for best performance.

To inspect or convert Parquet files from the command line without writing code, install the [command-line tool](#command-line-tool).

## Dependency Management

### Using the BOM (Bill of Materials)

The `hardwood-bom` manages versions for all Hardwood modules and their optional runtime dependencies.
Import it in your dependency management so you can declare Hardwood dependencies without specifying versions:

**Maven:**

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>dev.hardwood</groupId>
            <artifactId>hardwood-bom</artifactId>
            <version>{{hardwood_version}}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

**Gradle:**

```groovy
dependencies {
    implementation platform('dev.hardwood:hardwood-bom:{{hardwood_version}}')
}
```

Then declare dependencies inside your project's `<dependencies>` block without specifying a version:

```xml
<dependencies>
    <dependency>
        <groupId>dev.hardwood</groupId>
        <artifactId>hardwood-core</artifactId>
    </dependency>
</dependencies>
```

### Specifying the Version Directly

**Maven:**

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
    <version>{{hardwood_version}}</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'dev.hardwood:hardwood-core:{{hardwood_version}}'
```

## Optional Dependencies

### Logging

Hardwood uses the Java Platform Logging API (`System.Logger`).
Bindings are available for all popular logger implementations, for instance for [log4j 2](https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-jpl).

### Compression Libraries

The compression libraries are optional dependencies; add only the ones you need. The [Writer Reference](reference/writer.md#compression-codecs) lists what each codec needs. Snappy and ZSTD are the codecs most commonly seen in the wild; LZ4 and Brotli are rarer.

Writing compresses with ZSTD where `zstd-jni` is on the classpath, and writes uncompressed pages where it is not, so add that dependency to get compressed output under the default configuration.

| Codec | Group ID | Artifact ID |
|-------|----------|-------------|
| Snappy | `org.xerial.snappy` | `snappy-java` |
| ZSTD | `com.github.luben` | `zstd-jni` |
| LZ4 (`LZ4_RAW` and Hadoop-framed `LZ4`) | `at.yawk.lz4` | `lz4-java` |
| Brotli | `com.aayushatharva.brotli4j` | `brotli4j` |

When using the BOM, declare without a version. For example, to add Snappy:

**Maven:**

```xml
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.xerial.snappy:snappy-java'
```

If you attempt to read a file using a compression codec whose library is not on the classpath, Hardwood will throw an exception with a message indicating which dependency to add.

## Command-Line Tool

The `hardwood` command-line tool ships as a GraalVM native binary for Linux, macOS, and Windows, available from the [release page](https://github.com/hardwood-hq/hardwood/releases/tag/{{cli_release_tag}}). The [CLI reference](reference/cli.md) documents its commands.

!!! note "macOS"
    The binary is not notarized. On first run, macOS Gatekeeper will block it. Remove the quarantine flag after extracting:

    ```shell
    xattr -r -d com.apple.quarantine hardwood-cli-*/
    ```

### Shell Completion

The distribution includes completion scripts for Bash, Zsh, and Fish under `bin/`:

| Shell | Script |
|-------|--------|
| Bash | `bin/hardwood_completion` |
| Zsh | `bin/hardwood_completion.zsh` |
| Fish | `bin/hardwood_completion.fish` |

Source the one for your shell to enable tab completion for commands, options, and arguments, and add that line to your shell's startup file (e.g. `~/.bashrc`, `~/.zshrc`) to make it permanent:

```shell
source hardwood_completion
```

### Docker

A minimal Fedora-based image is published to the GitHub Container Registry for Linux amd64 and arm64. Pass the command after the image name, and mount a local directory to reach files on the host:

```shell
docker run --rm \
  -v "$(pwd)":/data \
  ghcr.io/hardwood-hq/hardwood:{{cli_docker_tag}} \
  schema -f /data/data.parquet
```

The `dive` TUI needs an interactive terminal, so pass `-it`. With `-it` and no command, the image starts an interactive shell with tab completion loaded:

```shell
docker run --rm -it \
  -v "$(pwd)":/data \
  ghcr.io/hardwood-hq/hardwood:{{cli_docker_tag}} \
  dive -f /data/data.parquet
```

### Use with AI Coding Agents

The repository ships an [Agent Skill](https://agentskills.io) at `skills/hardwood-cli/` that teaches an AI coding agent when and how to reach for the CLI while debugging Parquet read/write code (checking schema and physical/logical types, diagnosing why predicate pushdown or page skipping isn't happening, reading dictionary entries, and so on).

For [Claude Code](https://claude.com/claude-code), it is packaged as the `hardwood` plugin, distributed from the [`hardwood-skills`](https://github.com/hardwood-hq/hardwood-skills) marketplace. Install it once by running, inside Claude Code:

```text
/plugin marketplace add hardwood-hq/hardwood-skills
/plugin install hardwood@hardwood-skills
```

After installing, the skill loads automatically in future sessions whenever a task involves a Parquet file. It drives the `hardwood` binary, so `hardwood` must be on your `PATH`.

For other agent harnesses, or a Claude Code setup without the plugin, copy `skills/hardwood-cli/SKILL.md` into that tool's skills directory (for Claude Code that is `~/.claude/skills/hardwood-cli/`).

## Read a File

The [Read Your First Parquet File](tutorial/first-read.md) tutorial reads a real dataset end to end; the [How-to Guides](how-to/index.md) and the [hardwood-examples](https://github.com/hardwood-hq/hardwood-examples) repository cover the rest of the API.
