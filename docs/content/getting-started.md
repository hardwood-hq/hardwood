<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Getting Started

Hardwood runs on Java 21 or newer; Java 25 is recommended for best performance.

If you just want to inspect or convert Parquet files from the command line, grab a pre-built native binary for Linux, macOS, or Windows from the [release page](https://github.com/hardwood-hq/hardwood/releases/tag/{{cli_release_tag}}); see the [CLI](cli.md) page for details.

## Using the BOM (Bill of Materials)

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

Then declare dependencies without versions:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-core</artifactId>
</dependency>
```

## Adding the Core Dependency

If you prefer not to use the BOM, you can specify the version directly:

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

## Logging

Hardwood uses the Java Platform Logging API (`System.Logger`).
Bindings are available for all popular logger implementations, for instance for [log4j 2](https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-jpl).

## Compression Libraries

Hardwood supports reading Parquet files compressed with GZIP (built into Java), Snappy, ZSTD, LZ4, and Brotli. The compression libraries are optional dependencies—add only the ones you need:

| Codec | Group ID | Artifact ID |
|-------|----------|-------------|
| Snappy | `org.xerial.snappy` | `snappy-java` |
| ZSTD | `com.github.luben` | `zstd-jni` |
| LZ4 | `at.yawk.lz4` | `lz4-java` |
| Brotli | `com.aayushatharva.brotli4j` | `brotli4j` |

When using the BOM, declare without a version — for example, to add Snappy:

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

??? note "Without the BOM (explicit versions)"

    ```xml
    <dependency>
        <groupId>org.xerial.snappy</groupId>
        <artifactId>snappy-java</artifactId>
        <version>1.1.10.8</version>
    </dependency>
    <dependency>
        <groupId>com.github.luben</groupId>
        <artifactId>zstd-jni</artifactId>
        <version>1.5.7-6</version>
    </dependency>
    <dependency>
        <groupId>at.yawk.lz4</groupId>
        <artifactId>lz4-java</artifactId>
        <version>1.8.1</version>
    </dependency>
    <dependency>
        <groupId>com.aayushatharva.brotli4j</groupId>
        <artifactId>brotli4j</artifactId>
        <version>1.20.0</version>
    </dependency>
    ```

    ```groovy
    implementation 'org.xerial.snappy:snappy-java:1.1.10.8'
    implementation 'com.github.luben:zstd-jni:1.5.7-6'
    implementation 'at.yawk.lz4:lz4-java:1.8.1'
    implementation 'com.aayushatharva.brotli4j:brotli4j:1.20.0'
    ```

If you attempt to read a file using a compression codec whose library is not on the classpath, Hardwood will throw an exception with a message indicating which dependency to add.

## First Read

With the core dependency in place, read a Parquet file:

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.rowReader()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        System.out.println(id + ": " + name);
    }
}
```

See [Usage](usage.md) for the full API reference including column projection, predicate pushdown, column-oriented reading, and more.
