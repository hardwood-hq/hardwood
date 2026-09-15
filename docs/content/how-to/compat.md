<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Parquet-Java Compatibility

If you have existing code that uses Apache parquet-java's `ParquetReader<Group>` API and want to switch to Hardwood without rewriting it, the `hardwood-parquet-java-compat` module provides a drop-in replacement. It implements the same `org.apache.parquet.*` interfaces backed by Hardwood's reader, so you get Hardwood's performance with minimal code changes.

!!! warning "Experimental"
    The `hardwood-parquet-java-compat` module is experimental; its API surface and behavior may change in future releases without prior deprecation. This module is not yet available from Maven Central; instead, it needs to be built from source.

!!! warning "Mutually exclusive with parquet-java"
    This module provides its own type shims in the `org.apache.parquet.*` namespace. It **cannot** be used alongside `parquet-java` on the same classpath — pick one or the other.

**Features:**

- Provides `org.apache.parquet.*` namespace classes compatible with parquet-java
- Includes Hadoop shims (`Path`, `Configuration`) — no Hadoop dependency required
- Supports S3 reading via `HadoopInputFile` with the same `fs.s3a.*` configuration properties
- Supports filter predicate pushdown with the standard `FilterApi` / `FilterCompat` classes

## Reading Local Files

```java
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.GroupReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

Path path = new Path("data.parquet");

try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
    Group record;
    while ((record = reader.read()) != null) {
        long id = record.getLong("id", 0);
        String name = record.getString("name", 0);

        Group address = record.getGroup("address", 0);
        String city = address.getString("city", 0);
    }
}
```

## Building the Input File Yourself

Instead of passing a `Path` to the builder, create the input file with `HadoopInputFile.fromPath()` and pass that. This works for local and S3 paths alike.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;

Configuration conf = new Configuration();
HadoopInputFile file = HadoopInputFile.fromPath(new Path("data.parquet"), conf);

try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).build()) {
    // read as usual
}
```

`fromPath()` returns `HadoopInputFile` and throws `IOException`. The file length is read during the call, so a path that does not exist fails there rather than on the first read. For an S3 path this is where the client first connects. `fromPathUnchecked()` is the same call with the exception wrapped in an `UncheckedIOException`.

The instance exposes `getPath()`, `getConfiguration()` and `getLength()`. `getLength()` declares no checked exception — the length is read when the file is created:

```java
HadoopInputFile file = HadoopInputFile.fromPathUnchecked(new Path("data.parquet"), conf);

long bytes = file.getLength();
Path resolved = file.getPath();
```

`fromStatus()` and `newStream()` are not provided; they take Hadoop `FileStatus` and parquet-java `SeekableInputStream`, neither of which this module shims.

## Reading from S3

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;

Configuration conf = new Configuration();
conf.set("fs.s3a.access.key", "...");
conf.set("fs.s3a.secret.key", "...");
conf.set("fs.s3a.endpoint", "https://s3.us-east-1.amazonaws.com");

Path path = new Path("s3a://my-bucket/data.parquet");

try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
        .withConf(conf)
        .build()) {
    // read as usual
}
```

S3 support requires `hardwood-s3` on the classpath. The compat layer loads it via reflection — if missing, a clear error message indicates which dependency to add.

## Filter Pushdown

```java
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import org.apache.parquet.filter2.compat.FilterCompat;

FilterPredicate pred = and(
    gtEq(longColumn("id"), 100L),
    lt(doubleColumn("amount"), 500.0)
);

try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
        .withFilter(FilterCompat.get(pred))
        .build()) {
    // only rows from matching row groups are returned
}
```
