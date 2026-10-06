<!--

    SPDX-License-Identifier: CC-BY-SA-4.0

    Copyright The original authors

    Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Write to S3

Add `hardwood-s3` to write Parquet directly to an existing S3 or S3-compatible bucket:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-s3</artifactId>
</dependency>
```

Configure a source, pass its output to `ParquetFileWriter`, and close the writer before reading the object back:

```java
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3Source;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

FileSchema schema = FileSchema.builder("events")
        .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
        .build();

try (S3Source source = S3Source.builder()
        .region("us-east-1")
        .credentials(S3Credentials.of(accessKeyId, secretKey))
        .build()) {
    try (ParquetFileWriter writer = ParquetFileWriter.create(
            source.outputFile("s3://my-bucket/data/events.parquet"), schema)) {
        writer.columnWriter().writeBatch(batch -> batch.ints("id", new int[] { 7, 11, 19 }));
    }

    try (ParquetFileReader reader = ParquetFileReader.open(
            source.inputFile("s3://my-bucket/data/events.parquet"));
         RowReader rows = reader.rowReader()) {
        while (rows.hasNext()) {
            rows.next();
            System.out.println(rows.getInt("id"));
        }
    }
}
```

Use `source.outputFile("my-bucket", "data/events.parquet")` to pass the bucket and key separately. Both overloads return an uncreated `OutputFile`; the writer calls `create()` automatically. Keep the source open until the writer finishes or is aborted. Publication replaces an existing object at the same key.

## Configure Upload Buffering

Set `uploadPartSize` on the source builder to change the buffer size for each active output:

```java
S3Source source = S3Source.builder()
        .region("us-east-1")
        .credentials(S3Credentials.of(accessKeyId, secretKey))
        .uploadPartSize(16 * 1024 * 1024)
        .build();
```

The default is 8 MiB; valid sizes start at 5 MiB. Use a larger buffer when the expected file exceeds the default 78.125 GiB limit. [S3 Reference](../reference/s3.md#writing) lists the size limits and request behavior.

## Use an S3-Compatible Endpoint

Configure the same endpoint and credentials used for reads:

```java
S3Source source = S3Source.builder()
        .endpoint("http://localhost:9000")
        .pathStyle(true)
        .credentials(S3Credentials.of(accessKeyId, secretKey))
        .build();
```

Grant [upload, cleanup, and verification permissions](../reference/s3.md#write-permissions) for the target. [Reading from S3](s3.md) covers refreshable credentials and the AWS credential chain.

## Handle a Failed or Abandoned Write

A failed writer write discards the output on close. If code producing the data fails outside the writer, call `writer.abort()` before leaving the block; [Handle Write Failures](write-failures.md) shows how to preserve a cleanup failure with the original exception.

If close throws, inspect the exception and its suppressed diagnostics. A diagnostic saying publication could not be confirmed means the completed object may already exist. Check the destination before starting a replacement write. The [publication contract](../reference/s3.md#publication-and-cleanup) describes what close and discard can establish.
