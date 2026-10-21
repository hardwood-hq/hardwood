<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Write to S3

Add the `hardwood-s3` dependency:

```xml
<dependency>
    <groupId>dev.hardwood</groupId>
    <artifactId>hardwood-s3</artifactId>
</dependency>
```

Create an `OutputFile` from an `S3Source` and pass it to `ParquetFileWriter`. The object appears at the key when the writer closes:

```java
try (S3Source source = S3Source.builder()
        .region("eu-west-1")
        .credentials(S3Credentials.of(accessKeyId, secretKey))
        .build();
     ParquetFileWriter writer = ParquetFileWriter.create(
             source.outputFile("s3://my-bucket/events/2026-10.parquet"), schema)) {
    writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
}
```

One `S3Source` serves reads and writes alike; [Reading from S3](s3.md) covers credentials and S3-compatible endpoints. A source built with `anonymous()` cannot write. The bucket must already exist.

## Change the part size

The output uploads the file in parts of 8 MiB. A larger part size raises the size limit of one file (10,000 parts) and takes more memory per open output, which holds up to five parts. Each part upload must complete within the source's `requestTimeout` (30 s by default), with four sharing the upload bandwidth, so raise the timeout with the part size on a slow link:

```java
S3Source source = S3Source.builder()
        .region("eu-west-1")
        .credentials(S3Credentials.of(accessKeyId, secretKey))
        .uploadPartSize(64 * 1024 * 1024)
        .requestTimeout(Duration.ofMinutes(2))
        .build();
```

## Clean up after a failed write

A writer that fails, or that `abort()` discards, aborts its upload, so nothing appears at the key. Add a lifecycle rule for incomplete multipart uploads to the bucket, so that the parts of an upload that could not be aborted (for instance because the process ended) are removed. [S3 Reference](../reference/s3.md#writing) lists the requests, limits and failure behavior.
