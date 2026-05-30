<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Package Structure

Hardwood is organized into public API packages and internal implementation packages. Application code should import only from the public packages; `dev.hardwood.internal.*` and its subpackages are implementation details and may change without notice.

| Package | Visibility | Purpose |
|---------|-----------|---------|
| [`dev.hardwood`](/api/latest/dev/hardwood/package-summary.html) | **Public API** | Entry point for creating readers and managing shared resources (thread pool, decompressor pool). |
| [`dev.hardwood.reader`](/api/latest/dev/hardwood/reader/package-summary.html) | **Public API** | Single-file and multi-file readers for row-oriented and column-oriented access. |
| [`dev.hardwood.metadata`](/api/latest/dev/hardwood/metadata/package-summary.html) | **Public API** | Parquet file metadata: row groups, column chunks, physical/logical types, and compression codecs. |
| [`dev.hardwood.schema`](/api/latest/dev/hardwood/schema/package-summary.html) | **Public API** | Schema representation: file schema, column schemas, and column projection. |
| [`dev.hardwood.row`](/api/latest/dev/hardwood/row/package-summary.html) | **Public API** | Value types for nested data access: structs, lists, and maps. |
| [`dev.hardwood.avro`](/api/latest/dev/hardwood/avro/package-summary.html) | **Public API** | Avro GenericRecord support: schema conversion and row materialization (`hardwood-avro` module). |
| [`dev.hardwood.s3`](/api/latest/dev/hardwood/s3/package-summary.html) | **Public API** | S3 object storage support: `S3Source`, `S3InputFile`, `S3Credentials`, `S3CredentialsProvider` (`hardwood-s3` module, zero external dependencies). |
| [`dev.hardwood.aws.auth`](/api/latest/dev/hardwood/aws/auth/package-summary.html) | **Public API** | Bridges the AWS SDK credential chain to Hardwood's `S3CredentialsProvider` (`hardwood-aws-auth` module, optional). |
| [`dev.hardwood.jfr`](/api/latest/dev/hardwood/jfr/package-summary.html) | **Public API** | JFR event types emitted during file reading, decoding, and pipeline operations. |
| `dev.hardwood.internal.*` | **Internal** | Implementation details — not part of the public API and may change without notice. |
