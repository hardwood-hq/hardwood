<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Release Notes

See [GitHub Releases](https://github.com/hardwood-hq/hardwood/releases) for the full changelog.

## 1.0.0.Beta2

- Memory-mapped file I/O support for files larger than 2 GB ([#75](https://github.com/hardwood-hq/hardwood/issues/75))
- Zero-copy native memory decoding pipeline (FFM) on Java 22+

## 1.0.0.Beta1

[Announcement blog post](https://www.morling.dev/blog/hardwood-reaches-beta-s3-predicate-push-down-cli/)

- S3 and remote object store support with coalesced reads ([#31](https://github.com/hardwood-hq/hardwood/issues/31))
- CLI tool for inspecting and querying Parquet files ([#38](https://github.com/hardwood-hq/hardwood/issues/38))
- Avro `GenericRecord` support via the `hardwood-avro` module ([#131](https://github.com/hardwood-hq/hardwood/issues/131))
- Row group filtering with predicate push-down ([#59](https://github.com/hardwood-hq/hardwood/issues/59))
- Page-level column index filtering ([#118](https://github.com/hardwood-hq/hardwood/issues/118))
- `InputFile` abstraction for pluggable file sources ([#98](https://github.com/hardwood-hq/hardwood/issues/98))
- `FieldPath` for unambiguous column lookup ([#59](https://github.com/hardwood-hq/hardwood/issues/59))
- Page CRC verification ([#76](https://github.com/hardwood-hq/hardwood/issues/76))
- Key/value metadata access ([#135](https://github.com/hardwood-hq/hardwood/issues/135))
- S3 support and filtering in the parquet-java compatibility layer ([#123](https://github.com/hardwood-hq/hardwood/issues/123))
- Project documentation site ([#109](https://github.com/hardwood-hq/hardwood/issues/109))

## 1.0.0.Alpha1

[Announcement blog post](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/)

- Zero-dependency Parquet file reader for Java
- Row-oriented and columnar read APIs
- Support for flat and nested schemas (lists, maps, structs)
- All standard encodings (RLE, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, etc.)
- Compression: Snappy, ZSTD, LZ4, GZIP, Brotli
- Projection push-down
- Parallel page pre-fetching and eager batch assembly
- Memory-mapped file I/O
- JFR events for observability
- Multi-file reader
- BOM for dependency management
- parquet-java compatibility layer
- Optional Vector API acceleration on Java 22+
