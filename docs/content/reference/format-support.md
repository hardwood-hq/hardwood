<!--

     SPDX-License-Identifier: Apache-2.0

     Copyright The original authors

     Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->

# Format Support

Hardwood reads and writes the parts of the Parquet format listed below. The [Writer reference](writer.md) and [Reader reference](reader.md) give the options and limits of each.

The Parquet project's [implementation status matrix](https://parquet.apache.org/docs/file-format/implementationstatus/) lists the same support per feature next to the other Parquet implementations.

Entries marked *legacy* are deprecated by the Parquet specification or kept in it for files older writers produced. Hardwood reads every legacy entry, and writes only the two-level lists a schema read from an existing file carries.

## Types

| Type | Read | Write |
|---|---|---|
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | Yes | Yes |
| `INT96` (legacy) | Yes | No |
| `STRING`, `ENUM`, `JSON`, `BSON`, `UUID`, `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, `INTERVAL`, `FLOAT16`, `UNKNOWN`, signed and unsigned `INT`, `GEOMETRY`, `GEOGRAPHY` | Yes | Yes |
| `VARIANT`, unshredded and shredded | Yes | As binary fields, when the schema is read from a file ([#1487](https://github.com/hardwood-hq/hardwood/issues/1487)) |
| `FINITE_VECTOR` | As a `LIST`, without its declared length ([#1804](https://github.com/hardwood-hq/hardwood/issues/1804)) | No ([#1837](https://github.com/hardwood-hq/hardwood/issues/1837)) |
| `LIST`, `MAP`, `FILE` and structs, nested | Yes | Yes |
| Two-level lists (legacy) | Yes | Yes, through `ColumnWriter` when the schema is read from a file |

The Java types each annotation reads as and writes from are in [Typed Accessors](accessors.md) and [Logical Types and Row Setters](writer.md#logical-types-and-row-setters).

## Encodings

| Encoding | Read | Write |
|---|---|---|
| `PLAIN` | Yes | Yes |
| `RLE_DICTIONARY` | Yes | Yes |
| `PLAIN_DICTIONARY` (legacy) | Yes | No |
| `RLE` for levels | Yes | Yes |
| `RLE` for `BOOLEAN` values | Yes | No ([#1821](https://github.com/hardwood-hq/hardwood/issues/1821)) |
| `BIT_PACKED` levels (legacy) | Yes | No |
| `DELTA_BINARY_PACKED` | Yes | Yes |
| `DELTA_LENGTH_BYTE_ARRAY` | Yes | Yes |
| `DELTA_BYTE_ARRAY` | Yes | Yes |
| `BYTE_STREAM_SPLIT` | Yes | Yes |
| `ALP` | Yes | Yes |

See [Encodings](writer.md#encodings) for the writer's choice of encoding and the encodings each physical type takes.

## Compression Codecs

| Codec | Read | Write |
|---|---|---|
| `UNCOMPRESSED` | Yes | Yes |
| `GZIP` | Yes | Yes |
| `SNAPPY` | Yes | Yes |
| `ZSTD` | Yes | Yes |
| `LZ4_RAW` | Yes | Yes |
| `BROTLI` | Yes | Yes |
| `LZ4`, Hadoop-framed or as raw blocks (legacy) | Yes | No |
| `LZO` | No | No |

Codecs other than `UNCOMPRESSED` and `GZIP` need their library on the classpath; see [Compression Codecs](writer.md#compression-codecs).

## Pages

| Page | Read | Write |
|---|---|---|
| Data pages V1 | Yes | Yes |
| Data pages V2 | Yes | Yes |
| Dictionary pages | Yes | Yes |
| Page CRC | Yes | Yes |
| Index pages (unused by writers) | No | No |

## Metadata

| Structure | Read | Write |
|---|---|---|
| Column statistics (`min_value` / `max_value`, `null_count`, `nan_count`) | Yes, used for pruning | Yes |
| `distinct_count` | Yes | Yes |
| Page index (`ColumnIndex`, `OffsetIndex`) | Yes, used for pruning | Yes |
| Bloom filters | Yes, used for pruning | For the columns configured |
| `SizeStatistics` and level histograms | Yes | Yes |
| `GeospatialStatistics` | Yes, used for pruning | No ([#1486](https://github.com/hardwood-hq/hardwood/issues/1486)) |
| `encoding_stats` | Yes, used for pruning | Yes |
| File key-value metadata | Yes | Yes |
| Column-chunk key-value metadata | Yes | No ([#1809](https://github.com/hardwood-hq/hardwood/issues/1809)) |
| `sorting_columns` | No ([#483](https://github.com/hardwood-hq/hardwood/issues/483)) | Yes |
| Schema `field_id` | Yes | Yes |

See [Query Controls](query-controls.md) for skipping data with these structures, and [Statistics Written](writer.md#statistics-written) for their contents in a file Hardwood writes.

## Encryption

Parquet Modular Encryption is not supported. Reading an encrypted file fails with `UnsupportedOperationException`.

## Sources and Destinations

| Storage | Read | Write |
|---|---|---|
| Local files | Yes | Yes |
| In memory | Yes | Yes |
| Amazon S3 and S3-compatible stores | Yes | Yes |
| Other stores | Through an `InputFile` implementation | Through an `AbstractOutputFile` subclass |

## Modules

| Module | Read | Write |
|---|---|---|
| `hardwood-core` | Yes | Yes |
| `hardwood-avro` (`GenericRecord`) | Yes | No |
| `hardwood-parquet-java-compat` (`ParquetReader<Group>`) | Yes | No |
| `hardwood` CLI | Yes | No |
