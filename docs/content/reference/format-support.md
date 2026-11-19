<!--

     SPDX-License-Identifier: Apache-2.0

     Copyright The original authors

     Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->

# Format Support

Hardwood 1.0 reads the parts of the Parquet format listed below. It does not write Parquet files ([#9](https://github.com/hardwood-hq/hardwood/issues/9)).

Entries marked *legacy* are deprecated by the Parquet specification or kept in it for files older writers produced.

## Types

| Type | Read |
|---|---|
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | Yes |
| `INT96` (legacy) | Yes |
| `STRING`, `ENUM`, `JSON`, `BSON`, `UUID`, `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, `INTERVAL`, `FLOAT16`, `UNKNOWN`, signed and unsigned `INT` | Yes |
| `GEOMETRY`, `GEOGRAPHY` | Yes |
| `VARIANT`, unshredded and shredded | Yes |
| `LIST`, `MAP` and structs, nested | Yes |
| Two-level lists (legacy) | Yes |

The Java types each annotation reads as are in [Typed Accessors](accessors.md).

## Encodings

| Encoding | Read |
|---|---|
| `PLAIN` | Yes |
| `RLE_DICTIONARY` | Yes |
| `PLAIN_DICTIONARY` (legacy) | Yes |
| `RLE` for levels | Yes |
| `RLE` for `BOOLEAN` values | Yes |
| `BIT_PACKED` levels (legacy) | No ([#569](https://github.com/hardwood-hq/hardwood/issues/569)) |
| `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY` | Yes |
| `BYTE_STREAM_SPLIT` | Yes |

## Compression Codecs

| Codec | Read |
|---|---|
| `UNCOMPRESSED`, `GZIP`, `SNAPPY`, `ZSTD`, `LZ4_RAW`, `BROTLI` | Yes |
| `LZ4`, with Hadoop framing (legacy) | Yes |
| `LZO` | No |

Codecs other than `UNCOMPRESSED` and `GZIP` need their library on the classpath; see [Compression Libraries](../getting-started.md#compression-libraries).

## Pages

| Page | Read |
|---|---|
| Data pages V1 and V2 | Yes |
| Dictionary pages | Yes |
| Page CRC | Yes |
| Index pages (unused by writers) | No |

## Metadata

| Structure | Read |
|---|---|
| Column statistics (`min_value` / `max_value`, `null_count`) | Yes, used for pruning |
| `nan_count` | No ([#607](https://github.com/hardwood-hq/hardwood/issues/607)) |
| `distinct_count` | Yes |
| Page index (`ColumnIndex`, `OffsetIndex`) | Yes, used for pruning |
| Bloom filters | No ([#105](https://github.com/hardwood-hq/hardwood/issues/105)) |
| `SizeStatistics` and level histograms | No ([#607](https://github.com/hardwood-hq/hardwood/issues/607)) |
| `GeospatialStatistics` | Yes, used for pruning |
| `encoding_stats` | No |
| File key-value metadata | Yes |
| Column-chunk key-value metadata | Yes |
| `sorting_columns` | No ([#483](https://github.com/hardwood-hq/hardwood/issues/483)) |
| Schema `field_id` | On `SchemaElement` |

See [Query Controls](query-controls.md) for skipping data with these structures.

## Encryption

Parquet Modular Encryption is not supported. Opening an encrypted file fails; see [Error Handling](error-handling.md) for the exception raised.

## Sources

| Storage | Read |
|---|---|
| Local files | Yes |
| In memory | Yes |
| Amazon S3 and S3-compatible stores | Yes |
| Other stores | Through an `InputFile` implementation |

## Modules

| Module | Read |
|---|---|
| `hardwood-core` | Yes |
| `hardwood-avro` (`GenericRecord`) | Yes |
| `hardwood-parquet-java-compat` (`ParquetReader<Group>`) | Yes |
| `hardwood` CLI | Yes |
