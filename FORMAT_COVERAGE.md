# Parquet Format Coverage

Field-level companion to [ROADMAP.md](ROADMAP.md). For every struct/union in the
Parquet [`parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
metadata definition, this table records whether Hardwood reads the field and
whether it acts on it.

ROADMAP tracks *capabilities* (encodings, codecs, bloom filters, the writer);
this table tracks *metadata fields*. It covers the footer/page metadata Hardwood
parses, not the breadth of encoding/compression decoders (that lives in ROADMAP
phases 2 and 8) or the value-conversion layer (phase 7.4).

## Legend

- ✅ **processed** — parsed and used by the reader/decoder/filter pipeline, or surfaced on the public API.
- 🟡 **read-only** — parsed but with no functional consumer.
- ❌ **skipped** — not read (the reader's `default` branch, or an explicit skip case).

The 🟡-vs-✅ distinction is a maintained judgment (whether a parsed value is
*meaningfully* consumed cannot be derived automatically — e.g. `bloom_filter_offset`
is read and displayed yet feeds no filtering). The read-vs-skipped axis matches the
`switch`-on-field-id in each `internal.thrift` reader.

---

## File metadata

### FileMetaData
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | version | ✅ | surfaced in CLI / dive / `info` |
| 2 | schema | ✅ | |
| 3 | num_rows | ✅ | |
| 4 | row_groups | ✅ | |
| 5 | key_value_metadata | ✅ | exposed on `FileMetaData` |
| 6 | created_by | ✅ | displayed in CLI / dive |
| 7 | column_orders | ✅ | decoded onto `FileMetaData.columnOrders`; #595 |
| 8 | encryption_algorithm | ❌ | encrypted footers fail fast (#600); full support #128 |
| 9 | footer_signing_key_metadata | ❌ | #128 |

### FileCryptoMetaData · EncryptionAlgorithm (AesGcmV1, AesGcmCtrV1) · ColumnCryptoMetaData (EncryptionWithFooterKey, EncryptionWithColumnKey)
All fields ❌ — Parquet Modular Encryption is unsupported; encrypted files fail
fast. Tracked by **#128** (support), #600 (graceful failure).

### ColumnOrder (TypeDefinedOrder, IEEE754TotalOrder)
✅ — decoded onto `FileMetaData.columnOrders` as `ColumnOrder`; an unrecognized union member maps to
`ColumnOrder.UNKNOWN`. The order drives float/double statistics pruning: type-defined (and absent)
orders widen `±0` bounds for correct signed-zero handling, while `IEEE754TotalOrder` prunes exactly;
#595.

---

## Row group & column

### RowGroup
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | columns | ✅ | |
| 2 | total_byte_size | ✅ | |
| 3 | num_rows | ✅ | |
| 4 | sorting_columns | ❌ | declared sort order not exploited |
| 5 | file_offset | ❌ | |
| 6 | total_compressed_size | ❌ | |
| 7 | ordinal | ❌ | |

### SortingColumn
All fields (column_idx, descending, nulls_first) ❌ — struct not read.

### ColumnChunk
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | file_path | ❌ | legacy split-file layout unsupported |
| 2 | file_offset | ❌ | |
| 3 | meta_data | ✅ | |
| 4 | offset_index_offset | ✅ | `RowGroupIndexBuffers` |
| 5 | offset_index_length | ✅ | |
| 6 | column_index_offset | ✅ | |
| 7 | column_index_length | ✅ | |
| 8 | crypto_metadata | ❌ | #128 |
| 9 | encrypted_column_metadata | ❌ | #128 |

### ColumnMetaData
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | type | ✅ | |
| 2 | encodings | ✅ | |
| 3 | path_in_schema | ✅ | |
| 4 | codec | ✅ | |
| 5 | num_values | ✅ | |
| 6 | total_uncompressed_size | ✅ | |
| 7 | total_compressed_size | ✅ | fetch planning |
| 8 | key_value_metadata | ✅ | |
| 9 | data_page_offset | ✅ | |
| 10 | index_page_offset | ❌ | explicit skip; index pages superseded by Column Index |
| 11 | dictionary_page_offset | ✅ | |
| 12 | statistics | ✅ | row-group filtering |
| 13 | encoding_stats | ❌ | dictionary/plain page mix not read |
| 14 | bloom_filter_offset | ✅ | shown in dive; filter body read & decoded (#669); used for `eq`/`in` row-group pruning (#105) |
| 15 | bloom_filter_length | ✅ | shown in dive; #669 read path; #105 pushdown |
| 16 | size_statistics | ❌ | #607 |
| 17 | geospatial_statistics | ✅ | row-group filter evaluator (no per-page geospatial stats exist) |

### PageEncodingStats
All fields (page_type, encoding, count) ❌ — struct not read.

---

## Statistics

### Statistics
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | max (deprecated) | ✅ | fallback when 5/6 absent |
| 2 | min (deprecated) | ✅ | fallback |
| 3 | null_count | ✅ | validity + filtering |
| 4 | distinct_count | 🟡 | on public record, no functional consumer (#483) |
| 5 | max_value | ✅ | preferred |
| 6 | min_value | ✅ | preferred |
| 7 | is_max_value_exact | ❌ | truncated-bound flag ignored; #483 |
| 8 | is_min_value_exact | ❌ | #483 |
| 9 | nan_count | ❌ | #607 |

### SizeStatistics
All fields (unencoded_byte_array_data_bytes, repetition_level_histogram,
definition_level_histogram) ❌ — struct not read. **#607**.

### GeospatialStatistics
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | bbox | ✅ | |
| 2 | geospatial_types | ✅ | |

### BoundingBox
All fields (xmin..mmax, ids 1–8) ✅.

---

## Page headers

### PageHeader
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | type | ✅ | |
| 2 | uncompressed_page_size | ✅ | |
| 3 | compressed_page_size | ✅ | |
| 4 | crc | ✅ | CRC32 validation |
| 5 | data_page_header | ✅ | |
| 6 | index_page_header | ❌ | INDEX_PAGE unused in practice |
| 7 | dictionary_page_header | ✅ | |
| 8 | data_page_header_v2 | ✅ | |

### DataPageHeader
All fields (ids 1–5: num_values, encoding, definition_level_encoding,
repetition_level_encoding, statistics) ✅.

### DataPageHeaderV2
All fields (ids 1–8, incl. is_compressed) ✅.

### DictionaryPageHeader
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | num_values | ✅ | |
| 2 | encoding | ✅ | |
| 3 | is_sorted | ❌ | sorted-dictionary hint not used |

### IndexPageHeader
Empty struct; not read (see `PageHeader.index_page_header`).

---

## Page index

### ColumnIndex
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | null_pages | ✅ | |
| 2 | min_values | ✅ | page skipping |
| 3 | max_values | ✅ | |
| 4 | boundary_order | ✅ | |
| 5 | null_counts | ✅ | |
| 6 | repetition_level_histograms | ❌ | #607 |
| 7 | definition_level_histograms | ❌ | skipped; surfacing tracked by #607 |
| 8 | nan_counts | ❌ | #607 |

### OffsetIndex
| id | field | status | notes |
|----|-------|--------|-------|
| 1 | page_locations | ✅ | page scanning |
| 2 | unencoded_byte_array_data_bytes | ❌ | #607 |

### PageLocation
All fields (offset, compressed_page_size, first_row_index) ✅.

---

## Logical types

### LogicalType (union)
All variants recognized ✅: STRING (1), MAP (2), LIST (3), ENUM (4), DECIMAL (5),
DATE (6), TIME (7), TIMESTAMP (8), INTERVAL (9, also via `ConvertedType` 21),
INTEGER (10), UNKNOWN/NULL (11), JSON (12), BSON (13), UUID (14), FLOAT16 (15),
VARIANT (16), GEOMETRY (17), GEOGRAPHY (18).

Parameterized sub-structs — all fields ✅: `DecimalType` (scale, precision),
`TimeType` (isAdjustedToUTC, unit), `TimestampType` (isAdjustedToUTC, unit),
`IntType` (bitWidth, isSigned), `VariantType` (specification_version),
`GeometryType` (crs), `GeographyType` (crs, algorithm), `TimeUnit` union
(MILLIS/MICROS/NANOS).

---

## Bloom filter

### BloomFilterHeader · BloomFilterAlgorithm · BloomFilterHash · BloomFilterCompression
All fields ✅ — the bloom filter at `bloom_filter_offset` is read and decoded (header plus the
split-block bitset), with an XXH64 membership-check primitive (#669). The decoded filter drives
`eq`/`in` row-group pruning for INT32, INT64, and binary columns (#105). Dive surfacing is #507.

---

## Capability gaps at a glance

The ❌ rows cluster into a handful of capabilities, cross-referenced to ROADMAP:

- **Modular encryption** — entire feature stubbed to fail-fast. #128 (ROADMAP has no phase yet).
- **Bloom-filter writing** — filter *serialization* is not implemented (ROADMAP 9.3). The read path (#669) and `eq`/`in` pushdown (#105) are done.
- **Size statistics & level histograms** — ROADMAP 9.x; #607.
- **Statistics completeness** — distinct_count, exactness flags, nan_count; #483, #607.
- **Declared sort order** — `sorting_columns`, `is_sorted`; ROADMAP 4.2.
- **Column orders** — float total-order vs type-defined; #483.
- **Deprecated / niche** — split-file `file_path`, index pages, encoding_stats, row-group ordinals: no planned support.
