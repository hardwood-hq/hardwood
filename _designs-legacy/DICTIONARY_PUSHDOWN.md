# Plan: Dictionary Predicate Push-Down

**Status: Implemented**

## Context

A dictionary-encoded column chunk stores its distinct values once, in a dictionary page that precedes the chunk's data pages. When *every* data page of the chunk is dictionary-encoded, that page enumerates the chunk's complete set of non-null values.

That makes it a decisive pruning source. Min/max statistics only drop a row group when the predicate value falls outside `[min, max]`; a bloom filter additionally drops one when the value lies inside that range but hashes to a definitely-absent slot, at the cost of false positives. A dictionary answers the same question exactly: a value missing from it cannot occur in any row of the chunk.

This feature consults the dictionary during row-group pruning, alongside statistics and bloom filters. It applies to equality (`EQ`) and membership (`IN`) predicates. Pruning is automatic whenever a filter predicate is supplied; the pruning types all live in `dev.hardwood.internal.*`.

## Eligibility

A chunk's dictionary proves absence only when the dictionary covers all of the chunk's values. A writer may begin a chunk dictionary-encoded and fall back to plain pages once the dictionary grows too large, leaving a dictionary that describes a prefix of the data.

The `encoding_stats` field distinguishes the two. A chunk is eligible when it records:

- at least one `DICTIONARY_PAGE`, and
- at least one data page, and
- no data page written with an encoding other than `PLAIN_DICTIONARY` or `RLE_DICTIONARY`.

`INDEX_PAGE` entries are ignored — an index page holds no values, so it cannot contradict the dictionary. A page type this release does not recognize makes the chunk ineligible, since it may hold values in any encoding. A chunk whose `encoding_stats` is absent is ineligible: without it, dictionary coverage cannot be established.

Reading `encoding_stats` requires exposing it on `ColumnMetaData`, which brings `PageEncodingStats` and `PageType` into the public metadata API.

## Locating the dictionary page

`dictionary_page_offset` is optional in `parquet.thrift`, and its absence is ordinary rather than corrupt — parquet-mr 1.12 omits it on every `PLAIN_DICTIONARY` column of `alltypes_tiny_pages.parquet` in `apache/parquet-testing`, and Trino did the same before 427. Current parquet-java writes it whenever a dictionary page exists, so it is usually but not reliably present.

A dictionary page is always the chunk's first page, so the chunk's start offset is also the dictionary page's. It is `dictionary_page_offset` whenever the file declares a positive one, and `data_page_offset` otherwise. parquet-java's `ColumnChunkMetaData.getStartingPos()` additionally ignores a declared `dictionary_page_offset` that does not precede the first data page, degrading to `data_page_offset` and so to a read that finds no dictionary page; that shape is rejected here instead.

Its *length* comes from the page header, never from the offsets. Where `data_page_offset` follows the dictionary page, the gap between them is the page's length for a well-formed writer and sizes the opening read exactly; where there is no such gap, a bounded probe opens instead and the header-declared length drives a second, exactly-sized read. The gap is only a hint: DuckDB before [duckdb/duckdb#10829](https://github.com/duckdb/duckdb/issues/10829) computed `data_page_offset` without the dictionary page's header size, understating it.

Two metadata shapes are rejected rather than worked around, both with the file name attached:

- a first data page that *precedes* the dictionary page, which contradicts the format's page ordering rather than merely omitting information, and
- a header-declared page length running past the end of the enclosing column chunk.

## Cost model

Dictionaries are read on demand and memoized per column. A row group already dropped by statistics or a bloom filter never pays for the dictionary page I/O, because those checks run first and short-circuit.

For a row group that survives, the dictionary read is one extra request per filtered column. That request is not recoverable by sharing it with the read path: the read path fetches the whole column chunk starting at `chunkStartOffset()`, which already contains the dictionary bytes, so handing it the parsed dictionary would save only the decompress-and-parse CPU and no round trips. The reader therefore parses its own dictionary, and the filter source's cache does not outlive predicate evaluation.

## Comparison semantics

Membership is decided with the same comparison the row matchers apply, so pruning never disagrees with the rows a read would return:

- `INT32` / `INT64` / `BYTE_ARRAY` — value equality, `Arrays.equals` for binary.
- `FLOAT` / `DOUBLE` — `Float.compare` / `Double.compare`, the IEEE 754 total order. This separates `-0.0` from `+0.0`, so a dictionary holding only `-0.0` proves `+0.0` absent; rows holding `-0.0` would not match a `+0.0` predicate either. It also treats all `NaN`s as equal, so a dictionary containing `NaN` reports a `NaN` probe present.

The `±0` ambiguity described by `parquet.thrift` applies to statistics min/max, whose zero bounds writers normalize, and not to stored values. It therefore has no bearing here, and dictionary pruning does not consult the column's `ColumnOrder`.

## Types and predicates

| Physical type | `EQ` | `IN` |
|---|---|---|
| `INT32` | yes | yes |
| `INT64` | yes | yes |
| `FLOAT` | yes | yes |
| `DOUBLE` | yes | yes |
| `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` | yes | yes |
| `FLOAT16` (`FIXED_LEN_BYTE_ARRAY(2)`) | yes | — |

`IN` on `FLOAT` and `DOUBLE` columns is evaluated via `DoubleInPredicate`. For `FLOAT` columns, dictionary entries are widened to `double` and compared with `Double.compare` without narrowing probes. `FLOAT16` does not support `IN`.

A `FLOAT16` chunk's dictionary holds 2-byte values, and each is widened to `float` and compared with `Float.compare` like any other float. The predicate is never narrowed to binary16: the narrowing is lossy, and a probe binary16 cannot represent would round to a neighbouring value and prove the wrong one absent. Widening the entries instead leaves such a probe matching nothing, which is what a full scan finds too. This is also why bloom filters skip `FLOAT16` — they hash the 2-byte stored form, so they have no way to probe without narrowing.

A dictionary can only prove absence, so it never upgrades a decision to `ALWAYS_MATCHES`. Statistics, bloom filters and dictionaries are independent: whichever proves no match first drops the row group.
