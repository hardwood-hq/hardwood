# Plan: bounds on untrusted input

Tracking issue: #610. Stage issues: #201, #860, #571, #1644, #1630. Related: #604 (negative sizes,
closed), #894 (Thrift malformed-input policy, closed), #1095 (corrupt page data read as values),
#1135 (bloom filter or dictionary region outside the file).

Hardwood reads files it did not write. A file can declare any value in any field, so no
allocation may be sized by a wire value before something has bounded it. This plan lists every
`parquet.thrift` field that sizes or counts something, states the bound each is held to, and
orders the changes that close the fields still unbounded.

## Target model

Every size, count or length taken from a file reaches an allocation only once one of these bounds
holds:

| Bound | Holds for |
| --- | --- |
| The bytes present: the Thrift buffer's remainder, the enclosing region, the file | every list count and binary length of the footer, page index and page headers; every offset and region length the footer declares |
| The bytes decoded: a count is checked against the bytes that must back it | dictionary entries (`numValues × width ≤ length`; 4 bytes per `BYTE_ARRAY` entry) |
| The page limit: `hardwood.internal.maxPageBytes`, default 256 MiB | a page's uncompressed size; a page's value count, at 4 bytes per value |
| The batch: `BatchSizing.MAX_BATCH` | rows and values per batch, whatever a row group or chunk declares |

A file breaking a bound fails with `ParquetReadException` naming the field and the bound, before
the allocation. The page limit is an internal system property, read on each check like
`hardwood.internal.sequentialChunkSize`: it is the escape hatch for a legitimate page above
256 MiB (a single binary value that large, or a writer that cuts pages by size alone and RLE-
encodes a constant column) and covers the CLI as well as the library.

The page limit bounds each allocation, not their sum: a page within it is decoded whole, and
the allocations of many columns are live at once. A file whose pages exceed the limit, such as
DuckDB's `42.parquet`, fails with a controlled error and is not read; no writer produces such
pages, so the reader does not decode pages in windows to read them. Bounding the sum is stage 6,
whose budget supersedes the page limit: the limit is an interim bound, fitted to no heap, that
also refuses legitimate pages larger than it, such as one holding a single large binary value.

## Fields

| Field | Feeds | Bound | Stage |
| --- | --- | --- | --- |
| every `list<…>` count, every `binary` length | collection pre-size, `byte[]` | bytes remaining in the Thrift buffer (#894) | done; #860 test in stage 1 |
| footer length | footer buffer | file size | done |
| `ColumnMetaData.total_compressed_size`, `data_page_offset`, `dictionary_page_offset` | chunk fetch | file size (`ChunkHandle`, `SharedRegion`) | done |
| `ColumnChunk.offset_index_offset/length`, `column_index_offset/length` | index region fetch | file size | done |
| `ColumnMetaData.bloom_filter_offset/length`, `BloomFilterHeader.numBytes` | bloom filter fetch | file size (message: #1135) | done |
| `PageHeader.compressed_page_size`, `PageLocation.compressed_page_size` | page slice | the fetched chunk | done |
| `DataPageHeaderV2.definition/repetition_levels_byte_length` | level slice | the page | done |
| `DictionaryPageHeader.num_values` | dictionary arrays | the bytes decoded | done |
| `SchemaElement.type_length` | FLBA width | positive (schema validation); dictionary checked against its bytes; batch values in bounded chunks | done |
| `RowGroup.num_rows`, `ColumnMetaData.num_values`, `SizeStatistics` | batch sizing | `MAX_BATCH` | done |
| `PageHeader.uncompressed_page_size` | decompression buffer, `scratch.bytes` | the page limit | 2 |
| `DataPageHeader.num_values`, `DataPageHeaderV2.num_values` | level and dictionary-id scratch arrays | the page limit; at most the chunk's `num_values` | 3, 4 |
| RLE/bit-packing run header, `DELTA_BINARY_PACKED` header counts | run length, block layout | `int` range | 5 |
| `TIME` stored value | `LocalTime` | the units of one day | 5 |

`DictionaryPageHeader.num_values` is not held to the chunk's `num_values`: a writer that stores
an Arrow dictionary array writes its unused entries too.

## Stages

Stages 1–5 land as one PR, a commit per field or site.

### Stage 1: #860 regression test

The list-header bound landed with #894 (`824d2080f`). Pin the issue's byte sequence
(`89 FC 80 80 80 80 08 00`, a `key_value_metadata` list declaring `2^31` elements) in
`MalformedMetadataValidationTest`.

### Stage 2: #201 page size

`PageHeaderReader` rejects an `uncompressed_page_size` above the page limit, through a bounded
counterpart of `ThriftCompactReader.readNonNegativeI32`. Every consumer (`PageDecoder` V1 and V2,
`DictionaryParser`, the CLI) reads headers through it. The property's resolution and its
rejection of an unparseable value follow `SequentialFetchPlan.defaultChunkSize`. Describe the
page limit in `VALUE_DECODE.md`.

### Stage 3: #610 page value count

`DataPageHeaderReader` and `DataPageHeaderV2Reader` reject a `num_values` above the page limit
divided by 4, the bytes of one `int` scratch entry. A hand-built file with the shape of DuckDB's
`42.parquet` (a one-entry dictionary and data pages of `2^31 − 1` values in a single RLE run)
fails with `ParquetReadException` instead of `OutOfMemoryError` at `LevelScratch.ids`. The
writer cuts its pages at the default count (`ColumnChunkBuffer.pageCut`), which only binds for a
caller who raises both `pageTargetBytes` and `pageTargetRows`, and clamps its byte target to half
the page limit, since the cut does not charge a page's framing. A single record of more entries
or bytes is still one page.

### Stage 4: #610 page against chunk

`PageDecoder` rejects a data page whose `num_values` exceeds its column chunk's. It catches a
file whose layers disagree, at no cost; it does not bound a file that inflates every layer
together, which stage 3 does.

### Stage 5: #571 arithmetic

- A run header of the RLE/bit-packing hybrid is a varint narrowed to an `int`: a bit-packed
  group count past `Integer.MAX_VALUE / 8` and an RLE count past `Integer.MAX_VALUE` wrap, to a
  negative run the decoder never finishes or to a shorter one that misreads the page. Refuse
  both, and a varint longer than 64 bits, in `RleBitPackingHybridDecoder` and
  `PageRecordCounter`; `FixedSizeListDetector` declines such a stream.
- `DeltaBinaryPackedDecoder` accumulates its header counts into an `int`, so a block size of
  `2^32 + 128` reads as 128. Read them as `long` and refuse them past the `int` range.
- `LogicalTypeConverter.longToTime` multiplies a `MICROS` value by 1000 unchecked, so a stored
  value past the day can wrap into one inside it. Reject a stored value outside `[0, units per
  day)` with `DateTimeException` naming the stored value and unit, as `LOGICAL_TYPES.md` does for
  out-of-range timestamps. `TIME(MILLIS)` is legal on `INT32` only since #1239, so its product
  cannot overflow.
- `DELTA_BINARY_PACKED`: the spec pads the last miniblock to full length, and the decoder
  requires those bytes. Pin the rejection of a truncated last miniblock with a test, and record
  the spec text on #571.

### Stage 6: total decode memory

The page limit bounds each allocation; what a read holds at once is their sum, and part of it
grows with the projection rather than the pool:

| Allocation | Bounded by | Grows with |
| --- | --- | --- |
| flat level and dictionary-id scratch, decompression output | the page limit | the pool's threads |
| dictionaries | the page limit, through their bytes | projected columns × row groups read ahead |
| nested page scratch and decompressed bytes held until a page's cells fill | the page limit | projected nested columns × pages in flight |

Compression makes a declared size real: a dictionary page of 256 MiB of zeros takes a few KiB on
disk, so a file of a thousand such columns asks for 256 GiB from kilobytes. Charge the
allocations sized from file-declared values against a decode-memory budget, reserved before
each is made and released when it is done. That covers the transient ones as well as those that
outlive their task: the per-thread decompression output and level and dictionary-id scratch are
bounded by the pool's threads only through the page limit, so a budget that leaves them out
leaves threads × one large page unbounded once the limit goes. A single allocation larger than
the whole budget fails the read with `ParquetReadException`; reservations that together exceed
it wait for others to be released rather than fail, the way the read-ahead budget of #1397
holds back fetches, so the outcome for a file depends on the heap only when one of its
allocations alone exceeds the budget.

Measure first: a many-column file of dictionary pages at the limit, and the heap a read of it
reaches. The budget's home and default (per read or per context, a share of the heap) are open.

When the budget lands, the page limit goes, or stays only as a check that a declared size fits
a Java array. The writer's page cuts at the default limit (`PageLimit.DEFAULT_MAX_PAGE_VALUES`
entries, a byte target clamped to half the limit) exist only to match it and are revisited with
it; whether the writer caps its dictionary pages, as parquet-java, Arrow and parquet-go do at
their dictionary page size limits, is a question of encoding selection, not of a reader bound.
Tracked in #1644.

### Stage 7: #1630 fuzzing

Jazzer targets over the footer, page headers and page index, in regression mode in the PR build
and fuzzing mode on a schedule. Seed the corpus with the stage 3 file and parquet-testing's
`bad_data`. Coming after stages 2–5, every out-of-memory a target reports is a new finding.

## End state

`VALUE_DECODE.md` (page limit, page-against-chunk check) and `FILE_METADATA.md` (the Thrift
policy, which already states the remaining-bytes bound) describe every bound in the target model.
This plan is deleted in the PR that completes stage 7.
