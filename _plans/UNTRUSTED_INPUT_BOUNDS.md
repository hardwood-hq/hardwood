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
| The page limit: the longest Java array (`PageLimit`) | a page's uncompressed size; a page's value count, less one for its offsets |
| The working-set limit `W` and the read-ahead buffer `R` (stage 6) | what a read holds at once; its large tasks beside its first |
| The batch: `BatchSizing.MAX_BATCH` | rows and values per batch, whatever a row group or chunk declares |

A file breaking a bound fails with `ParquetReadException` naming the field and the bound, before
the allocation. A legitimate large page (a single binary value of hundreds of MiB, or a writer
that cuts pages by size alone and RLE-encodes a constant column) reads wherever `W` allows it;
the internal property `hardwood.internal.maxWorkingSetBytes` is the escape hatch, and it covers
the CLI as well as the library.

The page limit keeps each size a header declares within the array it sizes, so that no arithmetic
sizing an array overflows; a page past it, such as one of DuckDB's `42.parquet` (2^31 - 1
values), fails with a controlled error and is not read. What a read holds at once, the sum over
its columns and tasks, is the working-set limit's and the read-ahead buffer's (stage 6): a page
within the page limit is read wherever the working set it brings fits, and the reader does not
decode pages in windows to read a page larger than that.

## Fields

| Field | Feeds | Bound | Stage |
| --- | --- | --- | --- |
| every `list<…>` count, every `binary` length | collection pre-size, `byte[]` | bytes remaining in the Thrift buffer (#894) | done; #860 test in stage 1 |
| footer length | footer buffer | file size | done |
| `ColumnMetaData.total_compressed_size`, `data_page_offset`, `dictionary_page_offset` | chunk fetch | file size, and no overlap with another chunk the read reads (`FetchPlanBuilder`) | done |
| `ColumnChunk.offset_index_offset/length`, `column_index_offset/length` | index region fetch | file size | done |
| `ColumnMetaData.bloom_filter_offset/length`, `BloomFilterHeader.numBytes` | bloom filter fetch | file size (message: #1135) | done |
| `PageHeader.compressed_page_size`, `PageLocation.compressed_page_size` | page slice | the fetched chunk | done |
| `DataPageHeaderV2.definition/repetition_levels_byte_length` | level slice | the page | done |
| `DictionaryPageHeader.num_values` | dictionary arrays | the bytes decoded | done |
| `SchemaElement.type_length` | FLBA width | positive (schema validation); dictionary checked against its bytes; batch values in bounded chunks | done |
| `RowGroup.num_rows`, `ColumnMetaData.num_values`, `SizeStatistics` | batch sizing | `MAX_BATCH` | done |
| `PageHeader.uncompressed_page_size` | decompression buffer, `scratch.bytes` | the page limit (the longest array); the working-set limit | 2, 6 |
| `DataPageHeader.num_values`, `DataPageHeaderV2.num_values` | level, dictionary-id and offset scratch arrays, batches | the page limit (the longest array, less one); at most the chunk's `num_values`; the working-set limit | 3, 4, 6 |
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

### Stage 6: working-set limit and read-ahead buffer

#1644. The page limit bounds each allocation; what a read holds at once is their sum, and part of
it grows with the projection rather than the pool. Measured on the container (JDK 25, G1,
12 GiB heap) with `RleExpansionFile.buildWide`, INT64 columns whose GZIP dictionary pages
declare 256 MiB of zeros in about 260 KB each:

| File | Threads | Live heap at peak | Held after the read, context open |
| --- | --- | --- | --- |
| 8 columns, 1 row group | 1 / 4 / 8 | 2.4 / 3.2 / 4.2 GiB | 0.26 / 1.0 / 2.1 GiB |
| 16 columns, 1 row group | 8 | 6.4 GiB | 2.1 GiB |
| 8 columns, 3 row groups | 8 | 6.4 GiB | 2.1 GiB |
| 40 columns, 1 row group (10 MB) | 8 | `OutOfMemoryError` at 12 GiB | |
| 8 columns, one data page of 64 Mi values each (1.1 KB) | 1 / 8 | 4.2 / 5.4 GiB | 0.26 / 2.1 GiB |

A read holds a dictionary per projected column and chunk, the next row group's as well once its
planning has run, and each thread that decoded keeps its largest decompression output and
level and dictionary-id scratch until the context closes. A data page's task fills every batch
the page covers, so a page of 64 Mi `INT64` values holds 512 MiB of batches until the consumer
takes them, and the recycling exchange keeps the holders until the reader closes. Nested page
scratch, up to `MAX_INFLIGHT_PAGES` per nested column, is not measured.

#### Goals

- **Safety.** No file makes a read allocate without bound: a file asking for more than a read
  accepts fails with `ParquetReadException` before the allocation, never with an
  `OutOfMemoryError`.
- **Throughput.** A read works far enough ahead to hide the store's latency and keep the decode
  threads busy, and no further: a slow consumer, `head(N)` or an early exit does not make it
  hold or fetch more.
- **Predictability.** Valid files read, and whether a read succeeds depends on the file and the
  configuration, not on timing or on other reads.

#### Two kinds of memory

| Kind | What it holds | Decided by | Released when |
| --- | --- | --- | --- |
| Working set | per projected column: the entries of the dictionaries of every chunk the batch it fills has rows of, more than one where a batch spans row groups; the batches of the page holding the rows of the batch the column publishes next (`ReadScheduler`'s first submission rule), and for a nested column the page's decompressed bytes and levels; the fetched range that page is read from (a `RangeRead` of priority `DEMAND`). Once per read: the buffers of one task decoding a page, its decompressed bytes and level and dictionary-id scratch | the file's declared sizes | the consumer takes the page's last batch; the column moves past the chunk or the range; the task ends |
| Read-ahead | everything else: speculative `RangeRead`s (the next chunk, region, row group and file), pages inside the scheduler's cap, the task buffers of pages decoded at once beyond the first, the next row group's dictionaries, dictionaries parsed only to prune | the reader | the read reaches it, which moves it into the working set, or abandons it |

Safety is a bound on the working set, throughput a bound on read-ahead. The working set is not
optional: short of decoding pages in pieces, a read cannot produce its next batch with less.
Read-ahead is: skipping it costs speed, not results.

Memory is counted where Hardwood allocates it, on the heap or off it: an S3 range arrives in a
direct buffer, decoded values on the heap. A range of a mapped or in-memory file
(`HeldBytes.inMemory`) is a view of memory that exists already and counts nothing.

#### Working-set limit

A read's working set is held to a limit `W`, checked before each dictionary and data page is
decompressed. Each column counts the most dictionary entries one of its batches holds and the
largest page it has met in the read, and the read the largest task buffers once; `W` holds their
sum. A batch holds the dictionary of every chunk it has rows of, as their pages decode with them,
so a batch spanning row groups holds several: a column sums the dictionaries of the chunks whose
rows end after the batch holding its current chunk's first row starts, from the read's batch
plan. A dictionary pruning parsed counts when the column takes it. The sum bounds what the read must hold at
any one time from above and does not depend on how far apart the columns run, so the outcome
depends on the file and `W` alone, not on timing, the context's threads or other reads. A read
whose working set would exceed `W` fails with `ParquetReadException` naming the column, the page,
what it needs, the working set it would reach and `W`; a single page larger than `W` is the
simplest case. The check holds nothing back and waits for nothing: it is a bound on the file, the
page limit taken from one allocation to the sum of what a read must hold at once.

Counting each column's task buffers, or the task buffers once per thread, would reject valid
files that read today: the NYC taxi file of January 2016 (19 columns, one page per row group of
5.4 M values in its widest columns) estimates more than twice as much that way as the 439 MB it
estimates with the task buffers counted once, and reads on a heap of 512 MiB. The estimate errs
high, so `W` defaults to the whole heap: a read whose estimate exceeds it could not run on that
heap anyway. At 75 % of the heap it refused the taxi file on a heap of 512 MiB, which reads
there. With the whole heap the taxi file reads from 512 MiB up with `W` as without it, and below
that it runs out of memory either way, before its columns have charged enough to cross `W`. `W`
fails what declares far more than the heap holds, such as the crafted files above, not reads
near the heap's size.

`W` is per read. Working-set memory is released only as a consumer reads, and Hardwood does not
know in which order a caller reads its readers: a thread merging two readers blocks in one while
the other holds the memory, and on a context of zero threads the thread that would release is the
one waiting. A limit shared by a context's reads would either fail a valid read because of
others or make reads wait for each other, which deadlocks in those cases.

Read-ahead moves into the working set when the read reaches it (`RangeRead.raiseToDemand`, the
first page of a column's next batch, the dictionary of the chunk the column enters), and is
checked against `W` then, with the outcome a demand allocation would have had.

A buffer kept for later pages (`DecodeBuffers` slots, the level scratch of a flat column's task
and of a nested column's pool) is kept only up to a retention size (`DecodeBuffers.RETAINED_BYTES`)
and dropped above it once its page is done. A context then keeps at most threads × retention size
after a read, where it kept each thread's largest page. The exchange's free batch holders and a
nested column's pool belong to the read and go with it.

#### Read-ahead buffer

A context's reads hold their read-ahead within one buffer `R`. Read-ahead starts only while it
fits `R`; otherwise it is not started and is considered again on the next release, from the
places that already re-run on one: the scheduler's `submit` after a task ends or a batch is
published, and the walk that would have used the range. Nothing waits for `R`, so it can be
shared by every read on the context without the deadlock a shared `W` would bring.

Each read also stops at what its throughput needs, whether or not `R` has room: the scheduler's
decode cap (`CAP_BUDGETS` batch byte budgets past the read's first unpublished row), and #1264's
fetch target, which replaces the one-ahead rules of `ChunkHandle`, `SharedRegion`, the next row
group and the next file. The fetch target is counted in bytes held, about the store's bandwidth
× latency, because a unit of the file weighs whatever the file makes it: a row group of 1 GiB or
of 1 KiB is one ahead alike. A slow consumer stops a read at its targets; `R` binds when many
reads, or a file's large declared sizes, make the read-ahead within the targets large.

A read whose consumer stalls keeps its share of `R` full, and the context's other reads read
ahead less: they slow down but do not stop. A cap per read on its share of `R` would prevent that;
it is not added until a shared-context measurement asks for it.

#### What a process holds

The sum of its reads' working sets, each at most `W`, plus one `R` per context. Valid files keep
working sets of a few MiB per column, far below `W`. N concurrent reads of crafted files can
still hold N × `W`: a bound across reads needs them to wait for each other, which the working set
rules out.

#### Defaults

`W` defaults to `Runtime.maxMemory()` (see [Working-set limit](#working-set-limit)) and `R` to a
share of it, which is also the JVM's default bound on direct memory; the share is fixed by PR 2's
measurement. The internal properties
`hardwood.internal.maxWorkingSetBytes` and `hardwood.internal.readAheadBytes` override them.
Neither is a `ReaderConfig` option until a user needs one; the CLI reads with the defaults. The
throughput targets are fixed thresholds, not settings.

#### The page limit and the writer

With `W`, the page limit goes, both its halves: `hardwood.internal.maxPageBytes` on a page's
uncompressed size, and the `maxPageBytes / 4` (64 Mi by default) cap on a data page's
`num_values`. A page is read when the working set it brings fits `W`, whatever its size or
count. At header parse a check remains that a declared count fits a Java array of the widest
type it sizes. The error for an oversized page names `W` instead of the page limit, and the
`DeclaredValueCountTest` messages change with it.

A page's task fills every batch the page covers, so its batches count in full: a page of
64 Mi `INT64` values brings 512 MiB of working set, and reads where `W` allows that.

The writer cuts pages at the reader's value bound and clamps its byte target to half its byte
bound, so that it writes no page a reader refuses; with the bounds at the longest array, both
bind only where a caller raises both page targets past anything a reader could hold. The byte
target bounds a page's size, but not its value count where the values compress to almost
nothing, such as a constant column RLE-encoded; the record target cuts those at its default.

#### PRs

1. **Working-set limit.** `WorkingSet` on the read's `RowGroupIterator`, charged by the fetch
   plans' dictionaries and the columns' page decoders; code decoding outside a read charges a
   working set of its own per column, and a read charges a dictionary pruning parsed once its
   column takes it. Each column's dictionaries count per batch, from the batch plan. Retention
   sizes. Tests on `RleExpansionFile.buildWide`: a read whose working set fits `W` passes, on row
   groups in batches of their own too; row groups sharing a batch count each dictionary, pruned or
   parsed; one past it fails with the
   message, on a zero-thread context, whose order makes the crossing charge the same each run;
   buffers above the retention size are not kept. A/B against main on the container and the N300:
   the narrow, flat and 1BRC presets.
2. **Read-ahead buffer, decoded side.** The context's account, charged with the task buffers of
   a page or dictionary decoded while another of the read decodes, for tasks above the
   retention size (a context keeps that much per thread anyway); one that does not fit is
   deferred until a task of the read ends. A read always has its one task of `W`, so deferring
   never stalls it. The indexed walk parses each data page's header to size its task. Tests:
   the admission rules; reads with `R` at 0 on crafted large dictionaries and pages return
   every row on 0, 1 and 4 threads. Measured with the probe (8 columns of 256 MiB dictionaries,
   8 threads): one row group reads on a 3 GiB heap, where PR 1 runs out of memory, and its live
   peak at 5 GiB drops from 4.2 to 2.6 GiB; three row groups in one batch read on 7 GiB, where
   PR 1 runs out of memory.

   The scheduler's pages ahead and the dictionaries of chunks a column reaches early are not
   charged. A first version charged them and measured no change: with `R` at 0, live peaks on
   the crafted files stayed where they were (8 columns, 8 threads: 5.9 GiB on three row groups
   in one batch, 3.9 GiB on 64 Mi-value pages, 2.1 GiB on one row group), as the excess over
   `W` was the dictionaries of row groups sharing a batch, which PR 1 counts, and concurrent
   parses. They are charged with #1264, which adds the fetched read-ahead they ride on, once a
   measurement such as a slow consumer shows them to matter.
3. **The page limit goes**, both halves, down to the array bound, with the writer's matching
   cuts following it; `VALUE_DECODE.md` and `WRITER_ENCODING.md` describe the bound, and the
   roadmap lists stage 6. Tests: the `42.parquet` shape still fails at the header, on the array
   bound; a page of 64 Mi + 1 values passes its header and fails on `W`.

Fetched bytes are charged with #1264, demand ranges to `W` and speculative ones to `R`. #1264
puts the fetched side in the same buffer: a speculative read starts while it is
within the read's fetch target and fits `R`, and its context budget on bytes in flight is
subsumed by `R` and `W`. #1264 keeps split requests, the cap on parts per request, and the
context's requests in flight with demand ahead of speculative ones.

### Stage 7: #1630 fuzzing

Jazzer targets over the footer, page headers and page index, in regression mode in the PR build
and fuzzing mode on a schedule. Seed the corpus with the stage 3 file and parquet-testing's
`bad_data`. Coming after stages 2–5, every out-of-memory a target reports is a new finding.

## End state

`VALUE_DECODE.md` (page limit, page-against-chunk check, working-set limit, read-ahead buffer) and `FILE_METADATA.md` (the Thrift
policy, which already states the remaining-bytes bound) describe every bound in the target model.
This plan is deleted in the PR that completes stage 7.
