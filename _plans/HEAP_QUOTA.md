# Plan: a heap quota per HardwoodContext

Tracking issue: #1841. Related: #1842 (direct memory on S3, outside this plan's heap quota).

Hardwood will never cause an `OutOfMemoryError` by itself: everything a `HardwoodContext`
holds on the heap, across all of its reads, is charged against one quota, and a read whose
needs do not fit fails with a `ParquetReadException` before it allocates. The application
keeps the rest of the heap: if its own use plus the quota fits the heap, no file, however
crafted, and no number of concurrent reads can exhaust it through Hardwood.

## Threat model

A service reads Parquet files it does not trust, possibly several at once on one context. A
file must not be able to take down the JVM.

| Shape | Bounded today |
| --- | --- |
| A page or dictionary declaring a huge size (bomb) | yes: declared sizes are checked against the working-set limit before allocating |
| Many columns of moderate pages (50 `INT64` columns of 20 MiB pages) | only by the working-set limit, which defaults to the whole heap and trips after the heap is full: `OutOfMemoryError` from 25 columns at 512m |
| Many concurrent reads, each moderate | no: the working-set limit is per read, so N reads hold up to N heaps |
| A record spanning many pages; large dictionaries | charged to the working set, so as the second row |
| Pages decoded ahead of the consumer, the next row group's dictionaries | no: outside the working set (VALUE_DECODE.md, Boundaries) |
| Huge footers and metadata | yes, bounded at parse (FILE_METADATA.md) |
| Large S3 ranges | direct memory, #1842 |

The second and third rows are the reason for a quota per context rather than a limit per read.

## Today

| Holder | Bound | Scope |
| --- | --- | --- |
| A read's working set W: dictionaries, the largest page's batches per column, one task's buffers | `hardwood.internal.maxWorkingSetBytes`, default the whole heap | per read |
| Read-ahead R: ranges fetched ahead, decodes and dictionary parses above `DecodeBuffers.RETAINED_BYTES` beside the one the consumer needs, pruning parses | `hardwood.internal.readAheadBytes`, default 25 % of the heap | per context |
| Buffers each decode thread keeps between tasks | `DecodeBuffers.RETAINED_BYTES` per set, not charged | per context |
| Pages decoded ahead of the consumer | the look-ahead, in rows, not bytes | per read |

Measured on `yellow_tripdata_2016-01.parquet` (151 MB, one row group, pages of up to 4.2M
values written by Arrow 7), full row read: the working set charges 381 MiB; the smallest
heap that reads it is 371 MiB on a context of zero threads and 418 MiB on the default pool.
Live bytes at the read's peak are 342 MiB on zero threads and 385 MiB on the pool; the pool's
extra is charged to R. A nested read (`list<int64>`) and a flat read with small pages charge a
few MiB against 40 to 60 MiB live: the batches decoded ahead of the consumer on the pool, ~48 MB
by the look-ahead's estimate, and the pages nested cells keep until their batch is published,
are charged nowhere, since a decode within `DecodeBuffers.RETAINED_BYTES` charges R nothing.

## Target model

One ledger per `HardwoodContext` holding a quota Q, by default 75 % of the maximum heap,
configurable on the context's builder. Every holder in the table above charges the ledger
what it holds now: before it allocates, and returning it as it lets go. A page's batches are
charged as its decode starts and returned batch by batch as each is published; a dictionary
while a batch holds rows of its chunk; a task's buffers while it runs; a range from its fetch
until the batches decoded from it are published. Nothing is charged as a high-water mark, so
the ledger's total tracks what the context holds and is an upper bound of it.

Charges are of two kinds:

- **Needed**: what the consumer's next batch cannot be produced without: each column's pages
  covering it, their dictionaries, the task decoding them. The decode or fetch that need
  forces.
- **Chosen**: what a read holds ahead to keep its consumer fed: ranges fetched ahead, pages
  decoded ahead, decodes and parses running beside the needed one.

**Reserve.** Each read keeps free in the ledger what need could force next: per column, the
first page not yet decoded or admitted, and the dictionary pages of a row group the frontier
enters. Decodes ahead run in key order, so where they stop, the next forced decode is exactly
that frontier. Its sizes come from the page headers the chunk's scan reads before decodes are
admitted; a frontier page whose header has not been read counts its compressed size times its
codec's expansion bound.

**Admission.** A chosen charge starts only where the ledger holds it beside everything charged
and every read's reserve: `held + charge + reserves <= Q`. Otherwise it waits for a release.
A needed charge is taken from the read's reserve, so it always fits; a needed charge larger than
the reserve the read could keep, which only a read alone on the ledger meets, fails the read
with `ParquetReadException` naming the page, what it needs and the quota. A needed charge never
waits and nothing is evicted.

Whether a read alone on its context reads or fails then depends on the file and Q: it fails
only when the pages a batch needs, beside what that batch's rows hold, exceed Q. Decoding ahead
uses whatever the reserves leave of Q, at every heap size. Concurrent reads share Q, so whether
one of them fits depends on what the others hold and reserve.

The retained decode buffers are charged as held, so a context whose threads keep large sets
leaves less of Q to its reads.

## Folding the working set into the ledger

The reserve keeps the invariant `held + reserves <= Q` only for the decodes it covers, so every
decode a read can be forced into joins its column's frontier: flat data pages, nested data
pages and dictionary pages. A frontier page carries two bounds: what its decode keeps past its
end, and the buffers its task holds while it runs.

- **Reserve.** Per read, the kept bound of each column's first frontier page, less the free
  holders no running claim counts on, plus the largest task bound among those pages: one decode
  is forced at a time.
- **Task buffers.** Every decode above `DecodeBuffers.RETAINED_BYTES` charges them as it starts,
  the forced one included, and returns them as it ends. A decode beside the forced one that does
  not fit waits for a release; the forced one fits by the invariant or fails the read.
- **Flat batches.** A claim settled to the decode's allocation tally (stage 1).
- **Dictionaries.** A dictionary page's kept bound is its entries, from its header; the parse
  claims it and the column holds it until the consumer takes the last batch with rows of its
  chunk. A dictionary parsed for pruning is charged as it is parsed and held the same way.
- **Nested pages.** The kept bound is the page's batches, levels and offsets from its value
  count (`WorkingSet.Column#keptPage` today), claimed by the decode and held until the consumer
  takes the last batch with values of the page.
- **Reconstructed values.** A `DELTA_BYTE_ARRAY` page's reconstructed bytes are known once its
  decode has read the prefix and suffix lengths, before it allocates them. The decode draws them
  past its claim where they fit beside the reserves. Otherwise a forced decode fails the read, and
  a decode ahead of need gives up, frees its buffers and leaves its page in the frontier with the
  exact bound it learned, so it starts again when that fits or need forces it.
- **What goes.** The working set as a per-read limit, `batchesOnLedger`, the forced decode's
  uncharged buffers, and the count of running tasks' buffers that decided whether a needed page
  waits or fails. `WorkingSet` stays for decoding outside a read (the metadata tools).

## Decisions

1. **Scope:** one quota per `HardwoodContext`. Decided.
2. **Default:** 75 % of the maximum heap. Decided.
3. **Needed and chosen charges:** current holdings against one Q, with a reserve per read for
   what need could force next (Target model). Decided. Rejected: a fixed split of Q between
   needed and chosen (a file whose pages hold millions of values lost its decode-ahead, ~10 %
   slower at heaps it fits in), failing or waiting a needed charge that chosen holdings crowd
   out (timing-dependent, or deadlocking on the read's own read-ahead), and evicting chosen
   holdings (the reserve keeps the room free instead).
4. **Public option:** Q only, on the context builder, as bytes (`heapQuota(long)`) or as a
   share of the maximum heap (`heapQuota(double)`), which follows the heap when `-Xmx` changes;
   the internal properties `maxWorkingSetBytes` and `readAheadBytes` go. Decided.

## Open

- **Small heaps.** At 64 MiB, reads under the default quota still ran out of memory (stage 1, `HeapExhaustionTest` with the quota in force): memory charged nowhere yet dominates there.
- **The cost near the limit.** On the 2016 taxi file the reserve holds back decodes ahead at 512m, ~5 % slower than `main`; at 680m and above the difference is within noise (~1.5 %, not yet attributed: profile at stage 4).

- **Pages decoded through levels.** A nested page's batches depend on its records, which its
  header does not give; the reserve and charge take an upper bound from its value count.

## Stages

| # | Stage | Gate before the next stage |
| --- | --- | --- |
| 0 | An `OutOfMemoryError` on a Hardwood thread reaches the reader's thread instead of hanging the read | done (5f67dfbbd) |
| 1 | Current charges and the reserve, on the flat path: a page's batches charged from its decode until the consumer takes them (a recycled holder until the reader closes), trued up to the arrays held; the working set's flat batches moved to the ledger; the reserve per read over found and queued pages; decodes ahead admitted by the rule, not past a column whose next page is not found; pages below need charged from the reserve, waiting for running tasks' buffers | done: charges equal live batch arrays in heap dumps of the taxi read; N300 taxi read +4.9 % at 512m, +1.3 % at 680m, +1.7 % at 1g against `main` |
| 2 | The nested path, dictionaries, tasks within the retention size and the buffer sets each thread keeps, the holders an exchange preallocates, and a binary page whose header has not arrived on a remote read (decodes ahead wait for it) on the ledger; the R property becomes Q | the shapes of the threat model under a child JVM: each either reads or fails with `ParquetReadException`, never `OutOfMemoryError`, at heaps from 128m to 1g, including several concurrent reads on one context |
| 3 | The public option on the context builder, user docs, internal properties removed | docs reviewed |
| 4 | Performance | flat, nested and 1BRC benchmarks neutral on the container and the N300 |

The PR completing stage 4 folds the end state into VALUE_DECODE.md and READ_PIPELINE.md and
deletes this plan.
