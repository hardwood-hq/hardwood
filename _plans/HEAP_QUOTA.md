# Plan: a heap quota per HardwoodContext

Tracking issue: #1859. Related: #1841 (an `OutOfMemoryError` fails the read instead of hanging it), #1842 (direct memory on S3, outside this plan's heap quota).

A prototype of stages 1 and 2 is on the branch `1841-heap-quota-wip`: full `verify` green, unreviewed, not squashed. Its design is what [Target model](#target-model) and [Folding the working set into the ledger](#folding-the-working-set-into-the-ledger) describe.

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
| A binary dictionary's interned strings | one `String` per entry, not charged | per dictionary |

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

## Learned in the prototype

- **What is not bounded in full holds back what follows it.** A page whose size is not known in full (not found yet, header not arrived, `DELTA_BYTE_ARRAY` values not yet learned) reserves nothing for that part, so a decode ahead of it can take the room its forced decode then needs, and whether the read fails depends on timing. Such a page therefore holds back every decode after it in frontier order, until its own decode has ended. The order must be total (key, then the order found): with "at or below my key", the pages of a long nested record, which share a key, held each other back and the read stalled.
- **R's count lags the read's releases.** A read returns released bytes to R after it lets go of its lock, so a decision under the lock subtracts what it has returned and not yet handed back.
- **Numeric dictionaries die with their chunk's decode.** A numeric column's batches hold the values they gathered, not the dictionary, so charging it until the batch spanning two row groups is taken holds two dictionaries per column for nothing.
- **Charging at allocation needs a claim.** Allocation sites run on several threads at once, a cell's arrays are allocated by whichever page's decode reaches it first, and a recycled holder brings arrays charged earlier, so a decode's charge must be admitted as a bound before it starts and settled to what its own allocations counted as it ends.
- **"Everything charged" cannot be shown by inspection.** The interned-string cache, which nothing charges, was missing from this plan's own threat model. The stage 2 gate needs a leak check: the threat model's shapes and the fuzz corpus read in a child JVM, a heap dump at a read's peak, and what the reader's objects retain held to the quota plus a per-object allowance.

## Open

- **Small heaps.** At 64 MiB, reads under the default quota still ran out of memory (stage 1, `HeapExhaustionTest` with the quota in force): memory charged nowhere yet dominates there. Re-measure after stage 2.
- **The cost near the limit.** Stage 1 as first built was ~5 % slower than `main` on the 2016 taxi file at 512m and within noise from 680m; not yet measured on the current design.
- **Free-holder races.** A decode that takes a free holder another decode's claim counted on makes that one draw past its claim until the first settles: the ledger overstates by at most a holder for that window.
- **Nested columns' unfound pages.** Only flat columns have a frontier entry for the pages the read has not found yet, so a decode ahead can take room a nested column's next page needs.
- **Not charged yet.** Decode tasks within `DecodeBuffers.RETAINED_BYTES` and the buffer sets each thread keeps; the first window of a dictionary batch, which its consumer builds lazily; per-object overheads (headers, pointer arrays, the per-page objects of a read). Memory the consumer owns (detached batches, returned strings and arrays) is outside the quota by definition.

- **Pages decoded through levels.** A nested page's batches depend on its records, which its
  header does not give; the reserve and charge take an upper bound from its value count.

## Stages

| # | Stage | Gate before the next stage |
| --- | --- | --- |
| 0 | An `OutOfMemoryError` on a Hardwood thread reaches the reader's thread instead of hanging the read | done in 1.1 (#1841) |
| 1 | Current charges and the reserve on the flat path: a decode claims its page's bound and settles it to what the batch holders' arrays gain and lose (`AllocationTally`); one frontier of unstarted pages per column, with the pages not bounded in full (not found, header not arrived) blocking decodes past them; free holders earmarked by the claims that count on reusing them; recycling exchanges no longer preallocate | prototype on `1841-heap-quota-wip`; the N300 numbers of stage 1 as first built (+2.5 % cold single read) predate this design and are redone at stage 4 |
| 2 | The working set folded into the ledger: dictionary and nested pages in the frontier with kept and task bounds; the reserve adds the largest task bound; the forced decode takes its buffers and claim from the reserve or fails; dictionaries and nested pages ride on the batch whose rows reach their end, a numeric dictionary returned once its chunk has decoded; `DELTA_BYTE_ARRAY` values drawn past the claim, a decode ahead giving up and its page staying unknown until a decode learns it; `WorkingSet` left to decoding outside a read | prototype on `1841-heap-quota-wip`; gate: the leak check above, over the threat model's shapes at heaps from 128m to 1g, concurrent reads included, and the fuzz corpus |
| 3 | The public option on the context builder, user docs, internal properties removed | docs reviewed |
| 4 | Performance | flat, nested and 1BRC benchmarks neutral on the container and the N300 |

The PR completing stage 4 folds the end state into VALUE_DECODE.md and READ_PIPELINE.md and
deletes this plan.
