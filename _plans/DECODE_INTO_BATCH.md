# Plan: decode pages straight into batches

Tracking issue: #1287. Part of #1476 (its stage 3). Supersedes stage 6 of
[READ_PATH_CONVERGENCE.md](READ_PATH_CONVERGENCE.md) (#1024, #750, and the staging garbage of
#1045).

Every read today decodes a page into arrays of its own and then copies them into the batch on
the drain. This plan replaces that with one mechanism for every column kind (flat and nested,
fixed-width and binary), every read (filtered or not, capped or not) and both exchange modes:
a page's values, levels and bytes are decoded once, on the decode pool, into the arrays of the
batches they belong to.

## What it removes

| Column | Today, per value | After |
| --- | --- | --- |
| Flat fixed-width | page array allocated and zeroed, decoded into, copied into the batch on the drain | decoded into the batch |
| Flat dictionary binary | page `int[]` of entry ids allocated, copied into the batch's `dictIndices` | ids unpacked into the batch |
| Flat plain binary | one `byte[]` per value, appended into the batch's byte buffer | bytes copied once, page to batch |
| Nested | page arrays; per-element copy into growable accumulators (doubled as they fill); accumulators copied out at publish (`trimValues`, `Arrays.copyOf` of levels and offsets) | values, levels and bytes decoded into arrays allocated once at their exact size |

Plus, for Snappy through a direct buffer, the two copies around native decompression (#1015;
stage 5 below).

On the taxi flat scan the flat row alone was measured at 306 → 209 ms on eight threads (#1287,
prototype). On an Overture nested scan the append path is 26% of inclusive time (#1024).

## Design

A page goes through three steps.

| Step | Runs on | Does |
| --- | --- | --- |
| **Measure** | the decode pool, one task per page | Decompresses the page and decodes its level streams, and for a variable-length leaf its value lengths (the length prefixes of `PLAIN`, the length streams of the delta encodings). Produces the page's placement inputs and keeps a value decoder positioned at its first value. |
| **Place** | the drain, in page order | Cuts the page into segments of batches by the batch-close rules, and allocates a batch's arrays once their sizes are known. |
| **Fill** | the decode pool, one continuation per page | Decodes the page's values, levels and bytes straight into its segments, in order. |

### Measure

The measured page holds:

- **records kept**: the page's records after its row mask, as intervals of the page's records
- **values per record range**: for a column with repetition, the value position where each kept
  record starts (the positions of repetition level `0`); identity for a column without
- **bytes per value range**: for a variable-length leaf, the byte length of each value, so
  placement can size a batch's byte buffer and give each segment its byte range
- **all-present**: whether the definition levels are a single run of the maximum
  (`isSingleRleRunOf`), the gate that lets a batch keep `null` validity
- **fixed-size list `k`**: as `PageDecoder` stamps it today; placement then computes value
  positions arithmetically
- **the decoder**: decompressed bytes, level arrays (in the slot's `LevelScratch`), and a value
  decoder positioned at the first value, which the fill continues

A null placeholder and a boundary marker are not measured: a placeholder's records are known
from the `PageInfo` and it has no values, and a marker has no records.

### Place

The drain takes measured pages in sequence order and runs today's batch-close rules over their
records: batch capacity in records, file change, the row-group cut of reads with a binary
column, the always-match flag of filtered reads, and the row cap. The rules and the alignment
argument across columns are unchanged ([READ_PIPELINE.md](../_designs/READ_PIPELINE.md#when-a-batch-closes));
they now run over counts instead of over copied values.

Placement cuts each page into segments `(batch, record range, value range, byte range)`. A mask
with several intervals gives several segments in one batch. A placeholder gives a segment with
records and no values. A record never spans two batches, as today.

**Allocation.** A batch's arrays are allocated as soon as their sizes are known, each once and
at its final size:

| Array | Size known | Allocated |
| --- | --- | --- |
| values of a fixed-width leaf without repetition | at open: the capacity, or the rows its row group has left under the row-group cut | when the batch opens |
| values and levels of a leaf with repetition; record offsets | at close: the batch's value count | when the batch closes |
| byte buffer of a variable-length leaf | at close: the batch's byte count | when the batch closes |
| dictionary ids of a binary leaf | with its values | with its values |
| validity | at open | when the batch opens, only if a page placed into it is not all-present |

A batch that publishes short (end of input, row cap, file change) keeps arrays sized for the
batch as it was planned for a fixed-width flat leaf, as today; a batch allocated at close is
exact. Nothing grows and nothing is trimmed at publish.

On the recycling exchange (the row readers) "allocated" means taking a holder from the free
queue; a held array is reused when it is large enough and replaced at its exact size when it is
not. The drain blocks on the free queue as it blocks today, which is the back-pressure chain
unchanged.

### Fill

A page's fill task decodes into its segments in order, keeping the decoder's state from one
segment to the next. When the next segment's batch is not allocated yet (it closes later, or the
recycling pool has no free holder), the task records its position and ends; the drain
resubmits it when it allocates that batch. Every encoding is therefore filled by its sequential
decoder: no decoder has to seek.

The fill writes:

- **values**: typed kernels per encoding (dictionary lookup, `PLAIN`, `BYTE_STREAM_SPLIT`,
  delta), the present values at the positions the definition levels give them, only the kept
  value ranges of a masked page
- **dictionary ids** of a binary leaf, unpacked into the batch's id array; the batch's
  dictionary reference is set by placement, since a batch draws on one chunk's dictionary
- **bytes** of a variable-length leaf, into the batch's byte buffer at the segment's byte range,
  with each value's start and end
- **levels and record offsets** of a leaf with repetition, as the batch's index mode keeps them
- **validity**, with an atomic OR on the words at a segment's edges, which a page filled at the
  same time can share

A batch is complete when every segment placed into it is filled. The drain publishes complete
batches in order, after running what it runs at publish today: drain-side filter fragments,
nested `computeIndex` and the real view.

### Threading and lifecycle

- Decode work stays on the pool. The drain loses its copying and keeps counting, allocation and
  the publish-time work it already does.
- A reorder slot's level scratch and decompressed bytes live until the page's fill completes.
  The retriever's throttle advances on fill completion instead of on drain consumption, so the
  pages a column holds stay bounded by `MAX_INFLIGHT_PAGES`, now as encoded bytes and
  batch-sized arrays instead of decoded page-sized arrays.
- A fill task is tracked like a decode task: `close()` waits for it, and its failure is placed
  with the page's file, row group and page ordinal.

### Invariants for READ_PIPELINE.md

- Placement is the only place batch boundaries are decided; every column runs it over the same
  pages with the same rules.
- A batch is published only after every segment placed into it is filled, and batches are
  published in order.
- A segment is written by one fill task; two segments of one batch touch no common element
  except validity words, which are merged atomically.
- A slot is reused only after its page's fill completed.

## Stages

Each stage lands as its own PR, keeps every read correct, and is judged on the benchmarks named.
A stage's gate is also: no benchmark of `run-regression.sh` slower than the 1.1.0.Beta2 baseline
on the N300.

| # | Stage | Removes | Gate |
| --- | --- | --- | --- |
| 1 | Measure, place and fill for every read, with fixed-width flat kernels (dictionary, `PLAIN`, `BYTE_STREAM_SPLIT`, delta `INT32`/`INT64`, `BOOLEAN`). Other page kinds are filled by decoding them as today and copying into their segments, which keeps them correct until their stage. | steps 3–5 of #1287 for flat fixed-width columns | `run-1brc.sh` and `run-flat.sh` faster |
| 2 | Binary leaves without repetition: dictionary ids, `PLAIN` and delta bytes | the per-page id array; the `byte[]` per value | `run-1brc.sh` (the `station` ids), `run-dictfilter.sh` |
| 3 | Leaves with repetition: values, levels, offsets and bytes at their exact size; fixed-size lists | the accumulators, their doubling and the publish copies (#1024, #750, finding 6 of READ_PATH_CONVERGENCE) | `run-nested.sh`, `run-fixedlist.sh` faster |
| 4 | Remove the decode-and-copy fill, fold the end state into `READ_PIPELINE.md`, `VALUE_DECODE.md` and `NESTED_DECODE.md`, delete this plan | — | — |
| 5 | Snappy decompresses heap to heap (#1015) | steps 1–2 of #1287 | `run-filter.sh` (SNAPPY) faster |

Stage 5 is independent of the others and can land at any point.

## Tests

- **Differential.** A test reads each fixture with the fill kernels and with the
  decode-and-copy fill (an internal switch until stage 4, then against the prior release's
  output recorded as expected batches) and compares every batch: pages spanning many batches and
  pages smaller than one, required and optional leaves, single-entry dictionaries, dictionary and
  `PLAIN` pages mixed in one chunk, masks of one and of several intervals, null placeholders,
  boundary markers, the row cap inside a page, the file cut and the row-group cut, and both
  exchange modes.
- **Parallel fills.** Pages decoded concurrently into one batch, with nulls at the page edges,
  for the atomic validity merge.
- **Lifecycle.** `close()` during fills, a fill failure, a recycling pool with one free holder.
- The existing suites (`DifferentialReadTest`, the parquet-testing runner, `MisalignedPageBoundariesTest`,
  `WideListBatchBoundTest`, the fixed-size-list tests) run unchanged.

## Taken from the spike and the prototype

From `scatter-decode-spike`: the segment loop of `ScatterScan.decodePage` (a page over
consecutive batch segments, decoder state kept across them), the `scatter` and `unpackIds`
kernels, and `markValidity`. From `1287-proto-deferred-dict`: the per-type dictionary loops with
and without definition levels, and the zero-width (single-entry dictionary) case. Not taken: the
spike's single scheduler, batch ring, recycling and unordered sink, which belong to stages 4 and
5 of #1476.

## Not in scope

- `NestedLevelComputer.computeRealView` (#1023) and leaf compaction (#756): derived per batch at
  publish, unchanged here.
- Parallel consumption and one scheduler per read: stages 4 and 5 of
  [1BRC_THROUGHPUT.md](1BRC_THROUGHPUT.md).
