# Plan: the fastest 1BRC-over-Parquet scan with landed code

Tracking issue: #1476. Stage issues: #1471, #1474, #1472, #1287, #1475.

The One Billion Row Challenge over Parquet reads a `station` string column and a
`measurement` double column and computes min, mean and max per station. This plan orders
the changes that make Hardwood's scan of it as fast as the measurements so far say it can
be, using only code that lands on `main` and the public API.

## Measure

#1472 adds the workload to hardwood-benchmarks: 100M rows in its regression preset, 1B
rows for local runs, consumers by dictionary id on one thread and on row-group byte-range
splits. The flat regression benchmark guards against slowing other scans. Numbers are taken
on the Intel N300 pinned at 1.50 GHz, with the DuckDB 1.5.6 CLI as the reference.

| 1B rows, N300 pinned | Wall |
| --- | --- |
| DuckDB CLI | 8.10 s |
| `scatter-decode-spike` with callback consumption, recycled batches and #1471 | 5.34 s |

The spike's figure is the ceiling seen so far, not a target: it needs a new scheduler, an
API #737 rejected in another form, and batch recycling #737 has not accepted.

## Stages

| # | Stage | Issues | Gate before the next stage |
| --- | --- | --- | --- |
| 1 | Land the decoder fixes | #1471, #1474 | merged |
| 2 | The 1BRC benchmark | #1472 | baseline numbers for `main` |
| 3 | Decode pages straight into batch arrays | #1287 | 1BRC and flat faster, no regression benchmark slower |
| 4 | Parallel consumption | #1475 (callback), #737 | decided by measurement, see below |
| 5 | One scheduler per read | #1475 | only if a gap remains that stage 3 and 4 cannot close |

Stages 2 and 3 can run in parallel; stage 3's gate needs stage 2.

### Stage 3: decode into batches

Today a decode task writes each page into a fresh array and the drain copies it into the
batch. On the flat file, whose pages hold about 870K values each, that is about 30% of the
scan's CPU in zeroing and copying arrays, most of them above G1's humongous threshold. A
prototype that skipped the page array for dictionary-encoded numeric pages cut the flat
scan from 306 to 209 ms on eight threads.

The design, for every column kind and read, and its own stages are in
[DECODE_INTO_BATCH.md](DECODE_INTO_BATCH.md).

### Stage 4: parallel consumption

1BRC is consumer-bound once decode is fast: with one consumer thread the spike's wall time
did not move. Two ways to consume in parallel:

- Splits by row-group byte range (`RowGroupPredicate.byteRange`), one reader and one
  consumer per split. Public API today. On 100M rows: 0.84 to 1.03 s.
- An opt-in callback that consumes each batch on the pool thread completing it (#1475).
  On 100M rows: 0.71 to 0.78 s, against 1.47 s for one consumer.

Measure splits on stage 3's pipeline first. Propose the callback only if splits stay
clearly behind it; that reopens #737's "decode-time sink", which was rejected for running
on the drain, not the pool.

### Stage 5: one scheduler per read

The spike replaces the per-column pipelines with one scheduler that places pages of every
column into row-aligned batches. On the flat file it used the least CPU and was fastest on
one thread (804 ms against 1112 ms), but on eight threads its single scheduler thread kept
it behind the stage 3 prototype (262 ms against 209 ms). It is worth its rewrite only if,
after stages 3 and 4, single-core efficiency or CPU per row is what separates Hardwood from
the reference. The design, measurements and open questions are on #1475; the code is on
branch `scatter-decode-spike`.

## Not in scope

| Topic | Why |
| --- | --- |
| Recycling batch arrays | #737's bar: a Flink-shaped scan, filtered and unfiltered, with no slower filtered scan |
| Vector API bit unpacking | #680; the unpack kernel is a small share of the scan after #1471 |
| Cold start | set aside for this work; JEP 544 (AOT method code, JDK 28) addresses it on the JVM |
| Codec library extraction per process | #1470, independent of throughput |

## Completion

When stage 4 is decided and stage 5 is either done or dropped, fold the end state into
`READ_PIPELINE.md` and `COLUMN_READER.md`, record the final 1BRC numbers on #1476, and
delete this plan.
