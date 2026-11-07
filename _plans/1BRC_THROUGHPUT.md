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

The spike's figure is the ceiling seen so far, not a target: it needs an API #737 rejected in
another form, and batch recycling #737 has not accepted. Its scheduler is part of stage 3.

## Stages

| # | Stage | Issues | Gate before the next stage |
| --- | --- | --- | --- |
| 1 | Land the decoder fixes | #1471, #1474 | merged |
| 2 | The 1BRC benchmark | #1472 | baseline numbers for `main` |
| 3 | Decode pages straight into batch arrays | #1287 | done |
| 4 | Parallel consumption | #1475 (callback), #737 | decided by measurement, see below |

Stages 2 and 3 can run in parallel; stage 3's gate needs stage 2.

### Stage 3: decode into batches

Done (#1287). One scheduler per read submits the pages of every column to the decode pool in
row order, and each page task writes its values once, from the decompressed page straight into
the arrays of the batches its rows belong to. The design is in
[READ_PIPELINE.md](../_designs/READ_PIPELINE.md#decoded-columns),
[NESTED_DECODE.md](../_designs/NESTED_DECODE.md#cells) and
[VALUE_DECODE.md](../_designs/VALUE_DECODE.md#page-decode).

### Stage 4: parallel consumption

1BRC is consumer-bound once decode is fast: with one consumer thread the spike's wall time
did not move. Two ways to consume in parallel:

- Splits by row-group byte range (`RowGroupPredicate.byteRange`), one reader and one
  consumer per split. Public API today. On 100M rows: 0.84 to 1.03 s.
- An opt-in callback that consumes each batch on the pool thread completing it (#1475).
  On 100M rows: 0.71 to 0.78 s, against 1.47 s for one consumer. On stage 3's pipeline the
  callback would run on the thread that completes the batch's last cell, as a column's filter
  fragment does.

Measure splits on stage 3's pipeline first. Propose the callback only if splits stay
clearly behind it; that reopens #737's "decode-time sink", which was rejected for running
on the drain, not the pool.

## Not in scope

| Topic | Why |
| --- | --- |
| Recycling batch arrays | #737's bar: a Flink-shaped scan, filtered and unfiltered, with no slower filtered scan |
| Vector API bit unpacking | #680; the unpack kernel is a small share of the scan after #1471 |
| Cold start | set aside for this work; JEP 544 (AOT method code, JDK 28) addresses it on the JVM |
| Codec library extraction per process | #1470, independent of throughput |

## Completion

When stage 4 is decided, fold the end state into
`READ_PIPELINE.md` and `COLUMN_READER.md`, record the final 1BRC numbers on #1476, and
delete this plan.
