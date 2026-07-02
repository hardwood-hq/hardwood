# Design: fixed-size-list read fast path

**Status: Implemented.** Tracking issue: #740.

## Goal

Provide a read-side fast path for Parquet columns that carry fixed-shape vector
data (embeddings, image tensors, fixed-length arrays) but are stored using the
standard three-level `LIST` encoding. For such a column every present row has
exactly *k* children, so the per-element repetition-level stream is a constant,
redundant description of a shape the schema already fixes. The fast path
recognises this case from the level streams alone and skips the per-element
list-reconstruction state machine, emitting the same output as the regular path
with implicit `(rows × k)` offsets.

It requires **no Parquet format change and no writer change** — it is purely a
reader optimisation over files existing writers already produce. Its second
purpose is to measure how much of the known read-time gap between
fixed-shape-as-`LIST` and the same data laid out flat is recoverable in the
reader alone, as a data point for the `parquet-format` discussion on a native
fixed-size-list type.

## Background & motivation

Arrow's `FixedSizeList<T, k>` and `FixedShapeTensor` map to Parquet's standard
3-level `LIST` encoding on write:

```
optional group embedding (LIST) {
  repeated group list {
    required <element-type> element;
  }
}
```

The leaf has `max_rep_level = 1` and `max_def_level = 2`. For N rows of
k-element vectors the writer emits N·k repetition-level entries and N·k
definition-level entries. For fixed-shape data those entries are pure
boilerplate: the rep stream is the pattern `0, 1, 1, …, 1` (one `0`, then k−1
`1`s) repeated once per row, and — when vectors are dense and present — the def
stream is a single constant. Both encode *k*, which the schema already fixes.

The cost is not disk footprint (RLE compresses these regular streams to almost
nothing; file sizes come out equal to the flat layout). The cost is decode time:
the rep-level walk sits on the critical path for record reconstruction, and a
reader has to march through it element-by-element to rediscover boundaries it
already knew. Multiple independent measurements (`apache/arrow#34510` and
follow-on vector-storage benchmarks) report a ~3× read-time gap between a
`FixedSizeList` column and the same data flattened, at equal file size.

## Scope

**In scope:**

- Read-side fast path for the column shape
  `optional group <name> (LIST) { repeated <primitive> element }`
  where every non-null row has exactly *k* children (`maxRep == 1`,
  `maxDef == 2`).
- Dense, present vectors: no null vectors, no null elements.
- `DataPageV2` and `DataPageV1` pages whose levels use the RLE hybrid. The
  detector runs on the raw level bytes and is page-version-agnostic; only the
  decode seam differs (see Implementation).
- Transparent fallback to the regular decode path whenever the fast-path
  invariants do not hold — selected automatically, per page.

**Out of scope:**

- Any writer-side or Parquet-format change.
- Nullable *vectors* (null rows) — a follow-up (the def stream stops being a
  single constant run; the detector's structure does not preclude it).
- Nullable *elements* within a vector.
- Nested / struct / list-of-list element types — primitive elements only.
- Legacy standalone `BIT_PACKED` level encoding — pre-2.0 `DataPageV1` files may
  encode levels with the deprecated `BIT_PACKED` scheme rather than the RLE
  hybrid the detector parses; those pages take the regular path.

## Algorithm

The two level streams behave very differently and are handled separately. The
detector is a pure predicate over a page's rep/def level regions that returns
`CleanFixedK(k)` or `NotApplicable`; it reads no vector interiors and mutates
nothing.

### Definition-level gate (O(1))

In the target case every leaf sits at `def == max_def_level` (2). Encoded with
the RLE/bit-packing hybrid, that is a **single RLE run**. The gate inspects one
varint header plus the value byte and accepts iff it is a single RLE run of
value `max_def_level` covering all values. Any other shape — a bit-packed run,
multiple runs, any value below the max — means nulls or empty/short lists are
present, and the page falls back. This gate is the precondition for everything
downstream: once the def stream is a pure max-value run, no null can be hiding,
and the rep stream must be the clean periodic form.

### Repetition-level verification (O(rows))

The rep stream over `{0, 1}` is bit width 1: `0` opens a row, `1` continues it.
The detector locates the `0`s, derives *k* from the first inter-boundary gap,
and requires every subsequent gap — including the final row, closed against the
end of the stream — to equal it. *k* is **verified, not assumed**: a writer
emitting variable inner lengths that merely sum to a multiple of the row count
is rejected by the per-gap check rather than silently mis-decoded. Two regimes,
split by how the hybrid encoder stores the run of k−1 `1`s:

**Large *k* (interior run ≥ 8).** The run of `1`s is an RLE run; the lone `0`
and a few neighbours land in a short bit-packed boundary group at each row
start. A scalar run-walk reads the RLE run's length **from its header** and
advances by that count — hundreds of interior levels consumed as a single
number, never scanned. Detection is O(rows), touching only boundary bytes. The
split between the bit-packed boundary group and the RLE run is
encoder-dependent (parquet-mr / arrow-cpp / arrow-rs differ); the walk matches
the structure rather than assuming a split point.

**Small *k* (interior run < 8, i.e. k ≤ 8).** No run reaches the RLE threshold,
so the stream is entirely bit-packed. When `k` divides 8 (k ∈ {2, 4, 8}) the
clean pattern is a single repeating byte (bit `p` is `0` exactly when
`p % k == 0`), so cleanliness reduces to a bulk equality of each run's payload
against that one byte. Writers chunk bit-packed runs (arrow-cpp emits 63-group
runs), so the compare walks run by run; within a run it compares a `long` at a
time — a SWAR-style equality over 8 packed levels against a constant word — with
only the sub-word remainder and the final padding-masked byte handled scalar. A
single word mismatch proves the shape unclean. Small `k` with a multi-byte
period (3/5/7) and the large-*k* boundary-then-RLE shape both have no clean
single-byte tiling, so they fall to the scalar walk.

Both regimes are sound in O(rows): a vector interior is never read for decode or
for verification, because the RLE length that says "k−1 ones" is a number in a
header, not something reconstructed by scanning.

### Output

Once the gates pass, offsets are implicit: `offsets[i] = i * k`, all elements
present. The page is decoded to its values only; record and layer offsets are
set arithmetically and validity is omitted (the all-present representation). The
public output is identical to the regular path — the same `NestedBatch` and the
same `ColumnReader` accessors — produced without materialising level arrays or
walking the reconstruction state machine.

## Implementation

The shape signal rides from the page decoder to the batch on a `fixedListK`
marker (`0` for a regular page/batch). All types live in
`dev.hardwood.internal.reader`.

- **Detector** — `FixedSizeListDetector.detect(...)` returns a sealed
  `FixedSizeListShape` (`CleanFixedK(k)` / `NotApplicable`). The def-gate reuses
  `RleBitPackingHybridDecoder.isSingleRleRunOf`; the rep verification is the
  tiled small-*k* compare (`tryTiledBitPacked` / `matchesConstant`, single-byte
  period only) with the scalar run-walk as the fallback for every other case
  (multi-byte-period small `k`, large `k`, multi-run). The tiled compare reads
  each 8-byte word through a `byte[]`-to-`long` `VarHandle` (one unaligned load
  and bounds check per word) and tests it against a constant pattern word — no
  per-word division and no loop-carried state.
- **Page decode seam** — `PageDecoder.parseDataPageV2` runs the detector on the
  raw level regions when `maxRep == 1 && maxDef == 2`. On `CleanFixedK(k)` it
  decodes only the values (the regular value decoders already read densely from
  a null definition-level array — the all-present convention shared with flat
  columns) and stamps the shape with `Page.withFixedListK`. No value-decode path
  changed. `parseDataPage` (`DataPageV1`) is analogous: it slices the inline
  level regions after page decompression, is restricted to RLE-encoded levels,
  and passes `ROWS_UNKNOWN` since the V1 header carries no `num_rows` (the
  detector then verifies `numValues % k == 0` in place of the cross-check). V2
  slices levels pre-decompression; V1 reads them from the decompressed body —
  the only difference between the two seams.
- **Assembly** — `NestedColumnWorker.assembleCleanPage` bulk-copies each
  record's values and sets record offsets arithmetically, skipping the
  per-element level copy. A published batch stays homogeneous — wholly clean
  (levels omitted) or wholly regular — but batch boundaries must fall at the
  same rows across every column, which holds only at batch-capacity and file
  boundaries. The assembler therefore never cuts a batch at a page boundary:
  when an open clean batch meets a regular page or a different *k*, it converts
  that batch to the regular representation in place — `materializeCleanBatchLevels`
  synthesizes the omitted levels (all elements present, each record a `0`
  followed by `k - 1` ones) — and keeps filling the same batch;
  `assembleCleanPageAsRegular` then folds a clean page into an already-regular
  batch with synthesized levels. A file with any irregular page transparently
  falls back for the batches that span it, while wholly clean batches keep the
  level-free layout. `computeIndex` produces the arithmetic layer offsets and
  null validity for a clean batch.
- **Column reader** — `ColumnReader.ensureRealView` builds the real-items view
  directly for a clean batch (arithmetic offsets, null validity, identity
  pass-through of the dense values) instead of scanning levels;
  `compactNestedBatch` preserves the marker and is null-safe on the omitted
  level arrays.
- **Configuration** — the `hardwood.fixed-list-fast-path` [ReaderConfig] option
  (default enabled; see [READER_CONFIG.md](READER_CONFIG.md)) is resolved in
  `ParquetFileReader` and threads through to `PageDecoder`. Setting it to `"false"`
  disables the fast path, both as a kill-switch and so the record-reconstruction
  baseline can be measured on the very same files.

## Results

Measured with `FixedSizeListDecodeBenchmark` (JMH, average time, 2 forks × 5
iterations). Each file holds the same 8M float32 leaf values, `DataPageV2`,
`PLAIN`, uncompressed, so times compare directly across *k*. Three reference
points plus the isolated detector cost:

- **naive** — the `LIST` files with the fast path disabled (record
  reconstruction, the baseline being beaten).
- **fast** — the same files via the fast path.
- **flat** — the identical values as a plain float32 column (the decode floor).
- **detector** — the detector alone over each page's raw level regions, no value
  decode (`repScanOnly`).

| k | naive (ms) | fast (ms) | flat (ms) | naive→fast | recovered | detector (ms) |
|-----:|-----:|-----:|-----:|-----:|-----:|-----:|
| 4 | 41.2 | 12.9 | 8.0 | 3.2× | 85.0% | 0.31 |
| 8 | 47.2 | 13.0 | 8.0 | 3.6× | 87.3% | 0.31 |
| 16 | 64.8 | 16.7 | 8.0 | 3.9× | 84.7% | 3.41 |
| 128 | 64.2 | 15.5 | 8.0 | 4.1× | 86.6% | 0.47 |
| 768 | 62.2 | 14.8 | 8.0 | 4.2× | 87.4% | 0.08 |
| 1536 | 61.8 | 14.8 | 8.0 | 4.2× | 87.3% | 0.04 |

`recovered = (naive − fast) / (naive − flat)`.

Findings:

- The fast path recovers a **stable ~85–87%** of the reconstruction overhead
  across the whole *k* sweep, at **3.2–4.2×** over the naive `LIST` decode. The
  naive path is 5.2–8.1× slower than the flat floor; the fast path lands at
  ~1.6–2.1× of it.
- The residual over the flat floor is **value decode plus per-record assembly**,
  not level handling — the honest ceiling. A native fixed-size-list type would
  need to justify itself on that residual, not on the reconstruction cost this
  fast path already removes.
- The SWAR tiled compare makes small-*k* detection negligible (0.31 ms,
  ~2% of the fast path; without it the scalar bit walk was ~5 ms, ~39% at k=4).
  It does not move the recovered fraction: in the full read, detection overlaps
  value decode and memory traffic, so it was never the wall-clock bottleneck.
- The small-*k* tiled compare tests each word against a constant, with no
  per-word division and no loop-carried dependency — a tight scalar 64-bit word
  loop. Instruction-level profiling (perfasm, JDK 25) confirms C2 does **not**
  auto-vectorize it: the mismatch early-exit (`!= refWord → return false`) is a
  loop side-exit its SuperWord pass rejects. That is fine — detection is ~2–3% of
  the end-to-end read, so its throughput is a code-clarity matter, not a
  wall-clock lever on `fastPathList`. True SIMD here would require a branchless
  accumulate (scan the whole run, check once), trading the early-out for width
  with no end-to-end payoff.
- The table above is `DataPageV2`. `DataPageV1` files reach the same fast path
  through the V1 seam and perform **within noise of V2** (marginally faster at
  small *k* on this uncompressed corpus), recovering ~86–90% of the same gap.
  The naive V1 baseline matches V2, so the reconstruction cost — and its removal
  — are independent of page version. The win does not "evaporate" for V1: the
  savings are level-array and reconstruction work, which decompression (needed
  by both versions regardless) does not affect.

## Limitations & follow-ups

- **k with an RLE interior and a high row count.** When the interior is
  RLE-encoded (k ≥ 9) the detector uses the scalar O(rows) run-walk, whose cost
  is per-row varint-header parsing. At k=16 (the most rows in the sweep) that is
  ~20% of the fast path. A structural bulk check of the fixed
  `boundary-group + constant-length-RLE` per-row stride would remove it; the
  tiled compare does not apply there.
- **Value-encoding ceiling.** The fast path recovers reconstruction cost only.
  Elements stored under the `LIST` shape do not gain per-element
  `BYTE_STREAM_SPLIT` / `ALP` unless the writer already chose them, and the fast
  path still scans (does not decode) the level bytes.
- **Nullable vectors.** Supporting null rows relaxes the def-gate from "single
  max-value run" to a two-level `{present, absent}` def stream; the rep detection
  is largely unchanged. Out of scope here; the detector's structure does not
  preclude it.
- **Legacy standalone `BIT_PACKED` levels.** The fast path is restricted to the
  RLE-hybrid `DataPageV2` levels and falls back otherwise.
