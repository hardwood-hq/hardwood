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
reader optimisation over files existing writers already produce. It ships as a
real but opt-in capability (off by default; see the configuration flag below),
enabled deliberately while it matures in the field. As a byproduct it also
quantifies how much of the known read-time gap between fixed-shape-as-`LIST` and
the same data laid out flat is recoverable in the reader alone — a useful data
point for the `parquet-format` discussion on a native fixed-size-list type.

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

- Read-side fast path for the 3-level compliant `LIST` column shape
  `<optional|required> group <name> (LIST) { repeated group list { <primitive> element } }`
  where every non-null row has exactly *k* children (`maxRep == 1`). Both an
  optional list (`maxDef == 2`) and a required list of required elements
  (`maxDef == 1`) qualify. For `maxDef == 1` the leaf must be non-`REPEATED`: a
  bare unannotated `repeated <primitive>` and the legacy 2-level list encoding
  share this level geometry but carry a `REPEATED` leaf, indistinguishable from
  each other by levels alone, so they stay on the regular path (see Out of scope).
  A `maxDef == 2`, `maxRep == 1` column is unambiguously a list, so an optional
  2-level list is admitted incidentally — the exclusion is specific to the
  ambiguous `maxDef == 1` case.
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
- **Legacy 2-level required lists and bare repeated primitives.** A `maxRep == 1`,
  `maxDef == 1` column with a `REPEATED` leaf — either the legacy 2-level `LIST`
  encoding (`<optional|required> group <name> (LIST) { repeated <primitive> array }`,
  still the default of an unconfigured `AvroParquetWriter`,
  `parquet.avro.write-old-list-structure = true`) or a bare unannotated
  `repeated <primitive>` — is read through the regular path. The two share the same
  level geometry, and treating a bare repeated primitive as a fixed-size list would
  be a type error, so both are excluded rather than distinguished. Fixed-shape
  vector data from the writers that matter here — pyarrow
  (`use_compliant_nested_type`), Spark (`writeLegacyFormat = false`), DuckDB — is
  3-level compliant, so this costs the target workloads nothing. (An optional
  2-level list, `maxDef == 2`, is not excluded — see In scope.)
- Legacy standalone `BIT_PACKED` level encoding — pre-2.0 `DataPageV1` files may
  encode levels with the deprecated `BIT_PACKED` scheme rather than the RLE
  hybrid the detector parses; those pages take the regular path.

## Algorithm

The two level streams behave very differently and are handled separately. The
detector is a pure predicate over a page's rep/def level regions that returns
`FixedWidth(k)` or `NotApplicable`; it reads no vector interiors and mutates
nothing.

### Definition-level gate (O(1))

In the target case every leaf sits at `def == max_def_level` — `2` for an
optional list, `1` for a required list of required elements. Encoded with the
RLE/bit-packing hybrid, that is a **single RLE run**. The gate inspects one
varint header plus the value byte and accepts iff it is a single RLE run of
value `max_def_level` covering all values. Any other shape — a bit-packed run,
multiple runs, any value below the max — means nulls or empty/short lists are
present, and the page falls back. This gate is the precondition for everything
downstream: once the def stream is a pure max-value run, no null can be hiding,
and the rep stream must be the fixed-width periodic form.

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
start. Because that per-row encoding (boundary group + RLE run) is byte-aligned
and identical across rows, the whole stream is one **byte stride repeated once
per row**. The detector parses only the first row to derive the stride and `k`,
then verifies the stream is byte-periodic with that stride via a single bulk
compare (`Arrays.equals` of the stream against itself shifted by one stride) —
no per-row header parsing. The split between the boundary group and the RLE run
is encoder-dependent (parquet-mr / arrow-cpp / arrow-rs differ), and some
encoders lay a row out non-uniformly or unaligned; the stride tiling defers
those to the scalar run-walk, which reads each RLE run's length from its header
and matches the structure rather than assuming a split point.

**Small *k* (interior run < 8, i.e. k ≤ 8).** No run reaches the RLE threshold,
so the stream is entirely bit-packed. The fixed-width pattern is byte-periodic
with period `k / gcd(k, 8)`: a single repeating byte for `k` dividing 8
(k ∈ {1, 2, 4, 8}, bit `p` is `0` exactly when `p % k == 0`), or 3/5/7 bytes for
the multi-byte periods (k ∈ {3, 5, 6, 7}). Either way the check reduces to a bulk
equality of each run's payload against that period tiled across it. Writers chunk
bit-packed runs (arrow-cpp emits 63-group runs), so the compare walks run by run;
within a run it compares a `long` at a time — a SWAR-style equality over 8 packed
levels — with only the sub-word remainder and the final padding-masked byte
handled scalar. For a single-byte period the reference word is a constant; for a
multi-byte period it is selected per word from a `period`-entry table keyed by the
run's byte offset in the stream (its phase). A single word mismatch proves the
shape is not fixed-width. Only irregular or unaligned encodings that neither the
small-`k` tiling nor the large-`k` stride tiling recognises fall to the scalar
walk.

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
  `FixedSizeListShape` (`FixedWidth(k)` / `NotApplicable`). The def-gate reuses
  `RleBitPackingHybridDecoder.isSingleRleRunOf`; the rep verification tiles both
  regimes — `tryTiledBitPacked` for all `k ≤ 8` (`matchesConstant` for a single-byte
  period, `matchesPattern` for the 3/5/7-byte periods) and `tryTiledRle` for the
  large-`k` per-row byte stride — leaving `scalarFallback` (the scalar run-walk)
  only for irregular or unaligned encodings neither recognises. The tiled compare
  reads each 8-byte word through a `byte[]`-to-`long` `VarHandle` (one unaligned
  load and bounds check per word) and tests it against the reference word — a
  constant for a single-byte period, a per-phase table entry for a multi-byte one —
  precomputed once per small-`k` case in `TILE_WORDS` (keyed by `k`; the sub-word
  tail uses a word's low byte), so no per-page rebuild or allocation is on the
  detection path.
- **Page decode seam** — `PageDecoder.parseDataPageV2` runs the detector on the
  raw level regions when `hasFixedListLevelShape()` holds: `maxRep == 1` with
  `maxDef == 2` (optional list) or `maxDef == 1` for a required list whose leaf is
  not itself `REPEATED` (excluding bare unannotated repeated fields). On
  `FixedWidth(k)` it
  decodes only the values (the regular value decoders already read densely from
  a null definition-level array — the all-present convention shared with flat
  columns) and stamps the shape with `Page.withFixedListK`. No value-decode path
  changed. `parseDataPage` (`DataPageV1`) is analogous: it slices the inline
  level regions after page decompression, is restricted to RLE-encoded levels,
  and passes `ROWS_UNKNOWN` since the V1 header carries no `num_rows` (the
  detector then verifies `numValues % k == 0` in place of the cross-check). V2
  slices levels pre-decompression; V1 reads them from the decompressed body —
  the only difference between the two seams.
- **Assembly** — `NestedColumnWorker.assembleFixedWidthPage` bulk-copies each
  contiguous run of kept records in one `copyValueRun` and sets record offsets
  arithmetically, skipping the per-element level copy. A published batch stays homogeneous — wholly
  fixed-width (levels omitted) or wholly regular — but batch boundaries must fall
  at the same rows across every column, which holds only at batch-capacity and
  file boundaries. The assembler therefore never cuts a batch at a page boundary:
  when an open fixed-width batch meets a regular page or a different *k*, it
  converts that batch to the regular representation in place —
  `materializeFixedWidthBatchLevels` synthesizes the omitted levels (all elements
  present, each record a `0` followed by `k - 1` ones) — and keeps filling the
  same batch. The same `assembleFixedWidthPage`, given an already-regular batch,
  folds a fixed-width page into it with synthesized levels (its `asRegularBatch`
  arm). A file with any irregular page transparently falls back for the batches
  that span it, while wholly fixed-width batches keep the level-free layout.
  `computeIndex` produces the arithmetic layer offsets and null validity for a
  fixed-width batch.
- **Level-array allocation** — the value accumulator and the definition/repetition
  level arrays are grown independently. On the fast path only the value array
  grows (`ensureValueCapacity`); the level arrays stay at their initial size,
  since a fixed-width batch publishes with `null` levels. They are grown to the
  accumulated value count only when a batch must fall back
  (`materializeFixedWidthBatchLevels`) or absorb a regular page — so a wholly
  fixed-width column never allocates the level storage its lists would need in the
  regular representation. This keeps the fast path's per-batch footprint at the
  value array alone rather than tripling it with two same-length `int` level
  arrays, which matters most for large *k*, where a batch holds *k* values per
  row.
- **Column reader** — `ColumnReader.ensureRealView` builds the real-items view
  directly for a fixed-width batch (arithmetic offsets, null validity, identity
  pass-through of the dense values) instead of scanning levels;
  `compactNestedBatch` preserves the marker and is null-safe on the omitted
  level arrays.
- **Configuration** — the `hardwood.fixed-list-fast-path` [ReaderConfig] option
  (default disabled — opt-in while the fast path matures; see
  [READER_CONFIG.md](READER_CONFIG.md)) is resolved in `ParquetFileReader` and
  threads through to `PageDecoder`. Set it to `"true"` to enable the fast path;
  leaving it unset keeps the record-reconstruction path, which also lets the
  baseline be measured on the very same files.

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
- The residual over the flat floor is **value decode plus the value copy into the
  batch**, not level handling — the honest ceiling. A native fixed-size-list type would
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
