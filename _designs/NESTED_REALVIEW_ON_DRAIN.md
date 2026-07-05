<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# RealView on the drain; drop ColumnReader-path level materialization

Status: **Implemented**

Tracking issue: #751

This change landed in two stages. **Stage 1** moved `computeRealView` onto the
drain while the batch kept carrying raw def/rep levels — the primary win (the
critical-path scan runs on idle threads) at low risk, with no dependency on #749
and no change to the record-selection path. **Stage 2** (this document's end
state) drops the raw level materialization on the unfiltered `ColumnReader` path,
which removes per-batch memory traffic on the now-bandwidth-bound drain. It depends
on #749 (the raw-level escape-hatch accessors removed) and distinguishes the
filtered from the unfiltered path with a per-worker `IndexMode` — the filtered path
simply keeps the levels, so record selection is unchanged.

## Problem

On the `ColumnReader` (real-items) nested read path the raw Dremel def/rep level
arrays are produced on the drain, copied into every batch, re-scanned by
`NestedLevelComputer.computeRealView` on the **serial consumer thread**, and then
never read again. The `RealView` scan is the single largest cost on the read's
critical path.

Profiling `NestedListReadBenchmark` on the N300
([NESTED_READ_PERFORMANCE_ANALYSIS.md](NESTED_READ_PERFORMANCE_ANALYSIS.md))
isolates the cause: the eight decode threads sit **96% parked** while the serial
consumer is the bottleneck, and **~75% of the consumer's on-CPU time is
`computeRealView`**. The scan runs on the one busy thread while eight idle threads
wait. Separately, the per-batch def/rep level copy and the drain's all-items index
(`elementValidity` + `multiLevelOffsets`) are pure waste on this path — the
`ColumnReader` reads none of them, only the `RealView`.

## Enabling facts

- The `RealView` is a pure function of the batch's def/rep levels, record count,
  and schema layers — `computeRealView(defLevels, repLevels, valueCount,
  recordCount, maxDefinitionLevel, layers)`. It produces its own independent
  arrays (`layerOffsets`, `layerValidity`, `leafValidity`, `realToRawLeaf`), so it
  can be computed on the drain from the accumulators and outlive them.
- A `NestedColumnWorker` feeds exactly one reader kind: `ColumnReader` constructs
  it for the real-items path, `NestedRowReader` for the all-items path. The mode
  is fixed per worker instance, so the split is a constructor flag, not a
  per-batch branch.
- The two reader paths read disjoint batch state:
  - **`ColumnReader` (real-items):** `RealView` only —
    `getValueCount`/`getLayerOffsets`/`getLayerValidity`/`getLeafValidity` and the
    value compaction all route through `ensureRealView()`.
  - **`RowReader` (all-items):** raw def levels and the all-items index.
    `NestedBatchIndex.getDefLevel` is consumed by `PqListImpl`, `PqStructImpl`,
    `PqMapImpl`, `NestedBatchDataView`, and extensively by
    `VariantShredReassembler`. This path genuinely needs raw levels and is out of
    scope.
- With **#749** landed (the public `getDefinitionLevels()` /
  `getRepetitionLevels()` escape hatches removed), the `ColumnReader` path has
  *zero* raw-level readers left, so the levels can be dropped outright — no
  on-demand level synthesis is needed.

## End state

### IndexMode

`NestedColumnWorker` takes an `IndexMode`, chosen by the constructing reader, that
selects what the drain derives per batch at publish time (nothing else in assembly
changes):

- **`ALL_ITEMS`** — the `RowReader` path (and the convenience constructor tests
  use). Computes the all-items `elementValidity` + `multiLevelOffsets` index and
  keeps the raw def/rep levels, which `NestedBatchIndex` and its consumers read.
- **`REAL_VIEW`** — the unfiltered `ColumnReader` path. Builds the `RealView` (and
  compacted values) on the drain and drops the raw levels and the all-items index;
  nothing downstream reads them.
- **`REAL_VIEW_KEEP_LEVELS`** — the exact-filtered `ColumnReader` path. Keeps the
  raw levels so `applySelection` can slice them per kept record, and skips the
  all-items index; the drain derives no view (the consumer rebuilds it after
  selection).

### NestedBatch carries the RealView

`NestedBatch` gains a `RealView realView` field. On the real-items path the drain
populates it (`computeIndex` → `buildRealView`); on the all-items path it stays
`null` and today's fields (`definitionLevels`, `repetitionLevels`,
`elementValidity`, `multiLevelOffsets`) are populated as they are now.

### Drain publish

In `computeIndex`:

- **`REAL_VIEW`** builds the `RealView` from the **drain accumulators**
  (`nestedDefLevels` / `nestedRepLevels`, still intact for `[0, valueCount)` until
  the counts reset after publish), so `publishCurrentBatch` leaves the batch's
  `definitionLevels` / `repetitionLevels` **`null`** — the per-batch level
  `Arrays.copyOf` pair is dropped. The all-items index is skipped
  (`elementValidity` / `multiLevelOffsets` stay `null`). Fixed-size-list fast-path
  batches (`fixedListK > 0`) already omit levels; the drain stamps the arithmetic
  `RealView` (offsets from `fixedListLayerOffsets`, all-present validity,
  `realToRawLeaf == null`). Value compaction into `realValues` is also done here
  (see the value-compaction design); otherwise the raw values pass through.
- **`REAL_VIEW_KEEP_LEVELS`** keeps the raw levels (copied as before) and derives
  nothing else — no view, no all-items index. The batch is compacted by
  `applySelection` before the consumer reads it.
- **`ALL_ITEMS`** keeps the raw levels and computes `elementValidity` +
  `multiLevelOffsets`, as before.

### Consumer

`ColumnReader.ensureRealView()` returns `currentNestedBatch.realView` when the
drain set it (`REAL_VIEW`), and otherwise falls back to the lazy
`computeRealView` / `fixedListRealView` from the batch's levels — the path taken by
`REAL_VIEW_KEEP_LEVELS` batches and by the batches `compactNestedBatch` derives
from them. Because the filtered path keeps its levels, **record selection is
unchanged**: `compactNestedBatch` slices the raw `(levels, values, recordOffsets)`
triplet exactly as before, and the consumer rebuilds the view from the sliced
levels.

### RowReader path

Unchanged. `ALL_ITEMS` keeps materializing def/rep levels and the all-items index,
so `NestedBatchIndex` and its consumers see exactly today's batch.

## Scope

- **In:** the `ColumnReader` real-items path — `RealView` on the drain, and
  dropping the def/rep level copy and the all-items index on the unfiltered
  (`REAL_VIEW`) path.
- **Out:** value compaction on the drain is a separate change. The `RowReader`
  all-items path and the exact-filter record-selection logic are untouched.
  Bulk-copy assembly (#750) is orthogonal.

## Dependencies

- **#749** — the public `getDefinitionLevels()` / `getRepetitionLevels()`
  escape-hatch accessors removed. Required before the levels can be dropped on the
  `REAL_VIEW` path: without it the `ColumnReader` would still have a raw-level
  reader.

## Benefits

- The dominant critical-path scan moves off the serial consumer onto the ~96%-idle
  parallel drain threads — a latency win independent of any instruction-count
  change.
- On the unfiltered path the per-batch def/rep level `Arrays.copyOf` pair is
  eliminated, cutting drain memory traffic — which matters because the drain is
  bandwidth-bound once reconstruction has moved onto it.
- The drain stops computing the all-items `elementValidity` + `multiLevelOffsets`
  for `ColumnReader` batches, and the duplicated level scan (drain index vs
  consumer `RealView`) collapses to one scan.

## Testing

- Differential: `ColumnReader` and `RowReader` folds agree across null densities,
  masking, multi-level nesting, and the fixed-size-list fast path (the existing
  nested differential suite).
- `NestedListReadBenchmark`: `columnNested` improves in wall time; `rowNested` and
  `flatFloor` do not regress.

## Results (Stage 1)

`NestedListReadBenchmark`, `LIST<float64>`, 8 M leaves, N300, unpinned (the
cross-core rebalance the change targets is not visible pinned to one core):

| `columnNested` | baseline ms/op | Stage 1 ms/op | Δ |
|---|--:|--:|--:|
| none | 197.1 ± 4.1 | 188.4 ± 2.2 | −4.4% |
| dense | 312.6 ± 14.1 | 271.9 ± 12.2 | −13.0% |

`rowNested` is unchanged (none 147.6 → 147.3, dense 214.8 → 214.1). The win is
largest on `dense`, where the `RealView` scan does the most validity bookkeeping,
so moving it off the serial consumer helps most. The residual gap to `rowNested`
was the consumer-side value compaction and per-batch allocation, addressed by the
follow-on value-compaction work; Stage 2 (this document's end state) then removed
the raw-level materialization on top.

## Results (Stage 2)

Dropping the raw-level copy on the unfiltered path removes per-batch drain memory
traffic, so its effect tracks memory bandwidth. On the single-channel N300 (drain
bandwidth-bound), isolating Stage 2 against the otherwise-identical prior build:

| benchmark | pre-Stage 2 | Stage 2 | Δ |
|---|--:|--:|--:|
| `columnNested` none | 179.0 | 165.2 | −7.7% |
| `columnNested` dense | 277.1 | 263.5 | −4.9% |
| `columnMulti` (8 col) none | 129.8 | 113.2 | −12.8% |
| `columnMulti` (8 col) dense | 173.2 | 164.8 | −4.8% |

The win is largest on the wide, all-present case, where the most level bytes are
dropped (eight columns × two copies × the largest per-batch value counts). On a
bandwidth-rich machine (Apple Silicon, unified memory) the same A/B is neutral —
the traffic reduction has no scarce resource to free.
