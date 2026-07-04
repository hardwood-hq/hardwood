<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# RealView on the drain; drop ColumnReader-path level materialization

Status: **Stage 1 implemented; Stage 2 planned**

Tracking issue: #751

This change lands in two stages. **Stage 1** moves `computeRealView` onto the
drain while the batch keeps carrying raw def/rep levels — it captures the primary
win (the critical-path scan runs on idle threads) at low risk, with no dependency
on #749 and no change to the record-selection path. **Stage 2** drops the raw
level materialization on the `ColumnReader` path; it depends on #749 and rewrites
record selection to slice the `RealView`. The stages are split because profiling
attributes the wall-clock win to the *move*, not the level-drop, so Stage 1
delivers most of the benefit on its own.

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

## Stage 1 — RealView on the drain (implemented)

### Real-items mode flag

`NestedColumnWorker` takes a `realItemsMode` boolean. `ColumnReader` constructs
its worker with `true`, `NestedRowReader` with `false`; the convenience
constructor (tests) defaults to `false`. The flag selects what the drain computes
at publish time; nothing else in assembly changes.

### NestedBatch carries the RealView

`NestedBatch` gains a `RealView realView` field. On the real-items path the drain
populates it (`computeIndex` → `buildRealView`); on the all-items path it stays
`null` and today's fields (`definitionLevels`, `repetitionLevels`,
`elementValidity`, `multiLevelOffsets`) are populated as they are now.

### Drain publish, real-items mode

In `computeIndex`, when `realItemsMode`:

- Build the `RealView` from the batch's def/rep levels (already set on the batch
  at this point), and **skip** the all-items index (`computeElementValidity`,
  `computeLayerOffsets`) — it serves only the `RowReader`, so
  `batch.elementValidity` / `batch.multiLevelOffsets` stay `null`.
- Fixed-size-list fast-path batches (`fixedListK > 0`) already omit levels; the
  drain stamps the arithmetic `RealView` (layer offsets from
  `fixedListLayerOffsets`, all-present validity, `realToRawLeaf == null`) the
  consumer used to build in `fixedListRealView`.
- Raw def/rep levels are **still materialized** on the batch in Stage 1 (dropped
  in Stage 2). Value compaction stays on the consumer.

### Consumer

`ColumnReader.ensureRealView()` returns `currentNestedBatch.realView` when the
drain set it, and otherwise falls back to the existing lazy
`computeRealView` / `fixedListRealView` — the path a batch derived by
consumer-side record selection still takes, since selection builds its batch
without a drain view. This keeps the selection path unchanged in Stage 1.

### RowReader path

Unchanged. `realItemsMode == false` keeps materializing def/rep levels and the
all-items index, so `NestedBatchIndex` and its consumers see exactly today's
batch.

## Stage 2 — drop ColumnReader-path level materialization (planned)

With **#749** landed (the public `getDefinitionLevels()` / `getRepetitionLevels()`
escape hatches removed), the real-items path has zero raw-level readers left, so:

- `computeIndex` builds the `RealView` directly from the drain accumulators and
  leaves `batch.definitionLevels` / `batch.repetitionLevels` **`null`**, dropping
  the per-batch level `Arrays.copyOf` pair.
- `applySelection` → `compactNestedBatch` can no longer slice raw levels; it
  **slices the parent `RealView`** for the kept records. Selection keeps whole
  top-level records — contiguous real-leaf spans — so `layerOffsets`,
  `layerValidity`, `leafValidity`, and `realToRawLeaf` are gathered/renumbered
  directly rather than reconstructed from levels.

## Scope

- **In:** the real-items `ColumnReader` path — RealView on the drain (Stage 1),
  and dropping the def/rep level copy plus the selection-path RealView slice
  (Stage 2).
- **Out:** value compaction stays on the consumer (moving it, and reusing consumer
  scratch across batches, is the separate buffer-reuse lever). The `RowReader`
  all-items path is untouched. Bulk-copy assembly (#750) is orthogonal.

## Dependencies

- **#749** (Stage 2 only) — remove the public raw-level escape-hatch accessors.
  Required before the levels can be dropped: without it the `ColumnReader` path
  would still have a raw-level reader.

## Benefits

- The dominant critical-path scan moves off the serial consumer onto the ~96%-idle
  parallel drain threads — a latency win independent of any instruction-count
  change.
- The per-batch def/rep level `Arrays.copyOf` pair is eliminated on real-items
  reads.
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
is the consumer-side value compaction and per-batch allocation — the targets of
the buffer-reuse lever and Stage 2.
