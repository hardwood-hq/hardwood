<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# RealView on the drain; drop ColumnReader-path level materialization

Status: **Implemented**

Tracking issues: #751, #766

On the unfiltered `ColumnReader` (real-items) path the drain computes the
real-items view and drops the raw def/rep level materialization, so the serial
consumer reads a ready-made view instead of scanning levels on the critical path.
A per-worker `IndexMode` separates this path from the `RowReader` all-items path
and the exact-filter path, both of which keep the levels; record selection is
unchanged. Dropping the raw levels depends on #749 (the escape-hatch accessors
removed), so no downstream reader needs them.

For an **all-present** batch — one with no null or empty parents — the drain
builds only a **lean offsets view** (`computeLayerOffsets` alone), skipping the
per-layer validity and gather-map work of the full `computeRealView` scan. It is
built unconditionally on the drain, so reconstruction stays off the serial
consumer for every consumer shape, including a list-reconstructing read that needs
the offsets.

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
- An **all-present** batch (every leaf definition level at max) has no phantom
  positions, so its real-items `layerOffsets` equal the all-items offsets and
  every validity is trivially all-present. Its view reduces to the list
  boundaries, which `computeLayerOffsets` produces without the per-layer
  presence, leaf-validity, and gather-map work of the full `computeRealView`.

## End state

### IndexMode

`NestedColumnWorker` takes an `IndexMode`, chosen by the constructing reader, that
selects what the drain derives per batch at publish time (nothing else in assembly
changes):

- **`ALL_ITEMS`** — the `RowReader` path (and the convenience constructor tests
  use). Computes the all-items `elementValidity` + `multiLevelOffsets` index and
  keeps the raw def/rep levels, which `NestedBatchIndex` and its consumers read.
- **`REAL_VIEW`** — the unfiltered `ColumnReader` path. Builds the real-items view
  on the drain and drops the raw levels and the all-items index; nothing downstream
  reads them. An all-present batch builds only a **lean offsets view** (see
  [All-present lean view](#all-present-lean-view)); a batch with phantom positions
  builds the full `RealView` and compacts its values.
- **`REAL_VIEW_KEEP_LEVELS`** — the exact-filtered `ColumnReader` path. Keeps the
  raw levels so `applySelection` can slice them per kept record, and skips the
  all-items index; the drain derives no view (the consumer rebuilds it after
  selection).

### NestedBatch carries the RealView

`NestedBatch` gains a `RealView realView` field. On the real-items path the drain
populates it (`computeIndex` → `buildAllPresentView` for all-present batches,
`buildRealView` otherwise); on the all-items path it stays
`null` and today's fields (`definitionLevels`, `repetitionLevels`,
`elementValidity`, `multiLevelOffsets`) are populated as they are now.

### Drain publish

In `computeIndex`:

- **`REAL_VIEW`** derives its view from the **drain accumulators**
  (`nestedDefLevels` / `nestedRepLevels`, still intact for `[0, valueCount)` until
  the counts reset after publish), so `publishCurrentBatch` leaves the batch's
  `definitionLevels` / `repetitionLevels` **`null`** — the per-batch level
  `Arrays.copyOf` pair is dropped. The all-items index is skipped
  (`elementValidity` / `multiLevelOffsets` stay `null`). What it builds depends on
  whether the batch has phantom (null/empty parent) positions:
  - **All-present** (every leaf definition level at max, including the
    fixed-size-list fast path where `fixedListK > 0`): the drain builds a **lean
    offsets view** — `layerOffsets` from `computeLayerOffsets` (or
    `fixedListLayerOffsets` on the fast path), every validity `null`, and
    `realToRawLeaf == null`. The dense values already are the real-leaf values, so
    `realValues` is the batch values unchanged.
  - **Phantom-bearing**: the drain builds the full `RealView` (`computeRealView`)
    and compacts the leaf values into `realValues` through `realToRawLeaf`
    (`LeafCompaction`); a batch whose phantom map is the identity passes its values
    through uncompacted.
- **`REAL_VIEW_KEEP_LEVELS`** keeps the raw levels (copied as before) and derives
  nothing else — no view, no all-items index. The batch is compacted by
  `applySelection` before the consumer reads it.
- **`ALL_ITEMS`** keeps the raw levels and computes `elementValidity` +
  `multiLevelOffsets`, as before.

### All-present lean view

An all-present batch has no null or empty parents, so no raw position is a
phantom: its real-items offsets are identical to the all-items offsets, every
item at every layer is present, and the leaf values need no gather. The drain
therefore skips the full `computeRealView` scan — which would build per-layer
validity bitmaps and a `realToRawLeaf` gather map only to fill them with
all-present / identity values — and computes the list boundaries alone with
`computeLayerOffsets`.

The lean view is built unconditionally on the drain, so it serves both
`ColumnReader` consumer shapes without a consumer-side scan:

- a **flat leaf** read (`getValueCount` plus the typed values — the
  vector/embedding pattern) ignores the offsets and reads the pass-through values
  directly; leaf validity is all-present, so `getLeafValidity` returns the
  no-nulls sentinel;
- a **list-reconstructing** read (`getLayerOffsets` / `getLayerValidity`) reads the
  drain-built boundaries and the all-present validities.

Deferring the view to a lazy consumer-side build would put a structural read's
offset scan back on the serial consumer; building the lean view eagerly avoids
that while costing a flat leaf read only the cheap boundary scan it discards.

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

- **In:** the `ColumnReader` real-items path — the real-items view on the drain
  (the lean offsets view for all-present batches, the full `RealView` otherwise),
  and dropping the def/rep level copy and the all-items index on the unfiltered
  (`REAL_VIEW`) path. Leaf-value compaction into `realValues` on the phantom-bearing
  path is tracked as #757 and lands on the same path.
- **Out:** the `RowReader` all-items path and the exact-filter record-selection
  logic are untouched. Bulk-copy assembly (#750) is orthogonal.

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
- `NestedListReadBenchmark.columnNestedStructural` — a list-reconstructing consumer
  reading `getLayerOffsets` / `getLayerValidity` — folds to the same sum as
  `columnNested`, `rowNested`, and the flat floor, and reads the drain-built lean
  view without a consumer-side scan.

## Results

The design's benefit was isolated in three parts; the numbers come from different
measurement points and machines rather than a single main-to-end-state run.

**Reconstruction off the serial consumer** — `NestedListReadBenchmark`,
`LIST<float64>`, 8 M leaves, N300, unpinned (the cross-core rebalance is not visible
pinned to one core). Moving the `computeRealView` scan onto the ~96%-idle drain
threads:

| `columnNested` | before ms/op | after ms/op | Δ |
|---|--:|--:|--:|
| none | 197.1 ± 4.1 | 188.4 ± 2.2 | −4.4% |
| dense | 312.6 ± 14.1 | 271.9 ± 12.2 | −13.0% |

`rowNested` is unchanged (none 147.6 → 147.3, dense 214.8 → 214.1). The win is
largest on `dense`, where the scan does the most validity bookkeeping.

**Dropping the per-batch level copy** — same benchmark, N300, single-channel, so the
effect tracks memory bandwidth. Eliminating the def/rep `Arrays.copyOf` pair on the
unfiltered path:

| benchmark | before | after | Δ |
|---|--:|--:|--:|
| `columnNested` none | 179.0 | 165.2 | −7.7% |
| `columnNested` dense | 277.1 | 263.5 | −4.9% |
| `columnMulti` (8 col) none | 129.8 | 113.2 | −12.8% |
| `columnMulti` (8 col) dense | 173.2 | 164.8 | −4.8% |

The win is largest on the wide, all-present case, where the most level bytes are
dropped. On a bandwidth-rich machine (Apple Silicon, unified memory) the same change
is neutral — the traffic reduction has no scarce resource to free.

**The lean all-present view** — in-container, no perf isolation, directional and
pending N300 confirmation; `LIST<float64>`, 2 M leaves, `none`. Building only the
offsets (vs the full `computeRealView`) drops both `ColumnReader` shapes below the
`rowNested` all-items path:

| benchmark | full drain build | lean view | `rowNested` |
|---|--:|--:|--:|
| `columnNested` (flat leaf) | 15.2 | 10.9 | 12.9 |
| `columnNestedStructural` | 15.4 | 11.4 | 12.9 |

Phantom-bearing (`sparse` / `dense`) batches still build the full `RealView` and
remain slower than `rowNested`; leaning that path is tracked separately.
