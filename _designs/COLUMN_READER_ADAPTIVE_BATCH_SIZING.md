<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Adaptive default batch sizing for the ColumnReader API

Status: **Completed**

## Problem

The reader has two batch-sizing regimes that must agree but did not:

- The `RowReader` path (`FlatRowReader` / `NestedRowReader`) sizes each batch by a **byte budget**: `BatchSizing.computeOptimalBatchSize` targets ~6 MB across all projected columns and caps the resulting row count at `524 288` (`BatchSizing.MAX_BATCH`), so the per-batch column arrays stay within L2 cache regardless of column width or projection breadth.
- The `ColumnReader` / `ColumnReaders` builders used a fixed **record count** default of `262 144` rows.

Because the `ColumnReader` default was a fixed row count, the real per-batch footprint floated with the data: ~1 MB for a single `INT32`, ~2 MB for a single `INT64`/`DOUBLE`, tens of MB for a `BYTE_ARRAY` column once payload and `String` objects are counted, and ~210 MB for a 100-column `INT64` projection (each reader allocating its own `262 144`-row arrays). The batch arrays are freshly allocated per batch (no scratch reuse) and `BatchExchange` prefetch adds a queue-depth multiplier on top, so an oversized default translates directly into allocation and GC churn.

The byte budget itself assumed **one value per row per column**. A repeated (list) column expands each row into many leaf values, so its per-batch value array — and the two `int` level arrays its nested worker holds alongside — are the list's *fan-out* times larger than a row-count budget predicts. A wide `LIST<float32>` (768-element vectors) whose row count fits under the clamp drained the entire column into a single ~512 MB batch: far past the L2 target, memory-bound to both assemble and scan, and at higher fan-out an outright heap exhaustion. So even once both regimes were byte-budgeted, a fan-out-blind estimate still oversized repeated columns and left the regimes disagreeing for them.

## End state

The default batch size for the column path is **byte-budgeted**, routed through the same `BatchSizing.computeOptimalBatchSize` the row path uses, and the budget is scaled by each column's **list fan-out**. The two regimes produce identical batch sizes for the same projection.

- The per-row cost of a column is its value width times its average leaf-values-per-row (fan-out), plus the def/rep level bytes a repeated column's nested worker allocates per value (two `int`s). A flat column's fan-out is `1` and it carries no per-value level arrays, so the budget collapses to the plain value width.
- The row-count floor is removed. Because the value count per batch is now roughly constant across fan-out (`≈ budget / value-bytes`), a high-fan-out column's small row count still carries a full batch of work, so no floor is needed to amortise per-batch overhead. `BatchSizing.MAX_BATCH` (`524 288`) remains the only clamp.
- `ColumnReaderBuilder.batchSize(int)` and `ColumnReadersBuilder.batchSize(int)` remain the explicit override and are honored verbatim. The positivity check is unchanged.
- When the caller does not set `batchSize`, the builder leaves it unset and the size is resolved at `build()` time from the projection's column widths and fan-out.
- The width used is that of **every column the batch decodes**: the full projection on the plain path, and the predicate-augmented projection on the exact-filtering path (the predicate columns allocate arrays too, so they count toward the budget).
- The public `ColumnReader.DEFAULT_BATCH_SIZE` constant is removed: there is no longer a single fixed default to name.

This is a no-op for callers that already pass an explicit `batchSize`. Callers relying on the default get smaller, width-aware batches and lower peak allocation, especially on wide and variable-width projections.

## Mechanics

`batchSize` carries a sentinel of `0` ("unset") from the builders down to the bridge methods on `ParquetFileReader`. `resolveBatchSize(requested, projectedSchema, rowGroups)` returns `requested` when positive and otherwise `BatchSizing.computeOptimalBatchSize(projectedSchema, BatchSizing.valuesPerRow(projectedSchema, rowGroups))`. `valuesPerRow` derives each projected column's fan-out from the row-group metadata — its total leaf value count (`Σ chunk.numValues`) over the total row count (`Σ rowGroup.numRows`) — so the estimate needs both the projection *and* the file's row groups. Resolution happens where both are known:

- `ParquetFileReader.buildColumnReaders` — resolves against the plain projection (unfiltered) or the augmented projection (filtered), with the file's row groups, before constructing `ColumnReaders`.
- `ColumnReader.create` — the single-column, unfiltered entry point builds its own one-column `ProjectedSchema` and resolves against it and its row groups.
- `NestedRowReader.create` — the nested row-materialisation path, sized from the same metadata (threaded from `createRowReader`) so it agrees with the column path for repeated projections. `FlatRowReader` needs no fan-out — a flat schema is one value per row — and is unchanged.

All downstream consumers (`ColumnReaders`, `ColumnReader.createFromIterator`, the workers, `BatchExchange.allocateArray`) continue to receive a concrete, already-resolved positive batch size.
