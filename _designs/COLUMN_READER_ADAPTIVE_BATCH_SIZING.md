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

- The `RowReader` path (`FlatRowReader` / `NestedRowReader`) sizes each batch by a **byte budget**: `BatchSizing.computeOptimalBatchSize` targets ~6 MB across all projected columns and clamps the resulting row count to `[16 384, 524 288]`, so the per-batch column arrays stay within L2 cache regardless of column width or projection breadth.
- The `ColumnReader` / `ColumnReaders` builders used a fixed **record count** default of `262 144` rows.

Because the `ColumnReader` default was a fixed row count, the real per-batch footprint floated with the data: ~1 MB for a single `INT32`, ~2 MB for a single `INT64`/`DOUBLE`, tens of MB for a `BYTE_ARRAY` column once payload and `String` objects are counted, and ~210 MB for a 100-column `INT64` projection (each reader allocating its own `262 144`-row arrays). The batch arrays are freshly allocated per batch (no scratch reuse) and `BatchExchange` prefetch adds a queue-depth multiplier on top, so an oversized default translates directly into allocation and GC churn.

## End state

The default batch size for the column path is **byte-budgeted**, routed through the same `BatchSizing.computeOptimalBatchSize` the row path uses. The two regimes now produce identical batch sizes for the same projection.

- `ColumnReaderBuilder.batchSize(int)` and `ColumnReadersBuilder.batchSize(int)` remain the explicit override and are honored verbatim. The positivity check is unchanged.
- When the caller does not set `batchSize`, the builder leaves it unset and the size is resolved at `build()` time from the projection's column widths.
- The width used is that of **every column the batch decodes**: the full projection on the plain path, and the predicate-augmented projection on the exact-filtering path (the predicate columns allocate arrays too, so they count toward the budget).
- The public `ColumnReader.DEFAULT_BATCH_SIZE` constant is removed: there is no longer a single fixed default to name. `BatchSizing.MAX_BATCH` (`524 288`) remains the upper clamp.

This is a no-op for callers that already pass an explicit `batchSize`. Callers relying on the default get smaller, width-aware batches and lower peak allocation, especially on wide and variable-width projections.

## Mechanics

`batchSize` carries a sentinel of `0` ("unset") from the builders down to the bridge methods on `ParquetFileReader`. `resolveBatchSize(requested, projectedSchema)` returns `requested` when positive and `BatchSizing.computeOptimalBatchSize(projectedSchema)` otherwise. Resolution happens at the single point where the full `ProjectedSchema` is known:

- `ParquetFileReader.buildColumnReaders` — resolves against the plain projection (unfiltered) or the augmented projection (filtered) before constructing `ColumnReaders`.
- `ColumnReader.create` — the single-column, unfiltered entry point builds its own one-column `ProjectedSchema` and resolves against it.

All downstream consumers (`ColumnReaders`, `ColumnReader.createFromIterator`, the workers, `BatchExchange.allocateArray`) continue to receive a concrete, already-resolved positive batch size.
