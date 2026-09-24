/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.CompiledBatchFilter;
import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BatchMatchMerger;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.PredicateView;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.schema.FileSchema;

/// Computes the per-batch **record selection** that makes a column-reader
/// filter exact (#624). Given the already-decoded batches of the decoded
/// projection (payload columns plus the predicate columns), it produces the
/// ascending indices of the records that satisfy the predicate, which the
/// [ColumnScan] then uses to compact each payload column.
///
/// Two backends, chosen once at construction:
///
/// - **Drain-side** — when [BatchFilterCompiler] accepts the predicate (flat,
///   top-level, supported `(type, op)`), a [BatchMatchMerger] runs the
///   per-column matcher fragments over the flat batches and combines them into
///   one survivor bitmap. This is the row reader's drain-side merge, in the
///   mode where the merger runs the matchers itself because there are no
///   worker threads to have run them already.
/// - **Record matcher** — otherwise (nested paths, float16, geospatial,
///   unsupported operators) the compiled [RowMatcher] is evaluated per record over a
///   [PredicateView] of the predicate columns — the accessor the row readers
///   evaluate the same matcher against.
final class SelectionEngine {

    // Drain-side backend (null when the record-matcher backend is used).
    private final BatchMatchMerger merger;
    /// The batches [#merger] reads, indexed by projected column index. Staged
    /// here rather than passed straight through because the merger takes the
    /// batches, and this engine holds cursors. `null` on the record-matcher
    /// backend.
    private final BatchExchange.Batch[] stagedBatches;
    /// The projected indices [#merger] reads — its own set, read once at
    /// construction, so nothing here re-derives which columns the plan touches.
    /// `null` on the record-matcher backend.
    private final int[] stagedColumns;

    private final ColumnCursor[] cursorsByProjectedIndex;

    // Record-matcher backend (null when the drain-side backend is used).
    private final RowMatcher rowMatcher;
    private final PredicateView predicateView;
    /// The batches [#predicateView] reads, indexed by projected column index and
    /// refreshed per evaluated batch. `null` on the drain-side backend.
    private final BatchExchange.Batch[] flatBatches;
    private final NestedBatch[] nestedBatches;

    /// Reusable buffer holding the matching record indices of the current batch
    /// in `[0, count)`. Owned by the engine and overwritten every batch, so the
    /// [ColumnScan] must consume it (apply it to all payload cursors)
    /// before the next [#computeSelection]. Sized to the batch capacity.
    private final int[] selection;

    private SelectionEngine(BatchMatchMerger merger, int columnCount,
                            ColumnCursor[] cursorsByProjectedIndex,
                            RowMatcher rowMatcher, PredicateView predicateView, int[] selection) {
        this.merger = merger;
        this.stagedBatches = merger != null ? new BatchExchange.Batch[columnCount] : null;
        this.stagedColumns = merger != null ? merger.referencedColumns() : null;
        this.cursorsByProjectedIndex = cursorsByProjectedIndex;
        this.rowMatcher = rowMatcher;
        this.predicateView = predicateView;
        this.flatBatches = predicateView != null ? new BatchExchange.Batch[columnCount] : null;
        this.nestedBatches = predicateView != null ? new NestedBatch[columnCount] : null;
        this.selection = selection;
    }

    /// Builds an engine for `resolved` over the decoded projection, reading
    /// predicate values from the current batches of `cursorsByProjectedIndex` (indexed
    /// by the decoded column index).
    static SelectionEngine create(FileSchema schema, ProjectedSchema decoded,
                                  ResolvedPredicate resolved,
                                  ColumnCursor[] cursorsByProjectedIndex, int batchSize) {
        int wordsLen = (batchSize + 63) >>> 6;
        int[] selection = new int[batchSize];
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(
                resolved, schema, decoded::toProjectedIndex);

        if (compiled != null) {
            // Owning mode: no column workers ran the matchers, so the merger runs
            // them itself into buffers it allocates.
            BatchMatchMerger merger = BatchMatchMerger.owning(
                    compiled, cursorsByProjectedIndex.length, wordsLen);
            return new SelectionEngine(merger, cursorsByProjectedIndex.length,
                    cursorsByProjectedIndex, null, null, selection);
        }

        // Record-matcher backend, over a view of the predicate columns' batches. The
        // nested batches of the filtered path arrive without element validity, which
        // the view derives from their definition levels.
        PredicateView view = PredicateView.create(schema, decoded, resolved,
                p -> cursorsByProjectedIndex[p].isNested(), true);
        RowMatcher matcher = RecordFilterCompiler.compile(resolved, schema, view::indexOf);
        return new SelectionEngine(null, cursorsByProjectedIndex.length,
                cursorsByProjectedIndex, matcher, view, selection);
    }

    /// Computes the matching records of the current batch into the reusable
    /// [#selection] buffer and returns their count, or `-1` when every record
    /// matches (the no-compaction fast path). The indices live in
    /// `selection()[0, count)` until the next call.
    ///
    /// A batch whose row group statistics proved to match in full is answered
    /// `-1` without evaluating anything: a predicate column outside the
    /// projection is not read for such a batch, and its cursor holds an earlier
    /// step's batch. The first cursor is a payload column's, which every step
    /// advances, and every worker flushes where the flag changes, so its batch
    /// speaks for the step.
    int computeSelection(int recordCount) {
        if (cursorsByProjectedIndex[0].filterAlwaysMatches()) {
            return -1;
        }
        return merger != null
                ? computeDrainSide(recordCount)
                : computeRecordMatcher(recordCount);
    }

    /// The reusable selection buffer; valid in `[0, count)` for the count
    /// returned by the most recent [#computeSelection].
    int[] selection() {
        return selection;
    }

    private int computeDrainSide(int recordCount) {
        for (int i = 0; i < stagedColumns.length; i++) {
            int p = stagedColumns[i];
            stagedBatches[p] = cursorsByProjectedIndex[p].flatBatch();
        }
        return collectSetBits(merger.merge(stagedBatches, recordCount), recordCount);
    }

    private int computeRecordMatcher(int recordCount) {
        for (int p = 0; p < cursorsByProjectedIndex.length; p++) {
            ColumnCursor cursor = cursorsByProjectedIndex[p];
            if (cursor.isNested()) {
                nestedBatches[p] = cursor.nestedBatch();
            }
            else {
                flatBatches[p] = cursor.flatBatch();
            }
        }
        predicateView.refresh(flatBatches, nestedBatches, cursorsByProjectedIndex[0].fileName());
        int count = 0;
        for (int r = 0; r < recordCount; r++) {
            predicateView.setRecord(r);
            if (rowMatcher.test(predicateView)) {
                selection[count++] = r;
            }
        }
        return count == recordCount ? -1 : count;
    }

    /// Collects set bits in `[0, recordCount)` into [#selection] and returns the
    /// count, or `-1` when all are set (the no-compaction fast path).
    private int collectSetBits(long[] words, int recordCount) {
        int count = 0;
        for (int r = 0; r < recordCount; r++) {
            if ((words[r >>> 6] & (1L << r)) != 0L) {
                selection[count++] = r;
            }
        }
        return count == recordCount ? -1 : count;
    }

    // ==================== Predicate column discovery ====================

    /// File leaf-column paths referenced by `resolved`, in first-seen order.
    /// Used to extend the decoded projection so the predicate columns are decoded.
    static List<String> predicateColumnPaths(ResolvedPredicate resolved, FileSchema schema) {
        Set<Integer> indices = PredicateView.predicateColumns(resolved);
        List<String> paths = new ArrayList<>(indices.size());
        for (int columnIndex : indices) {
            paths.add(schema.getColumn(columnIndex).fieldPath().toString());
        }
        return paths;
    }
}
