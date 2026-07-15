/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.concurrent.Executor;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Per-column pipeline that decodes pages in parallel and assembles nested batches.
///
/// Extends [ColumnWorker] with nested-specific assembly: tracking record boundaries
/// via repetition levels, growing value arrays, and copying definition/repetition
/// levels into [NestedBatch] holders.
public class NestedColumnWorker extends ColumnWorker<NestedBatch> {

    private final NestedLevelComputer.Layers layers;

    /// Which derived structures the drain computes per batch, selected by the
    /// constructing reader:
    ///
    /// - `ALL_ITEMS` — the [NestedRowReader] path: the all-items element-validity /
    ///   multi-level-offset index and raw def/rep levels, which
    ///   [NestedBatchIndex] and its consumers read.
    /// - `REAL_VIEW` — the unfiltered [dev.hardwood.reader.ColumnReader] path: the
    ///   real-items [NestedLevelComputer.RealView] (and compacted values), built on
    ///   the drain from the accumulators so the serial consumer does no scan. The
    ///   raw levels and the all-items index are dropped — nothing reads them.
    /// - `REAL_VIEW_KEEP_LEVELS` — the exact-filtered `ColumnReader` path: the raw
    ///   levels are kept so `applySelection`/`compactNestedBatch` can slice them per
    ///   kept record, and the consumer rebuilds the view lazily from the compacted
    ///   levels. The all-items index is still skipped.
    public enum IndexMode { ALL_ITEMS, REAL_VIEW, REAL_VIEW_KEEP_LEVELS }

    private final IndexMode indexMode;

    // --- Nested assembly state (drain thread only) ---
    private Object nestedValues;
    private int[] nestedDefLevels;
    private int[] nestedRepLevels;
    private int[] nestedRecordOffsets;
    private int nestedValueCount;

    /// Capacity (in values) currently backing [#nestedValues]. Tracked
    /// explicitly because the value array and the level arrays grow
    /// independently: the fixed-size-list fast path fills only the value array
    /// and leaves [#nestedDefLevels] / [#nestedRepLevels] at their smaller
    /// capacity, since a fixed-width batch publishes with `null` levels. The
    /// level arrays are grown to match only when a batch falls back to the
    /// regular representation ([#materializeFixedWidthBatchLevels]).
    private int nestedValuesCapacity;

    /// Whether we have seen the first value in the nested stream.
    private boolean nestedFirstValueSeen;

    /// Elements per row of the batch currently being assembled when it is on the
    /// fixed-size-list fast path, or `0` for a regular batch. A batch stays
    /// homogeneous: [#assemblePage] flushes the open batch before switching
    /// between the fast and regular paths (or between different `k`).
    private int batchFixedK;

    /// Creates a new nested column worker.
    ///
    /// @param pageSource yields [PageInfo] objects for this column
    /// @param exchange the output exchange for assembled batches
    /// @param column the column schema
    /// @param batchCapacity rows per batch
    /// @param decompressorFactory for creating page decompressors
    /// @param decodeExecutor executor for decode tasks
    /// @param maxRows maximum rows to assemble (0 = unlimited)
    /// @param layers per-layer descriptor (kinds + def thresholds) for the
    ///        column's schema chain, or `null` for non-repeated columns. The
    ///        descriptor drives the layer-indexed `multiLevelOffsets` shape.
    /// Convenience constructor feeding the all-items path with the fixed-size-list
    /// fast path enabled.
    public NestedColumnWorker(PageSource pageSource, BatchExchange<NestedBatch> exchange,
                              ColumnSchema column, int batchCapacity,
                              DecompressorFactory decompressorFactory,
                              Executor decodeExecutor, long maxRows,
                              NestedLevelComputer.Layers layers) {
        this(pageSource, exchange, column, batchCapacity, decompressorFactory,
             decodeExecutor, maxRows, layers, IndexMode.ALL_ITEMS, true);
    }

    public NestedColumnWorker(PageSource pageSource, BatchExchange<NestedBatch> exchange,
                              ColumnSchema column, int batchCapacity,
                              DecompressorFactory decompressorFactory,
                              Executor decodeExecutor, long maxRows,
                              NestedLevelComputer.Layers layers,
                              IndexMode indexMode,
                              boolean fixedListFastPathEnabled) {
        super(pageSource, exchange, column, batchCapacity, decompressorFactory,
              decodeExecutor, maxRows);
        this.layers = layers;
        this.indexMode = indexMode;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
    }

    @Override
    void initDrainState() {
        int initialCapacity = batchCapacity * 2;
        nestedValues = BatchExchange.allocateArray(column, initialCapacity);
        nestedValuesCapacity = initialCapacity;
        nestedDefLevels = new int[initialCapacity];
        nestedRepLevels = new int[initialCapacity];
        nestedRecordOffsets = new int[batchCapacity];
    }

    @Override
    void assemblePage(Page page, PageRowMask mask) {
        int k = page.fixedListK();
        boolean pageFixedWidth = k > 0;
        // A published batch must stay homogeneous — wholly fixed-width (levels
        // omitted) or wholly regular — but batches must also cut at the same row
        // boundaries across all columns, which is only true at batch-capacity and
        // file boundaries. We therefore never flush at a page boundary: when the
        // open fixed-width batch meets a regular page or a different k, we convert
        // it to the regular representation in place (synthesizing its levels) and
        // keep filling the same batch. A fixed-width page arriving into an
        // already-regular batch is assembled with synthesized levels too.
        if (batchFixedK > 0 && (!pageFixedWidth || k != batchFixedK)) {
            materializeFixedWidthBatchLevels();
        }
        if (pageFixedWidth && (rowsInCurrentBatch == 0 || batchFixedK == k)) {
            assembleFixedWidthPage(page, mask, k, false);
        }
        else if (pageFixedWidth) {
            assembleFixedWidthPage(page, mask, k, true);
        }
        else {
            assembleRegularPage(page, mask);
        }
    }

    /// Converts the open fixed-width fast-path batch to the regular representation
    /// by synthesizing the levels it omitted: every element is present (definition
    /// level `maxDefinitionLevel`) and each record of `k` elements starts a new
    /// list (repetition level `0` at the record start, `1` for the following
    /// `k - 1` elements). The fixed-width path lays records out as contiguous
    /// `k`-value runs from index `0`, so `v % k == 0` marks a record start. Called
    /// when a batch that began fixed-width must absorb a regular or
    /// differently-shaped page rather than being flushed early (which would
    /// misalign sibling columns).
    private void materializeFixedWidthBatchLevels() {
        // The fast path grew only the value array, so the level arrays may be
        // shorter than the values accumulated so far — size them up before
        // synthesizing the omitted levels for every value.
        ensureLevelCapacity(nestedValueCount);
        int k = batchFixedK;
        for (int v = 0; v < nestedValueCount; v++) {
            nestedDefLevels[v] = maxDefinitionLevel;
            nestedRepLevels[v] = (v % k == 0) ? 0 : 1;
        }
        batchFixedK = 0;
    }

    /// Assembly for a fixed-width `k`-element page: every record is a present list
    /// of exactly `k` elements, so record boundaries are implicit and values are
    /// bulk-copied per record with arithmetic record offsets. Masking and
    /// batch/row splitting mirror [#assembleRegularPage] at record granularity.
    ///
    /// When `asRegularBatch` is `false` the batch stays on the fast path: the
    /// level arrays are skipped and `batchFixedK` is stamped so the omitted
    /// levels are the published representation. When it is `true` the page is
    /// folded into an already-regular batch instead — the synthesized levels
    /// (every element present, each record a `0` followed by `k - 1` ones) are
    /// written and `batchFixedK` left `0`, keeping the batch wholly regular.
    /// `batchFixedK` is (re)stamped inside the loop because a mid-page
    /// [#publishCurrentBatch] (at batch capacity) resets it for the next batch.
    private void assembleFixedWidthPage(Page page, PageRowMask mask, int k, boolean asRegularBatch) {
        nestedFirstValueSeen = true;
        int pageRecords = page.size() / k;
        boolean maskAll = mask.isAll();
        // Kept record ranges are contiguous k-runs, so each range is copied in
        // batch-capacity-sized chunks with one arraycopy per chunk rather than one
        // per record. Unmasked = the whole page; masked = each interval (page-local
        // record indices), clamped to the page.
        int rangeCount = maskAll ? 1 : mask.intervalCount();
        for (int rangeIdx = 0; rangeIdx < rangeCount; rangeIdx++) {
            int r = maskAll ? 0 : Math.max(0, mask.start(rangeIdx));
            int rangeEnd = maskAll ? pageRecords : Math.min(pageRecords, mask.end(rangeIdx));
            while (r < rangeEnd) {
                if (rowsInCurrentBatch >= batchCapacity) {
                    publishCurrentBatch();
                    if (done) {
                        return;
                    }
                }
                if (maxRows > 0 && totalRowsAssembled >= maxRows) {
                    if (rowsInCurrentBatch > 0) {
                        publishCurrentBatch();
                    }
                    finishDrain();
                    return;
                }

                int records = Math.min(rangeEnd - r, batchCapacity - rowsInCurrentBatch);
                if (maxRows > 0) {
                    records = (int) Math.min(records, maxRows - totalRowsAssembled);
                }
                int span = records * k;
                int destStart = nestedValueCount;
                if (asRegularBatch) {
                    ensureNestedCapacity(nestedValueCount + span);
                }
                else {
                    ensureValueCapacity(nestedValueCount + span);
                }
                copyValueRun(page, r * k, destStart, span);
                for (int j = 0; j < records; j++) {
                    nestedRecordOffsets[rowsInCurrentBatch + j] = destStart + j * k;
                }
                if (asRegularBatch) {
                    Arrays.fill(nestedDefLevels, destStart, destStart + span, maxDefinitionLevel);
                    Arrays.fill(nestedRepLevels, destStart, destStart + span, 1);
                    for (int j = 0; j < records; j++) {
                        nestedRepLevels[destStart + j * k] = 0;
                    }
                }
                else {
                    batchFixedK = k;
                }
                nestedValueCount += span;
                rowsInCurrentBatch += records;
                totalRowsAssembled += records;
                r += records;
            }
        }
    }

    private void assembleRegularPage(Page page, PageRowMask mask) {
        int pageSize = page.size();
        int[] pageDefLevels = page.definitionLevels();
        int[] pageRepLevels = page.repetitionLevels();

        // Validate: the very first repetition level in the column must be 0
        if (!nestedFirstValueSeen && pageSize > 0) {
            nestedFirstValueSeen = true;
            if (pageRepLevels != null && pageRepLevels[0] != 0) {
                throw new IllegalStateException(
                        "Invalid column chunk for '" + column.name()
                        + "': first repetition level must be 0 but was " + pageRepLevels[0]);
            }
        }

        // Whole-page all-present fast path: every leaf is present (PageDecoder's
        // O(1) def-gate), so on an unmasked page the values are one contiguous,
        // record-aligned run that can be bulk-copied instead of walked per element.
        // Byte-array leaves stay on the per-element path — their values are appended
        // into a shared buffer, not arraycopy-able.
        if (page.allPresent() && mask.isAll() && !(page instanceof Page.ByteArrayPage)) {
            assembleAllPresentPage(page, pageRepLevels);
            return;
        }

        boolean maskAll = mask.isAll();
        int intervalCount = maskAll ? 0 : mask.intervalCount();
        int intervalCursor = 0;
        int recordIndex = -1;

        for (int i = 0; i < pageSize; i++) {
            int repLevel = pageRepLevels != null ? pageRepLevels[i] : 0;
            boolean atRecordStart = repLevel == 0;

            if (atRecordStart) {
                recordIndex++;
                if (!maskAll) {
                    while (intervalCursor < intervalCount
                            && recordIndex >= mask.end(intervalCursor)) {
                        intervalCursor++;
                    }
                }
            }

            if (!maskAll && (intervalCursor >= intervalCount
                    || recordIndex < mask.start(intervalCursor))) {
                continue;
            }

            // A record start (repLevel = 0) closes the previous top-level record and
            // opens a new one — except at the first kept value of the stream, where
            // we also start record 0. Masked-out records are already filtered by the
            // `continue` above, so they never reach this branch.
            if (atRecordStart && (nestedValueCount > 0 || rowsInCurrentBatch > 0)) {
                // Previous record is complete — check if batch is full
                if (rowsInCurrentBatch >= batchCapacity) {
                    publishCurrentBatch();
                    if (done) {
                        return;
                    }
                }

                // Check maxRows after publishing
                if (maxRows > 0 && totalRowsAssembled >= maxRows) {
                    if (rowsInCurrentBatch > 0) {
                        publishCurrentBatch();
                    }
                    finishDrain();
                    return;
                }

                // Start new record
                if (nestedRecordOffsets.length <= rowsInCurrentBatch) {
                    nestedRecordOffsets = Arrays.copyOf(nestedRecordOffsets,
                            nestedRecordOffsets.length * 2);
                }
                nestedRecordOffsets[rowsInCurrentBatch] = nestedValueCount;
                rowsInCurrentBatch++;
                totalRowsAssembled++;
            }
            else if (nestedValueCount == 0 && rowsInCurrentBatch == 0) {
                // First kept value of the stream — start record 0. May not be the
                // first value of the first page if the mask skipped earlier records.
                nestedRecordOffsets[0] = 0;
                rowsInCurrentBatch = 1;
                totalRowsAssembled++;
            }

            // Ensure capacity for the new value
            ensureNestedCapacity(nestedValueCount + 1);

            // Copy value and levels
            nestedDefLevels[nestedValueCount] = pageDefLevels != null
                    ? pageDefLevels[i] : maxDefinitionLevel;
            nestedRepLevels[nestedValueCount] = repLevel;
            copyOneValue(page, i, nestedValues, nestedValueCount);
            nestedValueCount++;
        }
    }

    /// Whole-page assembly for an all-present, unmasked page. Every leaf is present
    /// ([Page#allPresent]), so the page's values are 1:1 with positions and
    /// contiguous. The record and batch-capacity bookkeeping mirrors
    /// [#assembleRegularPage] — a record opens at each `repLevel == 0`, a full batch
    /// is published at the next record boundary — but the per-element value copy and
    /// level stores are deferred and flushed as bulk operations once per batch (see
    /// [#flushRun]). Records may span pages, so the batch is left open at page end
    /// for the next page to continue.
    private void assembleAllPresentPage(Page page, int[] pageRepLevels) {
        int pageSize = page.size();
        if (pageSize == 0) {
            return;
        }
        // The current batch's pending run of not-yet-flushed values: page positions
        // [runStartPage, i), written at batch value index starting at runStartDest.
        int runStartPage = 0;
        int runStartDest = nestedValueCount;

        for (int i = 0; i < pageSize; i++) {
            if (pageRepLevels != null && pageRepLevels[i] != 0) {
                continue; // continuation of the open record, not a boundary
            }
            int batchValueIndex = runStartDest + (i - runStartPage);
            if (batchValueIndex > 0 || rowsInCurrentBatch > 0) {
                // A record just closed; open the next, publishing first if the batch
                // is full (records are never split across batches).
                if (rowsInCurrentBatch >= batchCapacity) {
                    flushRun(page, pageRepLevels, runStartPage, i, runStartDest);
                    publishCurrentBatch();
                    if (done) {
                        return;
                    }
                    runStartPage = i;
                    runStartDest = 0;
                }
                if (maxRows > 0 && totalRowsAssembled >= maxRows) {
                    flushRun(page, pageRepLevels, runStartPage, i, runStartDest);
                    if (rowsInCurrentBatch > 0) {
                        publishCurrentBatch();
                    }
                    finishDrain();
                    return;
                }
                if (nestedRecordOffsets.length <= rowsInCurrentBatch) {
                    nestedRecordOffsets = Arrays.copyOf(nestedRecordOffsets,
                            nestedRecordOffsets.length * 2);
                }
                nestedRecordOffsets[rowsInCurrentBatch] = runStartDest + (i - runStartPage);
                rowsInCurrentBatch++;
                totalRowsAssembled++;
            }
            else {
                // First kept value of the stream — open record 0.
                nestedRecordOffsets[0] = 0;
                rowsInCurrentBatch = 1;
                totalRowsAssembled++;
            }
        }
        flushRun(page, pageRepLevels, runStartPage, pageSize, runStartDest);
    }

    /// Flushes the pending run of an all-present page — page positions
    /// `[fromPage, toPage)` written at batch value index `destStart` — as three bulk
    /// operations: the values via [#copyValueRun], the constant definition levels via
    /// `Arrays.fill`, and the repetition levels via `System.arraycopy` (all `0` when
    /// the column is not repeated). Advances [#nestedValueCount] past the run.
    private void flushRun(Page page, int[] pageRepLevels, int fromPage, int toPage, int destStart) {
        int span = toPage - fromPage;
        if (span <= 0) {
            return;
        }
        ensureNestedCapacity(destStart + span);
        copyValueRun(page, fromPage, destStart, span);
        Arrays.fill(nestedDefLevels, destStart, destStart + span, maxDefinitionLevel);
        if (pageRepLevels != null) {
            System.arraycopy(pageRepLevels, fromPage, nestedRepLevels, destStart, span);
        }
        else {
            Arrays.fill(nestedRepLevels, destStart, destStart + span, 0);
        }
        nestedValueCount = destStart + span;
    }

    @Override
    void publishCurrentBatch() {
        if (done) {
            return;
        }
        currentBatch.recordCount = rowsInCurrentBatch;
        currentBatch.valueCount = nestedValueCount;
        currentBatch.fixedListK = batchFixedK;
        if (batchFixedK > 0 || indexMode == IndexMode.REAL_VIEW) {
            // Fixed-width batches omit levels (boundaries are implicit); the
            // unfiltered real-view path derives the view from the accumulators in
            // computeIndex and drops the raw levels, which nothing downstream reads.
            currentBatch.definitionLevels = null;
            currentBatch.repetitionLevels = null;
        }
        else {
            currentBatch.definitionLevels = Arrays.copyOf(nestedDefLevels, nestedValueCount);
            currentBatch.repetitionLevels = Arrays.copyOf(nestedRepLevels, nestedValueCount);
        }
        currentBatch.recordOffsets = Arrays.copyOf(nestedRecordOffsets, rowsInCurrentBatch);
        currentBatch.values = trimValues(nestedValues, nestedValueCount);
        currentBatch.fileName = currentBatchFileName;

        // Compute index structures before publishing so the consumer thread
        // doesn't need to do expensive index computation.
        computeIndex(currentBatch);

        long t0 = System.nanoTime();
        try {
            if (!exchange.publish(currentBatch)) {
                done = true; // stopped during publish
                return;
            }
            currentBatch = exchange.takeBatch();
            if (currentBatch == null) {
                done = true; // stopped during take
                return;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            done = true;
            return;
        }
        publishBlockNanos += System.nanoTime() - t0;
        batchesPublished++;

        rowsInCurrentBatch = 0;
        nestedValueCount = 0;
        batchFixedK = 0;
        // The accumulator is reused for the next batch; clear its dictionary
        // slot so dictIndex state is rebuilt from scratch (the dictIndices array
        // is retained and overwritten in place).
        if (nestedValues instanceof BinaryBatchValues bbv) {
            bbv.dictionary = null;
        }
    }

    /// Computes index structures (element validity and layer-indexed
    /// multi-level offsets) on the batch before it is published. Set bits
    /// indicate **present** values; `null` validity references mean every
    /// item at that scope is present in this batch.
    ///
    /// `multiLevelOffsets` is indexed by **layer** (one slot per `STRUCT` /
    /// `REPEATED` layer). `multiLevelOffsets[k]` is `null` for `STRUCT`
    /// layers and sentinel-suffixed for `REPEATED` layers.
    private void computeIndex(NestedBatch batch) {
        if (indexMode == IndexMode.REAL_VIEW) {
            // Unfiltered ColumnReader: the consumer reads only the real-items view
            // and values, so build both here on the drain (off the serial consumer)
            // and skip the all-items index. An all-present batch has no phantom
            // positions, so its offsets equal the all-items offsets: build just the
            // lean offsets view (no per-layer validity or gather map) and pass the
            // dense values through. A batch with phantom positions builds the full
            // view and gathers the real leaf values. Building the view eagerly on the
            // drain keeps reconstruction off the serial consumer for both shapes: a
            // structural read gets its offsets without a consumer-side scan, and a
            // flat leaf read simply ignores them.
            boolean allPresent = batch.fixedListK > 0 || allDefsMax(batch.valueCount);
            if (allPresent) {
                batch.realValues = batch.values;
                // With no phantom positions the real-items offsets equal the all-items
                // offsets, so build just the boundaries (skipping the full projection's
                // per-layer presence, leaf-validity, and gather-map work). A structural
                // consumer reads these drain-built offsets without a consumer-side scan;
                // a flat leaf consumer ignores them.
                batch.realView = buildAllPresentView(batch);
            }
            else {
                NestedLevelComputer.RealView rv = buildRealView(batch);
                batch.realView = rv;
                int[] map = rv.realToRawLeaf();
                batch.realValues = map != null ? LeafCompaction.compact(batch.values, map) : batch.values;
            }
            batch.elementValidity = null;
            batch.multiLevelOffsets = null;
            return;
        }
        batch.realView = null;
        batch.realValues = null;

        if (indexMode == IndexMode.REAL_VIEW_KEEP_LEVELS) {
            // Exact-filtered ColumnReader: applySelection compacts the batch from the
            // raw levels (kept above) before the consumer reads it, then the consumer
            // rebuilds the view lazily. Nothing is derived on the drain here; the
            // all-items index the ColumnReader never reads is skipped.
            batch.elementValidity = null;
            batch.multiLevelOffsets = null;
            return;
        }

        if (batch.fixedListK > 0) {
            // All elements present, boundaries at multiples of k: no validity and
            // arithmetic layer offsets, skipping the O(valueCount) level scans.
            batch.elementValidity = null;
            batch.multiLevelOffsets = NestedLevelComputer.fixedListLayerOffsets(
                    batch.fixedListK, batch.recordCount, layers);
            return;
        }

        int[] defLevels = batch.definitionLevels;
        int valueCount = batch.valueCount;

        batch.elementValidity = NestedLevelComputer.computeElementValidity(
                defLevels, valueCount, maxDefinitionLevel);

        if (layers != null && layers.count() > 0 && valueCount > 0) {
            batch.multiLevelOffsets = NestedLevelComputer.computeLayerOffsets(
                    batch.repetitionLevels, valueCount, batch.recordCount, layers);
        }
        else {
            batch.multiLevelOffsets = null;
        }
    }

    /// True when every leaf in the batch is present (def at max) — no null/empty
    /// parents, so a flat consumer needs neither compaction nor per-layer offsets.
    private boolean allDefsMax(int n) {
        for (int i = 0; i < n; i++) {
            if (nestedDefLevels[i] != maxDefinitionLevel) {
                return false;
            }
        }
        return true;
    }

    /// Full real-items view for a batch with phantom (null/empty parent) positions,
    /// built on the drain so the serial consumer reads it without a scan. Scans the
    /// def/rep levels from the accumulators ([#nestedDefLevels] / [#nestedRepLevels],
    /// still intact for `[0, valueCount)` until the counts reset after publish) rather
    /// than the batch, so the `REAL_VIEW` path can leave the batch's level arrays
    /// `null`. An all-present batch takes the cheaper [#buildAllPresentView] instead.
    private NestedLevelComputer.RealView buildRealView(NestedBatch batch) {
        return NestedLevelComputer.computeRealView(
                nestedDefLevels, nestedRepLevels,
                batch.valueCount, batch.recordCount, maxDefinitionLevel, layers);
    }

    /// Lean real-items view for an all-present batch: only the layer offsets, which
    /// (absent phantom positions) equal the all-items offsets. Every validity is
    /// `null` (all present) and `realToRawLeaf` is `null` (the dense values are the
    /// real-leaf values). Cheaper than [#buildRealView] — no per-layer presence,
    /// leaf-validity, or gather-map scan — so a flat leaf consumer pays little for it
    /// while a structural consumer reads the list boundaries off the drain.
    private NestedLevelComputer.RealView buildAllPresentView(NestedBatch batch) {
        int layerCount = layers.count();
        int[][] offsets = batch.fixedListK > 0
                ? NestedLevelComputer.fixedListLayerOffsets(batch.fixedListK, batch.recordCount, layers)
                : NestedLevelComputer.computeLayerOffsets(
                        nestedRepLevels, batch.valueCount, batch.recordCount, layers);
        return new NestedLevelComputer.RealView(
                offsets, new long[layerCount][], null, batch.valueCount, null);
    }

    // ==================== Nested Helpers ====================

    private static final byte[] EMPTY_BYTES = new byte[0];

    /// Copies a single value from a page into the batch values array.
    /// For byte-array physical types, the value is appended into the
    /// shared bytes buffer of [BinaryBatchValues] and its offsets are
    /// updated; null entries collapse to a zero-length span (or, for
    /// `FIXED_LEN_BYTE_ARRAY`, leave the pre-filled trivial offsets
    /// unchanged).
    private void copyOneValue(Page page, int srcIndex, Object destValues, int destIndex) {
        switch (page) {
            case Page.IntPage p -> ((int[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.LongPage p -> ((long[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.FloatPage p -> ((float[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.DoublePage p -> ((double[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.BooleanPage p -> ((boolean[]) destValues)[destIndex] = p.values()[srcIndex];
            case Page.ByteArrayPage p -> {
                byte[] val = p.values()[srcIndex];
                BinaryBatchValues bbv = (BinaryBatchValues) destValues;
                if (val != null) {
                    bbv.appendAt(destIndex, val, 0, val.length);
                }
                else if (physicalType != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    // Variable-length null: zero-length span at this index.
                    bbv.appendAt(destIndex, EMPTY_BYTES, 0, 0);
                }
                // FIXED_LEN null: trivial offsets stay; bytes content is undefined.
                // Record the per-value dictionary index so stringAt can intern; a
                // no-op for non-string columns, and plain/null values fall back
                // to the packed-byte path (see BinaryBatchValues#recordDictIndex).
                bbv.recordDictIndex(p.dictIndices(), p.dictionary(), srcIndex, destIndex);
            }
        }
    }

    /// Ensures both the value and level accumulators hold at least `needed`
    /// slots. Used by the regular path, which writes values and levels at the
    /// same indices. The two are grown independently ([#ensureValueCapacity],
    /// [#ensureLevelCapacity]) and may end up at different capacities, but both
    /// cover `[0, needed)`.
    private void ensureNestedCapacity(int needed) {
        ensureLevelCapacity(needed);
        ensureValueCapacity(needed);
    }

    /// Ensures the value accumulator holds at least `needed` slots, growing it
    /// (and, for [BinaryBatchValues], its offsets/bytes) independently of the
    /// level arrays. The fixed-size-list fast path grows only this array.
    private void ensureValueCapacity(int needed) {
        if (needed <= nestedValuesCapacity) {
            return;
        }
        int newCapacity = nestedValuesCapacity * 2;
        if (newCapacity < needed) {
            newCapacity = needed;
        }
        nestedValues = growValues(nestedValues, newCapacity);
        nestedValuesCapacity = newCapacity;
    }

    /// Ensures the definition/repetition level arrays hold at least `needed`
    /// slots. Only the regular representation writes levels, so the fast path
    /// never calls this; a fixed-width batch that must fall back grows the level
    /// arrays here, lazily, at the point of conversion.
    private void ensureLevelCapacity(int needed) {
        if (needed <= nestedDefLevels.length) {
            return;
        }
        int newCapacity = nestedDefLevels.length * 2;
        if (newCapacity < needed) {
            newCapacity = needed;
        }
        nestedDefLevels = Arrays.copyOf(nestedDefLevels, newCapacity);
        nestedRepLevels = Arrays.copyOf(nestedRepLevels, newCapacity);
    }

    /// Copies `count` consecutive values from `page` starting at `srcStart` into
    /// the value accumulator at `destStart`. Primitive pages bulk-copy; byte
    /// arrays fall back to the per-element append.
    private void copyValueRun(Page page, int srcStart, int destStart, int count) {
        switch (page) {
            case Page.IntPage p -> System.arraycopy(p.values(), srcStart, (int[]) nestedValues, destStart, count);
            case Page.LongPage p -> System.arraycopy(p.values(), srcStart, (long[]) nestedValues, destStart, count);
            case Page.FloatPage p -> System.arraycopy(p.values(), srcStart, (float[]) nestedValues, destStart, count);
            case Page.DoublePage p -> System.arraycopy(p.values(), srcStart, (double[]) nestedValues, destStart, count);
            case Page.BooleanPage p -> System.arraycopy(p.values(), srcStart, (boolean[]) nestedValues, destStart, count);
            // The fast path is gated to primitive numeric element types
            // (PageDecoder#isFixedListElementSupported), so a byte-array page
            // never reaches fixed-width assembly.
            case Page.ByteArrayPage p -> throw new IllegalStateException(
                    "byte-array element unexpected on the fixed-size-list fast path");
        }
    }

    /// Grows a typed values array to the new capacity. For
    /// [BinaryBatchValues], the offsets array is grown to `newCapacity + 1`
    /// (re-filling the trivial `i * width` mapping for
    /// `FIXED_LEN_BYTE_ARRAY`); the bytes buffer for variable-length types
    /// grows lazily on overflow inside [BinaryBatchValues#appendAt], but for
    /// fixed-width it grows here to keep `width * newCapacity`.
    private Object growValues(Object values, int newCapacity) {
        return switch (values) {
            case int[] a -> Arrays.copyOf(a, newCapacity);
            case long[] a -> Arrays.copyOf(a, newCapacity);
            case float[] a -> Arrays.copyOf(a, newCapacity);
            case double[] a -> Arrays.copyOf(a, newCapacity);
            case boolean[] a -> Arrays.copyOf(a, newCapacity);
            case BinaryBatchValues bbv -> growBinaryBatchValues(bbv, newCapacity);
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }

    private BinaryBatchValues growBinaryBatchValues(BinaryBatchValues bbv, int newCapacity) {
        int oldCapacity = bbv.offsets.length - 1;
        if (newCapacity <= oldCapacity) {
            return bbv;
        }
        int[] newOffsets = Arrays.copyOf(bbv.offsets, newCapacity + 1);
        if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            int width = column.typeLength();
            for (int i = oldCapacity + 1; i <= newCapacity; i++) {
                newOffsets[i] = Math.multiplyExact(i, width);
            }
            byte[] newBytes = new byte[Math.multiplyExact(width, newCapacity)];
            System.arraycopy(bbv.bytes, 0, newBytes, 0, bbv.bytes.length);
            bbv.bytes = newBytes;
        }
        // Keep dictIndices sized to the value capacity once the accumulator has
        // switched onto the dictionary representation.
        if (bbv.dictIndices != null) {
            bbv.dictIndices = Arrays.copyOf(bbv.dictIndices, newCapacity);
        }
        bbv.offsets = newOffsets;
        return bbv;
    }

    /// Snapshots the assembled values for `[0, size)` into an independent array
    /// the published batch owns outright.
    ///
    /// The drain reuses its accumulators ([#nestedValues] and friends) for the
    /// next batch — [#publishCurrentBatch] only resets the counts, not the
    /// backing storage — so the published batch must not alias them. For
    /// [BinaryBatchValues] that means copying the meaningful **bytes** prefix as
    /// well as the offsets: sharing the bytes buffer would let the next batch's
    /// `appendAt` (which restarts at byte offset 0) overwrite the still-unread
    /// rows of the batch just published.
    private Object trimValues(Object values, int size) {
        return switch (values) {
            case int[] a -> Arrays.copyOf(a, size);
            case long[] a -> Arrays.copyOf(a, size);
            case float[] a -> Arrays.copyOf(a, size);
            case double[] a -> Arrays.copyOf(a, size);
            case boolean[] a -> Arrays.copyOf(a, size);
            case BinaryBatchValues bbv -> {
                int byteLength = bbv.offsets[size];
                BinaryBatchValues trimmed = new BinaryBatchValues(
                        Arrays.copyOf(bbv.bytes, byteLength),
                        Arrays.copyOf(bbv.offsets, size + 1));
                if (bbv.dictionary != null) {
                    trimmed.dictionary = bbv.dictionary;
                    trimmed.dictIndices = Arrays.copyOf(bbv.dictIndices, size);
                }
                yield trimmed;
            }
            default -> throw new IllegalStateException("Unexpected values array type: " + values.getClass());
        };
    }
}
