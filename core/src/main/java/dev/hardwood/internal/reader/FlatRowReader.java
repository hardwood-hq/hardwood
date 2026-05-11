/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// High-performance row reader for flat schemas.
///
/// This is a `final` class with all hot-path methods defined directly (not inherited),
/// giving the JIT a single concrete class with monomorphic call sites. Supports all
/// primitive types, logical type conversions (date, time, timestamp, decimal, UUID,
/// string), and both name-based and index-based access.
public final class FlatRowReader implements RowReader {

    private static final BitSet NO_NULLS = new BitSet(0);

    private final BatchExchange<BatchExchange.Batch>[] exchanges;
    private final FlatColumnWorker[] columnWorkers;
    private final int columnCount;

    // Schema info for name lookup and logical type conversion
    private final FileSchema fileSchema;
    private final ProjectedSchema projectedSchema;
    private final StringToIntMap nameToIndex;
    private final PhysicalType[] physicalTypes;
    private final ColumnSchema[] columnSchemas;

    // Hot fields — directly owned, no inheritance
    private Object[] flatValueArrays;
    private BitSet[] flatNulls;
    private BatchExchange.Batch[] previousBatches;
    private int rowIndex = -1;
    private int batchSize = 0;
    private boolean exhausted;

    // Drain-side filter path: when drainSide is true, iterate via nextSetBit over
    // combinedWords. When false, the original rowIndex++ path is taken.
    private final boolean drainSide;
    /// AND-merged per-row matches for the current batch. `null` when `!drainSide`.
    /// Multi-column case: an owned buffer filled by [#intersectMatches]. Single-column
    /// case: aliased per batch to the underlying [BatchExchange.Batch#matches] array
    /// (no copy needed — there is nothing to intersect).
    private long[] combinedWords;
    /// Projected indices of columns that have a [ColumnBatchMatcher] column-filter installed.
    /// Iterating this array in [#intersectMatches] avoids per-column null checks
    /// on `Batch.matches` and skips columns without columnFilters entirely. `null`
    /// when `!drainSide`.
    private final int[] filteredColumns;
    private int pendingRowIndex = -1;
    /// Exclusive upper bound of the current run of consecutive-1 bits in
    /// [#combinedWords] starting at or before `rowIndex + 1`. While
    /// `rowIndex + 1 < runEndExclusive`, [#hasNext] can advance without
    /// invoking [#nextSetBit] — replacing per-row mask + `numberOfTrailingZeros`
    /// with a simple bound check, which is the win for match-all-like batches.
    /// Reset to 0 each time a new batch loads.
    private int runEndExclusive;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;

    public FlatRowReader(BatchExchange<BatchExchange.Batch>[] exchanges, FlatColumnWorker[] columnWorkers,
                         FileSchema fileSchema, ProjectedSchema projectedSchema,
                         boolean drainSide, int wordsLen, int[] filteredColumns) {
        this.exchanges = exchanges;
        this.columnWorkers = columnWorkers;
        this.columnCount = exchanges.length;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.flatValueArrays = new Object[columnCount];
        this.flatNulls = new BitSet[columnCount];
        this.previousBatches = new BatchExchange.Batch[columnCount];
        this.drainSide = drainSide;
        // Multi-column drain needs a private buffer for the AND-merged words.
        // Single-column drain aliases the batch's matches array directly (see
        // intersectMatches) so no allocation is needed here.
        this.combinedWords = drainSide && filteredColumns != null && filteredColumns.length > 1
                ? new long[wordsLen]
                : null;
        this.filteredColumns = filteredColumns;

        // Build name-to-index map and cache column metadata
        this.nameToIndex = new StringToIntMap(columnCount);
        this.physicalTypes = new PhysicalType[columnCount];
        this.columnSchemas = new ColumnSchema[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema col = fileSchema.getColumn(originalIndex);
            nameToIndex.put(col.name(), i);
            physicalTypes[i] = col.type();
            columnSchemas[i] = col;
        }
    }

    /// Eagerly loads the first batch. Must be called after construction.
    public void initialize() {
        if (!loadNextBatch()) {
            exhausted = true;
        }
    }

    // ==================== Factory ====================

    /// Creates a flat v3 pipeline and returns a [RowReader].
    ///
    /// Wires up `RowGroupIterator → PageSource → ColumnWorker → BatchExchange → FlatRowReader`,
    /// starts all column workers, initializes the reader, and wraps with
    /// [FilteredRowReader] if a filter is present.
    ///
    /// @param rowGroupIterator pre-configured iterator (file opened, first file set, initialized)
    /// @param schema the file schema
    /// @param projectedSchema the projected column schema
    /// @param context the hardwood context
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited), enforced by [ColumnWorker] drain
    /// @return a [FlatRowReader] or [FilteredRowReader]
    public static RowReader create(RowGroupIterator rowGroupIterator,
                                   FileSchema schema,
                                   ProjectedSchema projectedSchema,
                                   HardwoodContextImpl context,
                                   ResolvedPredicate filter,
                                   long maxRows) {
        int batchSize = BatchSizing.computeOptimalBatchSize(projectedSchema);
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();

        // Try the drain-side path first. tryCompile returns null for any non-eligible
        // predicate; null falls through to the existing FilteredRowReader path below.
        ColumnBatchMatcher[] columnBatchMatchers = null;
        if (filter != null) {
            columnBatchMatchers = BatchFilterCompiler.tryCompile(filter, schema, projectedSchema::toProjectedIndex);
        }
        boolean drainSide = columnBatchMatchers != null;
        final int wordsLen = (batchSize + 63) >>> 6;

        FlatColumnWorker[] workers = new FlatColumnWorker[projectedColumnCount];
        @SuppressWarnings("unchecked")
        BatchExchange<BatchExchange.Batch>[] buffers = new BatchExchange[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            PageSource pageSource = new PageSource(rowGroupIterator, i);

            PhysicalType physType = columnSchema.type();
            // Allocate matches[] only when this column actually has a filter installed.
            // Other columns leave Batch.matches null (sentinel = all-ones in intersect).
            final boolean allocateMatches =
                    drainSide && i < columnBatchMatchers.length && columnBatchMatchers[i] != null;
            BatchExchange<BatchExchange.Batch> buffer = BatchExchange.recycling(
                    columnSchema.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(physType, batchSize);
                        if (allocateMatches) {
                            b.matches = new long[wordsLen];
                        }
                        return b;
                    });
            ColumnBatchMatcher columnFilter = allocateMatches ? columnBatchMatchers[i] : null;
            FlatColumnWorker worker = new FlatColumnWorker(
                    pageSource, buffer, columnSchema, batchSize,
                    context.decompressorFactory(), context.executor(), maxRows,
                    columnFilter);

            buffers[i] = buffer;
            workers[i] = worker;
            worker.start();
        }

        int[] filteredColumns = null;
        if (drainSide) {
            int len = columnBatchMatchers.length;
            int[] tmp = new int[len];
            int idx = 0;

            for (int i = 0; i < len; i++) {
                if (columnBatchMatchers[i] != null) {
                    tmp[idx++] = i;
                }
            }

            filteredColumns = java.util.Arrays.copyOf(tmp, idx);
        }
        if (filteredColumns == null || filteredColumns.length == 0) {
            drainSide = false;
        }

        FlatRowReader reader = new FlatRowReader(buffers, workers, schema, projectedSchema,
                drainSide, wordsLen, filteredColumns);
        reader.initialize();

        if (drainSide) {
            // Drain-side path filters per-batch on the worker threads and iterates via
            // nextSetBit; no consumer-side wrapper.
            return reader;
        }
        if (filter != null) {
            // Indexed compile path: for flat schemas, every leaf column is also
            // a top-level field, and the reader's `getInt(int)` etc. take a
            // projected leaf-column index. Map directly through the projection.
            RowMatcher matcher = RecordFilterCompiler.compile(filter, schema, projectedSchema::toProjectedIndex);
            return new FilteredRowReader(reader, matcher);
        }
        return reader;
    }


    // ==================== Iteration ====================

    @Override
    public boolean hasNext() {
        if (exhausted) {
            return false;
        }
        if (drainSide) {
            if (pendingRowIndex >= 0) {
                return true;
            }
            // Fast path: still inside a known run of consecutive 1-bits — skip
            // nextSetBit and just advance. This is the match-all/dense-batch win.
            if (rowIndex + 1 < runEndExclusive) {
                pendingRowIndex = rowIndex + 1;
                return true;
            }
            while (true) {
                int next = nextSetBit(combinedWords, rowIndex + 1, batchSize);
                if (next >= 0) {
                    pendingRowIndex = next;
                    runEndExclusive = scanRunEnd(combinedWords, next, batchSize);
                    return true;
                }
                if (!loadNextBatch()) {
                    return false;
                }
            }
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        return loadNextBatch();
    }

    @Override
    public void next() {
        if (drainSide) {
            if (pendingRowIndex < 0) {
                throw new java.util.NoSuchElementException("No matching row available. Call hasNext() first.");
            }
            rowIndex = pendingRowIndex;
            pendingRowIndex = -1;
        }
        else {
            rowIndex++;
        }
    }

    /// Finds the **exclusive** end of the run of consecutive 1-bits in `words`
    /// starting at `from`, bounded above by `limit`. Returns the index of the
    /// first 0-bit at or after `from`, or `limit` if every bit in `[from, limit)`
    /// is set.
    ///
    /// Caller must have already established that bit `from` is set; this method
    /// is used by [#hasNext] to amortize per-row `nextSetBit` calls when whole
    /// batches (or large stretches) match
    private static int scanRunEnd(long[] words, int from, int limit) {
        int wordIdx = from >>> 6;
        int endWord = (limit - 1) >>> 6;
        int bitInWord = from & 63;
        // Force the bits below `from` to 1 so they don't show up as the "next zero" — they're irrelevant.
        long lowMask = ~(~0L << bitInWord);
        long word = words[wordIdx] | lowMask;
        long zeros = ~word;

        if (zeros != 0L) {
            int bit = (wordIdx << 6) + Long.numberOfTrailingZeros(zeros);
            return Math.min(bit, limit);
        }

        while (++wordIdx <= endWord) {
            word = words[wordIdx];
            if (word != ~0L) {
                int bit = (wordIdx << 6) + Long.numberOfTrailingZeros(~word);
                return Math.min(bit, limit);
            }
        }
        return limit;
    }

    /// Finds the next set bit in `words` at or above `from`, bounded above by `limit` (exclusive).
    /// Returns `-1` if no such bit exists. Used by the drain-side iteration path so
    /// `hasNext()`/`next()` stay monomorphic without a wrapping reader.
    private static int nextSetBit(long[] words, int from, int limit) {
        if (from >= limit) return -1;

        // Word range that could contain bits in [from, limit)
        int startWord = from >>> 6;
        int endWord = (limit - 1) >>> 6;
        int wordIdx = startWord;

        // Mask first word to ignore bits before `from`
        long word = words[wordIdx] & (~0L << (from & 63));

        while (true) {
            if (word != 0L) {
                // Convert (word index + bit position) to get global bit index (row index)
                int bit = (wordIdx << 6) + Long.numberOfTrailingZeros(word);
                return bit < limit ? bit : -1;
            }

            if (++wordIdx > endWord) {
                return -1;
            }

            word = words[wordIdx];
        }
    }

    // ==================== Null Check ====================

    @Override
    public boolean isNull(int columnIndex) {
        return flatNulls[columnIndex].get(rowIndex);
    }

    @Override
    public boolean isNull(String name) {
        return isNull(resolveIndex(name));
    }

    // ==================== Primitive Accessors by Index ====================

    @Override
    public int getInt(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((int[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public long getLong(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((long[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public float getFloat(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((float[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public double getDouble(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((double[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
    }

    // ==================== Primitive Accessors by Name ====================

    @Override
    public int getInt(String name) {
        return getInt(resolveAndValidate(name, PhysicalType.INT32));
    }

    @Override
    public long getLong(String name) {
        return getLong(resolveAndValidate(name, PhysicalType.INT64));
    }

    @Override
    public float getFloat(String name) {
        return getFloat(resolveAndValidate(name, PhysicalType.FLOAT));
    }

    @Override
    public double getDouble(String name) {
        return getDouble(resolveAndValidate(name, PhysicalType.DOUBLE));
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(resolveAndValidate(name, PhysicalType.BOOLEAN));
    }

    // ==================== String / Binary ====================

    @Override
    public String getString(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return new String(((byte[][]) flatValueArrays[columnIndex])[rowIndex], StandardCharsets.UTF_8);
    }

    @Override
    public String getString(String name) {
        return getString(resolveIndex(name));
    }

    @Override
    public byte[] getBinary(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public byte[] getBinary(String name) {
        return getBinary(resolveIndex(name));
    }

    // ==================== Logical Type Accessors ====================

    @Override
    public LocalDate getDate(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        int rawValue = ((int[]) flatValueArrays[columnIndex])[rowIndex];
        try {
            return LogicalTypeConverter.convertToDate(rawValue, physicalTypes[columnIndex]);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(resolveIndex(name));
    }

    @Override
    public LocalTime getTime(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        Object rawValue;
        if (col.type() == PhysicalType.INT32) {
            rawValue = ((int[]) flatValueArrays[columnIndex])[rowIndex];
        }
        else {
            rawValue = ((long[]) flatValueArrays[columnIndex])[rowIndex];
        }
        try {
            return LogicalTypeConverter.convertToTime(rawValue, col.type(),
                    (LogicalType.TimeType) col.logicalType());
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(resolveIndex(name));
    }

    @Override
    public Instant getTimestamp(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        try {
            if (col.type() == PhysicalType.INT96) {
                byte[] rawValue = ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
                return LogicalTypeConverter.int96ToInstant(rawValue);
            }
            long rawValue = ((long[]) flatValueArrays[columnIndex])[rowIndex];
            return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(),
                    (LogicalType.TimestampType) col.logicalType());
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(resolveIndex(name));
    }

    @Override
    public BigDecimal getDecimal(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        Object rawValue = switch (col.type()) {
            case INT32 -> ((int[]) flatValueArrays[columnIndex])[rowIndex];
            case INT64 -> ((long[]) flatValueArrays[columnIndex])[rowIndex];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
            default -> throw new IllegalArgumentException(prefix()
                    + "Unexpected physical type for DECIMAL: " + col.type());
        };
        try {
            return LogicalTypeConverter.convertToDecimal(rawValue, col.type(),
                    (LogicalType.DecimalType) col.logicalType());
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(resolveIndex(name));
    }

    @Override
    public UUID getUuid(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        try {
            return LogicalTypeConverter.convertToUuid(
                    ((byte[][]) flatValueArrays[columnIndex])[rowIndex],
                    physicalTypes[columnIndex]);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(resolveIndex(name));
    }

    @Override
    public PqInterval getInterval(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        try {
            return LogicalTypeConverter.convertToInterval(
                    ((byte[][]) flatValueArrays[columnIndex])[rowIndex],
                    physicalTypes[columnIndex]);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public PqInterval getInterval(String name) {
        return getInterval(resolveIndex(name));
    }

    // ==================== Generic Value ====================

    @Override
    public Object getValue(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return switch (physicalTypes[columnIndex]) {
            case INT32 -> ((int[]) flatValueArrays[columnIndex])[rowIndex];
            case INT64 -> ((long[]) flatValueArrays[columnIndex])[rowIndex];
            case FLOAT -> ((float[]) flatValueArrays[columnIndex])[rowIndex];
            case DOUBLE -> ((double[]) flatValueArrays[columnIndex])[rowIndex];
            case BOOLEAN -> ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
        };
    }

    @Override
    public Object getValue(String name) {
        return getValue(resolveIndex(name));
    }

    // ==================== Nested (not supported for flat) ====================

    @Override public PqStruct getStruct(String name) { throw nestedUnsupported(); }
    @Override public PqStruct getStruct(int i) { throw nestedUnsupported(); }
    @Override public PqIntList getListOfInts(String name) { throw nestedUnsupported(); }
    @Override public PqIntList getListOfInts(int i) { throw nestedUnsupported(); }
    @Override public PqLongList getListOfLongs(String name) { throw nestedUnsupported(); }
    @Override public PqLongList getListOfLongs(int i) { throw nestedUnsupported(); }
    @Override public PqDoubleList getListOfDoubles(String name) { throw nestedUnsupported(); }
    @Override public PqDoubleList getListOfDoubles(int i) { throw nestedUnsupported(); }
    @Override public PqList getList(String name) { throw nestedUnsupported(); }
    @Override public PqList getList(int i) { throw nestedUnsupported(); }
    @Override public PqMap getMap(String name) { throw nestedUnsupported(); }
    @Override public PqMap getMap(int i) { throw nestedUnsupported(); }
    @Override public PqVariant getVariant(String name) { throw nestedUnsupported(); }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return columnCount;
    }

    @Override
    public String getFieldName(int index) {
        int originalIndex = projectedSchema.toOriginalIndex(index);
        return fileSchema.getColumn(originalIndex).name();
    }

    // ==================== Batch Loading ====================

    private boolean loadNextBatch() {
        if (exhausted) {
            return false;
        }
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].recycle(previousBatches[i]);
                previousBatches[i] = null;
            }
            BatchExchange.Batch batch;
            try {
                batch = exchanges[i].poll();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (batch == null || batch.recordCount == 0) {
                // Check for pipeline errors before returning exhausted —
                // the pipeline may have errored after publishing partial results.
                for (int j = 0; j < columnCount; j++) {
                    exchanges[j].checkError();
                }
                // If earlier columns returned data but this one is empty,
                // the file is corrupt (column count mismatch).
                if (i > 0) {
                    throw new IllegalStateException(prefix()
                            + "Column count mismatch: column " + i + " produced no data"
                            + " while earlier columns had " + batchSize + " records");
                }
                exhausted = true;
                return false;
            }
            flatValueArrays[i] = batch.values;
            flatNulls[i] = batch.nulls != null ? batch.nulls : NO_NULLS;
            previousBatches[i] = batch;
            if (i == 0) {
                batchSize = batch.recordCount;
                currentFileName = batch.fileName;
            }
        }
        rowIndex = -1;
        if (drainSide) {
            intersectMatches();
            // pendingRowIndex is already -1 here: hasNext() only calls loadNextBatch
            // after nextSetBit returns -1, which happens only when pendingRowIndex < 0;
            // next() clears it before any further hasNext(); initialize() runs with the
            // field's default -1.
            runEndExclusive = 0;
        }
        return true;
    }

    /// Intersects per-column [BatchExchange.Batch.matches] arrays into [combinedWords]
    /// for the rows currently held by this reader.
    ///
    /// Iterates only the columns recorded in [#filteredColumns] (built once at
    /// construction time), so there is no per-batch null check on `Batch.matches`.
    /// Only the words covering `[0, batchSize)` are touched — bits past
    /// `batchSize` are not read by the consumer (`nextSetBit` is bounded by
    /// `batchSize`), so leaving them stale is safe and avoids the trailing
    /// zero-fill the previous shape paid every batch.
    ///
    /// **Single-column fast path**: when only one column has a matcher there is
    /// nothing to AND-merge, so [combinedWords] is aliased directly to the batch's
    /// own `matches` array — no copy, no owned buffer. The alias is reseated each
    /// batch; the array stays valid until the batch is recycled in the next
    /// [#loadNextBatch].
    private void intersectMatches() {
        BatchExchange.Batch[] batches = previousBatches;

        if (filteredColumns.length == 1) {
            combinedWords = batches[filteredColumns[0]].matches;
            return;
        }

        int activeWords = (batchSize + 63) >>> 6;
        long[] combined = combinedWords;

        // First column: bulk copy the words covering the active range.
        long[] first = batches[filteredColumns[0]].matches;
        System.arraycopy(first, 0, combined, 0, activeWords);

        // Remaining columns: word-wise AND-merge. Track an OR across the merged
        // words so we can bail out the moment a highly selective column drives
        // the intersection to all-zero — skipping the remaining columns'
        // AND-passes when the batch is already known to produce no rows.
        for (int col = 1; col < filteredColumns.length; col++) {
            long[] matches = batches[filteredColumns[col]].matches;
            long anyBit = 0L;

            for (int wIndex = 0; wIndex < activeWords; wIndex++) {
                long merged = combined[wIndex] & matches[wIndex];
                combined[wIndex] = merged;
                anyBit |= merged;
            }

            if (anyBit == 0L) {
                return;
            }
        }
    }

    // ==================== Close ====================

    @Override
    public void close() {
        if (columnWorkers != null) {
            for (FlatColumnWorker worker : columnWorkers) {
                worker.close();
            }
        }
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].recycle(previousBatches[i]);
                previousBatches[i] = null;
            }
            exchanges[i].drainReady();
        }
    }

    // ==================== Internal ====================

    private String prefix() {
        return ExceptionContext.filePrefix(currentFileName);
    }

    private int resolveIndex(String name) {
        int index = nameToIndex.get(name);
        if (index < 0) {
            throw new IllegalArgumentException(prefix() + "Column not in projection: " + name);
        }
        return index;
    }

    private int resolveAndValidate(String name, PhysicalType expectedType) {
        int index = resolveIndex(name);
        if (physicalTypes[index] != expectedType) {
            throw new IllegalArgumentException(prefix()
                    + "Field '" + name + "' has physical type " + physicalTypes[index]
                    + ", expected " + expectedType);
        }
        return index;
    }

    private void throwNull(int columnIndex) {
        String name = getFieldName(columnIndex);
        throw new NullPointerException(prefix() + "Column '" + name + "' is null at row " + rowIndex);
    }

    private static UnsupportedOperationException nestedUnsupported() {
        return new UnsupportedOperationException("Nested type access not supported for flat schemas");
    }
}
