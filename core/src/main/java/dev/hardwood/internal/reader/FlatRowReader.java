/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.UUID;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.CompiledBatchFilter;
import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// High-performance row reader for flat schemas.
///
/// This is a `final` class with all hot-path methods defined directly (not inherited),
/// giving the JIT a single concrete class with monomorphic call sites. Supports all
/// primitive types, logical type conversions (date, time, timestamp, decimal, UUID,
/// string), and both name-based and index-based access.
public final class FlatRowReader implements FileAwareRowReader {

    /// Sentinel for "every leaf in the batch is present" — replaces the
    /// nullable validity reference on the hot path so the per-row check
    /// stays a single word load + mask. Sized for the largest batch
    /// [BatchSizing] will produce, so any in-range row index reads as
    /// present. Set-bit-= -present polarity, matching [BatchExchange.Batch#validity].
    private static final long[] ALL_PRESENT = allPresentSentinel();

    private static long[] allPresentSentinel() {
        long[] words = new long[(BatchSizing.MAX_BATCH + 63) >>> 6];
        Arrays.fill(words, ~0L);
        return words;
    }

    private final BatchExchange<BatchExchange.Batch>[] exchanges;
    private final FlatColumnWorker[] columnWorkers;
    private final int columnCount;

    // Schema info for name lookup and logical type conversion
    private final FileSchema fileSchema;
    private final ProjectedSchema projectedSchema;
    private final StringToIntMap nameToIndex;
    private final PhysicalType[] physicalTypes;
    private final ColumnSchema[] columnSchemas;
    /// Per-column decode strategy for [#getValue(int)], precomputed once from each
    /// column's fixed `(physicalType, logicalType)` so the hot path is a single
    /// table lookup instead of re-deriving the branch on every value.
    private final LeafKind[] kinds;

    // Hot fields — directly owned, no inheritance.
    // `flatValidity[col]` is a packed bitmap (set bit = leaf is present); the
    // per-row check is `(flatValidity[col][row >>> 6] & (1L << row)) != 0L`.
    // When every leaf for this column in the current batch is present, the
    // slot points to [#ALL_PRESENT] so the per-row check needs no null guard.
    private Object[] flatValueArrays;
    private long[][] flatValidity;
    private BatchExchange.Batch[] previousBatches;
    /// Whether statistics proved every row of the batch being served matches the filter,
    /// so [#recordMatcher] need not be run over it.
    private boolean currentRowsAlwaysMatch;
    /// The matcher in force for the batch being served: [#recordMatcher], or `null` when
    /// statistics decided the batch and there is no cap to count matches against. A
    /// proven batch is then an unfiltered batch, served by the plain cursor with no
    /// per-row state to consult — which is the whole of what the proof is worth.
    private RowMatcher activeMatcher;

    /// Record-level predicate for filters the drain side cannot compile, or `null` when
    /// the read has no filter or filters on the drain side. Present or absent purely by
    /// whether the caller asked for a filter — never by what any file turned out to hold.
    private final RowMatcher recordMatcher;
    private int rowIndex = -1;
    private int batchSize = 0;
    private boolean exhausted;
    private boolean closed;

    /// Owns the whole per-batch match merge — the plan, the evaluator and its
    /// scratch, the per-column bitmap slots and the combined buffer. Non-null is
    /// what makes this a drain-side read: [#hasNext] then iterates via
    /// [#nextSetBit] over [#combinedWords] instead of the plain `rowIndex++`
    /// cursor. `null` when the read has no drain-side filter.
    private final BatchMatchMerger matchMerger;
    /// The current batch's survivor bitmap, as [BatchMatchMerger#merge] returned
    /// it when the batch loaded. Cached in the reader so the per-row
    /// [#nextSetBit] and run walk read the array through one field rather than
    /// reaching through the merger. `null` when there is no drain-side filter.
    private long[] combinedWords;
    private int pendingRowIndex = -1;
    /// Cap on the number of *matching* rows yielded (SQL LIMIT over the filtered
    /// relation). [ColumnWorker#UNLIMITED] means no cap. Enforced on both filtering
    /// paths; without a filter the worker already capped scanned == matched rows.
    private final long maxMatchedRows;
    /// Count of matching rows yielded so far.
    private long matchedRowsYielded;
    /// Exclusive upper bound of the current run of consecutive-1 bits in
    /// [#combinedWords] starting at or before `rowIndex + 1`. While
    /// `rowIndex + 1 < runEndExclusive`, [#hasNext] can advance without
    /// invoking [#nextSetBit] — replacing per-row mask + `numberOfTrailingZeros`
    /// with a simple bound check, which is the win for match-all-like batches.
    /// Reset to 0 each time a new batch loads.
    private int runEndExclusive;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;

    /// Per-file record-filter counts for JFR, or `null` when the read has no
    /// filter and nothing evaluates records.
    private final RecordFilterTally tally;

    private FlatRowReader(BatchExchange<BatchExchange.Batch>[] exchanges, FlatColumnWorker[] columnWorkers,
                         FileSchema fileSchema, ProjectedSchema projectedSchema,
                         BatchMatchMerger matchMerger, long maxMatchedRows,
                         RowMatcher recordMatcher, RecordFilterTally tally) {
        this.maxMatchedRows = maxMatchedRows;
        this.recordMatcher = recordMatcher;
        this.tally = tally;
        this.exchanges = exchanges;
        this.columnWorkers = columnWorkers;
        this.columnCount = exchanges.length;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.flatValueArrays = new Object[columnCount];
        this.flatValidity = new long[columnCount][];
        this.previousBatches = new BatchExchange.Batch[columnCount];
        this.matchMerger = matchMerger;

        // Build name-to-index map and cache column metadata
        this.nameToIndex = new StringToIntMap(columnCount);
        this.physicalTypes = new PhysicalType[columnCount];
        this.columnSchemas = new ColumnSchema[columnCount];
        this.kinds = new LeafKind[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema col = fileSchema.getColumn(originalIndex);
            nameToIndex.put(col.name(), i);
            physicalTypes[i] = col.type();
            columnSchemas[i] = col;
            kinds[i] = classifyLeaf(col.type(), col.logicalType());
        }
    }

    /// Fails when the caller has asked a column for a float it does not hold.
    ///
    /// Reached only once the `FLOAT` fast path has been ruled out, so the whole question is
    /// whether the column is the other thing `getFloat` reads. It says what the column
    /// actually is, rather than the width `FLOAT16` would have needed — which is not what
    /// the caller asked about when the column is not annotated `FLOAT16` at all. A column
    /// whose annotation its width cannot carry arrives unannotated, the annotation having
    /// been dropped where the schema was built, so it is simply not a float column.
    private void requireFloatAccess(int columnIndex) {
        LogicalType logicalType = columnSchemas[columnIndex].logicalType();
        if (logicalType instanceof LogicalType.Float16Type) {
            return;
        }
        throw new IllegalArgumentException(prefix() + "Column '"
                + columnSchemas[columnIndex].fieldPath() + "' is "
                + physicalTypes[columnIndex]
                + (logicalType == null ? "" : " annotated " + logicalType)
                + ", which cannot be read as a float");
    }

    /// Decode strategy for [#getValue(int)], selected by a column's physical and
    /// logical type. Matches the original branch order: a `UTF8` / `JSON` leaf is
    /// served interned, an `INT96` leaf is the conventional timestamp, an
    /// unannotated leaf returns its raw boxed value, and everything else converts.
    private enum LeafKind {
        STRING,
        INT96_TIMESTAMP,
        RAW,
        CONVERT
    }

    private static LeafKind classifyLeaf(PhysicalType pt, LogicalType lt) {
        if (ValueConverter.isStringLeaf(pt, lt)) {
            return LeafKind.STRING;
        }
        if (pt == PhysicalType.INT96) {
            // INT96 has no LogicalType but is conventionally a TIMESTAMP.
            return LeafKind.INT96_TIMESTAMP;
        }
        return lt == null ? LeafKind.RAW : LeafKind.CONVERT;
    }

    /// Eagerly loads the first batch. Must be called after construction.
    public void initialize() throws IOException {
        if (!loadNextBatch()) {
            exhausted = true;
        }
    }

    // ==================== Factory ====================

    /// Creates a flat v3 pipeline and returns a [RowReader].
    ///
    /// Wires up `RowGroupIterator → PageSource → ColumnWorker → BatchExchange → FlatRowReader`,
    /// starts all column workers and initializes the reader. A filter is installed on
    /// whichever path can carry it — per batch on the drain side, per record otherwise —
    /// which is decided by the predicate's shape alone, never by what a file holds.
    ///
    /// @param rowGroupIterator pre-configured iterator (file opened, first file set, initialized)
    /// @param schema the file schema
    /// @param projectedSchema the projected column schema
    /// @param context the hardwood context
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited). Without a filter this caps scanned
    ///                rows at the [ColumnWorker] drain. With a filter it caps *matching*
    ///                rows (SQL LIMIT): the drain holds it over the row groups statistics
    ///                prove match in full, and the reader counts matches from the first
    ///                row group they do not.
    /// @return a [FlatRowReader]
    public static RowReader create(RowGroupIterator rowGroupIterator,
                                   FileSchema schema,
                                   ProjectedSchema projectedSchema,
                                   HardwoodContextImpl context,
                                   ResolvedPredicate filter,
                                   long maxRows) throws IOException {
        int batchSize = BatchSizing.computeOptimalBatchSize(projectedSchema);
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();

        // A row-level filter changes what `maxRows` counts: under SQL LIMIT semantics
        // the cap is on *matching* rows, not scanned rows. The workers still take it —
        // they hold it only while statistics prove every row they assemble matches, and
        // drop it at the first row group that is not proven, from where the drain-side
        // reader counts matches instead.
        //
        // Nothing here asks a question about the read as a whole, so nothing here plans
        // beyond the first file: statistics reach both filtering paths per row group,
        // on Batch.filterAlwaysMatches (see #1107).

        // Try the drain-side path first. tryCompile returns null for any non-eligible
        // predicate; null falls through to the record-matcher path below.
        CompiledBatchFilter compiledFilter = null;
        if (filter != null) {
            compiledFilter = BatchFilterCompiler.tryCompile(filter, schema, projectedSchema::toProjectedIndex);
        }
        ColumnBatchMatcher[] columnBatchMatchers = compiledFilter != null ? compiledFilter.columnMatchers() : null;
        final boolean drainSide = compiledFilter != null;
        final int wordsLen = (batchSize + 63) >>> 6;

        FlatColumnWorker[] workers = new FlatColumnWorker[projectedColumnCount];
        @SuppressWarnings("unchecked")
        BatchExchange<BatchExchange.Batch>[] buffers = new BatchExchange[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            PageSource pageSource = new PageSource(rowGroupIterator, i);

            // Allocate matches[] only when this column actually has a filter installed.
            // Other columns leave Batch.matches null (sentinel = all-ones in intersect).
            final boolean allocateMatches =
                    drainSide && i < columnBatchMatchers.length && columnBatchMatchers[i] != null;
            BatchExchange<BatchExchange.Batch> buffer = BatchExchange.recycling(
                    columnSchema.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(columnSchema, batchSize);
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

        // The merger *is* the drain-side path in the reader: present exactly when the
        // predicate compiled to one, absent when the record matcher takes over.
        BatchMatchMerger matchMerger = drainSide
                ? BatchMatchMerger.aliasing(compiledFilter.mergePlan(), projectedColumnCount, wordsLen)
                : null;

        // Whatever the drain side could not compile, the reader evaluates a record at a
        // time. Indexed compile path: for flat schemas every leaf column is also a
        // top-level field, and the reader's `getInt(int)` etc. take a projected
        // leaf-column index, so the projection maps them directly.
        RowMatcher recordMatcher = !drainSide && filter != null
                ? RecordFilterCompiler.compile(filter, schema, projectedSchema::toProjectedIndex)
                : null;
        // Filtering happens in the reader on both paths, so the reader caps matched
        // rows. Without a filter the worker already capped scanned == matched rows.
        long readerMatchLimit = filter != null ? maxRows : ColumnWorker.UNLIMITED;
        // The tally spans both filtered paths: the drain side feeds it whole batches,
        // the record matcher single records, and either way the reader marks the file
        // boundaries as it loads batches.
        RecordFilterTally tally = filter != null ? new RecordFilterTally() : null;
        FlatRowReader reader = new FlatRowReader(buffers, workers, schema, projectedSchema,
                matchMerger, readerMatchLimit, recordMatcher, tally);
        reader.initialize();
        return reader;
    }

    // ==================== Iteration ====================

    @Override
    public boolean hasNext() throws IOException {
        if (exhausted) {
            return false;
        }
        if (matchMerger != null) {
            if (maxMatchedRows != ColumnWorker.UNLIMITED && matchedRowsYielded >= maxMatchedRows) {
                exhausted = true;
                return false;
            }
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
        if (activeMatcher != null) {
            return hasNextMatching();
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        return loadAndDecide();
    }

    /// Loads the next batch and says whether it yields a row. Kept out of [#hasNext] so
    /// that method stays loop-free and small enough to inline into a caller's row loop,
    /// which is worth more than the call this costs once per batch.
    private boolean loadAndDecide() throws IOException {
        if (!loadNextBatch()) {
            return false;
        }
        return activeMatcher == null || hasNextMatching();
    }

    /// Advances to the next record the matcher accepts, evaluating one at a time.
    /// Returns to [#hasNext]'s plain cursor as soon as a batch loads that statistics
    /// decided, so a proven batch never pays for the per-row protocol.
    private boolean hasNextMatching() throws IOException {
        if (maxMatchedRows != ColumnWorker.UNLIMITED && matchedRowsYielded >= maxMatchedRows) {
            exhausted = true;
            return false;
        }
        if (pendingRowIndex >= 0) {
            return true;
        }
        while (true) {
            if (rowIndex + 1 >= batchSize) {
                if (!loadNextBatch()) {
                    return false;
                }
                if (activeMatcher == null) {
                    return true;
                }
            }
            rowIndex++;
            // Statistics already decided this batch's row group in full — reachable only
            // under a cap, which needs every match counted as it goes.
            boolean matched = currentRowsAlwaysMatch || activeMatcher.test(this);
            tally.record(matched);
            if (matched) {
                pendingRowIndex = rowIndex;
                return true;
            }
        }
    }

    @Override
    public void next() throws IOException {
        // Both filtering modes park the row they picked in `pendingRowIndex`; this only
        // commits it. They differ in how `hasNext` finds the row, not in what `next` does.
        if (matchMerger != null || activeMatcher != null) {
            if (pendingRowIndex < 0) {
                throw new NoSuchElementException("No matching row available. Call hasNext() first.");
            }
            rowIndex = pendingRowIndex;
            pendingRowIndex = -1;
            matchedRowsYielded++;
        }
        else {
            // Fail early on an unguarded next() past the batch: without this bound
            // rowIndex would walk into the capacity tail, where an all-present
            // column's ALL_PRESENT sentinel lets the accessor return phantom/stale
            // leaf values instead of throwing. After any hasNext() == true this
            // check never trips (a freshly loaded batch resets rowIndex to -1).
            if (rowIndex + 1 >= batchSize) {
                throw new NoSuchElementException("No row available. Call hasNext() first.");
            }
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
        return (flatValidity[columnIndex][rowIndex >>> 6] & (1L << rowIndex)) == 0L;
    }

    @Override
    public boolean isNull(String name) {
        return isNull(resolveIndex(name));
    }

    // ==================== Primitive Accessors by Index ====================

    @Override
    public int getInt(int columnIndex) {
        if ((flatValidity[columnIndex][rowIndex >>> 6] & (1L << rowIndex)) == 0L) {
            throwNull(columnIndex);
        }
        return ((int[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public long getLong(int columnIndex) {
        if ((flatValidity[columnIndex][rowIndex >>> 6] & (1L << rowIndex)) == 0L) {
            throwNull(columnIndex);
        }
        return ((long[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public float getFloat(int columnIndex) {
        if ((flatValidity[columnIndex][rowIndex >>> 6] & (1L << rowIndex)) == 0L) {
            throwNull(columnIndex);
        }
        if (physicalTypes[columnIndex] == PhysicalType.FLOAT) {
            return ((float[]) flatValueArrays[columnIndex])[rowIndex];
        }
        // FLOAT16 surfaces as FIXED_LEN_BYTE_ARRAY(2) annotated Float16Type; any other
        // column is one the caller has asked for a float it does not hold.
        requireFloatAccess(columnIndex);
        try {
            return LogicalTypeConverter.convertToFloat16(
                    ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex),
                    physicalTypes[columnIndex]);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public double getDouble(int columnIndex) {
        if ((flatValidity[columnIndex][rowIndex >>> 6] & (1L << rowIndex)) == 0L) {
            throwNull(columnIndex);
        }
        return ((double[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        if ((flatValidity[columnIndex][rowIndex >>> 6] & (1L << rowIndex)) == 0L) {
            throwNull(columnIndex);
        }
        return ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
    }

    // ==================== Primitive Accessors by Name ====================

    @Override
    public int getInt(String name) {
        return getInt(resolveIndex(name));
    }

    @Override
    public long getLong(String name) {
        return getLong(resolveIndex(name));
    }

    @Override
    public float getFloat(String name) {
        return getFloat(resolveIndex(name));
    }

    @Override
    public double getDouble(String name) {
        return getDouble(resolveIndex(name));
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(resolveIndex(name));
    }

    // ==================== String / Binary ====================

    @Override
    public String getString(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return ((BinaryBatchValues) flatValueArrays[columnIndex]).stringAt(rowIndex);
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
        return ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex);
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
                byte[] rawValue = ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex);
                return LogicalTypeConverter.int96ToInstant(rawValue);
            }
            TimestampAccessorKind.require(col.name(), col.logicalType(), true);
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
    public LocalDateTime getLocalTimestamp(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        try {
            TimestampAccessorKind.require(col.name(), col.logicalType(), false);
            long rawValue = ((long[]) flatValueArrays[columnIndex])[rowIndex];
            return LogicalTypeConverter.convertToLocalTimestamp(rawValue, col.type(),
                    (LogicalType.TimestampType) col.logicalType());
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(currentFileName, e);
        }
    }

    @Override
    public LocalDateTime getLocalTimestamp(String name) {
        return getLocalTimestamp(resolveIndex(name));
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
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                    ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex);
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
                    ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex),
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
    public String currentFileName() {
        return currentFileName;
    }

    @Override
    public PqInterval getInterval(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        try {
            return LogicalTypeConverter.convertToInterval(
                    ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex),
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
        return switch (kinds[columnIndex]) {
            // Dictionary-encoded UTF8/JSON: return the interned String (one per chunk).
            case STRING -> ((BinaryBatchValues) flatValueArrays[columnIndex]).stringAt(rowIndex);
            case INT96_TIMESTAMP -> LogicalTypeConverter.int96ToInstant((byte[]) rawValueUnchecked(columnIndex));
            case RAW -> rawValueUnchecked(columnIndex);
            case CONVERT -> LogicalTypeConverter.convert(
                    rawValueUnchecked(columnIndex), physicalTypes[columnIndex],
                    columnSchemas[columnIndex].logicalType());
        };
    }

    @Override
    public Object getValue(String name) {
        return getValue(resolveIndex(name));
    }

    @Override
    public Object getRawValue(int columnIndex) {
        return isNull(columnIndex) ? null : rawValueUnchecked(columnIndex);
    }

    /// Reads the raw column value at the current row, assuming the caller has
    /// already established the value is present (`!isNull(columnIndex)`); the
    /// returned value is never `null`.
    private Object rawValueUnchecked(int columnIndex) {
        return switch (physicalTypes[columnIndex]) {
            case INT32 -> ((int[]) flatValueArrays[columnIndex])[rowIndex];
            case INT64 -> ((long[]) flatValueArrays[columnIndex])[rowIndex];
            case FLOAT -> ((float[]) flatValueArrays[columnIndex])[rowIndex];
            case DOUBLE -> ((double[]) flatValueArrays[columnIndex])[rowIndex];
            case BOOLEAN -> ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    ((BinaryBatchValues) flatValueArrays[columnIndex]).byteArrayAt(rowIndex);
        };
    }

    @Override
    public Object getRawValue(String name) {
        return getRawValue(resolveIndex(name));
    }

    // ==================== Nested (not supported for flat) ====================

    @Override public PqStruct getStruct(String name) { throw nestedUnsupported(); }
    @Override public PqStruct getStruct(int i) { throw nestedUnsupported(); }
    @Override public PqList getList(String name) { throw nestedUnsupported(); }
    @Override public PqList getList(int i) { throw nestedUnsupported(); }
    @Override public PqMap getMap(String name) { throw nestedUnsupported(); }
    @Override public PqMap getMap(int i) { throw nestedUnsupported(); }
    @Override public PqVariant getVariant(String name) { throw nestedUnsupported(); }
    @Override public PqVariant getVariant(int i) { throw nestedUnsupported(); }

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

    private boolean loadNextBatch() throws IOException {
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
            flatValidity[i] = batch.validity != null ? batch.validity : ALL_PRESENT;
            previousBatches[i] = batch;
            if (i == 0) {
                batchSize = batch.recordCount;
                currentFileName = batch.fileName;
                // Uniform across columns: the flag comes from the work item, and the
                // workers flush on its transitions, so no batch mixes the two.
                currentRowsAlwaysMatch = batch.filterAlwaysMatches;
                // A batch statistics decided, with no cap to count matches against, needs
                // nothing evaluated and nothing counted per row: hand it to the plain cursor
                // and tally it whole.
                activeMatcher = currentRowsAlwaysMatch && maxMatchedRows == ColumnWorker.UNLIMITED
                        ? null : recordMatcher;
            }
        }
        rowIndex = -1;
        if (matchMerger != null) {
            combinedWords = matchMerger.merge(previousBatches, batchSize);
            // pendingRowIndex is already -1 here: hasNext() only calls loadNextBatch
            // after nextSetBit returns -1, which happens only when pendingRowIndex < 0;
            // next() clears it before any further hasNext(); initialize() runs with the
            // field's default -1.
            runEndExclusive = 0;
        }
        if (tally != null) {
            // Ahead of any record of this batch being counted, so the counts land
            // on the file the batch came from. Batches never straddle files.
            tally.switchFile(currentFileName);
            if (matchMerger != null) {
                // The whole batch was decided on the drain thread — count it here
                // rather than as rows are yielded, so an early exit does not leave
                // the batch reported as all-skipped.
                tally.recordBatch(batchSize, countMatches(combinedWords, batchSize));
            }
            else if (activeMatcher == null) {
                // Statistics decided this batch and there is no cap, so no row of it is
                // evaluated or counted individually: count it whole, for the same reason.
                // A non-drain-side read with a tally always has a record matcher, so a
                // null `activeMatcher` here means the proof, never the absence of a filter.
                tally.recordBatch(batchSize, batchSize);
            }
        }
        return true;
    }

    /// Counts the set bits of `words` below `limit`. The words past `limit` hold
    /// stale bits from an earlier, longer batch (see [BatchMatchMerger#merge]), so the
    /// partial tail word is masked rather than counted whole.
    private static int countMatches(long[] words, int limit) {
        int fullWords = limit >>> 6;
        int count = 0;
        for (int i = 0; i < fullWords; i++) {
            count += Long.bitCount(words[i]);
        }
        int tailBits = limit & 63;
        if (tailBits != 0) {
            count += Long.bitCount(words[fullWords] & (~0L >>> (64 - tailBits)));
        }
        return count;
    }

    // ==================== Close ====================

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (tally != null) {
            tally.close();
        }
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

    private void throwNull(int columnIndex) {
        String name = getFieldName(columnIndex);
        throw new NullPointerException(prefix() + "Column '" + name + "' is null at row " + rowIndex);
    }

    private static UnsupportedOperationException nestedUnsupported() {
        return new UnsupportedOperationException("Nested type access not supported for flat schemas");
    }
}
