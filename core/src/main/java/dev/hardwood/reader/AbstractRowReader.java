/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.RecordFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.BatchDataView;
import dev.hardwood.internal.reader.FlatColumnData;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.jfr.RecordFilterEvent;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Base class for RowReader implementations providing iteration control and accessor methods.
/// Subclasses must implement [#initialize()], [#loadNextBatch()], and [#close()].
abstract class AbstractRowReader implements RowReader {

    private static final System.Logger LOG = System.getLogger(AbstractRowReader.class.getName());

    protected BatchDataView dataView;
    protected ResolvedPredicate filterPredicate;
    protected ProjectedSchema projectedSchemaRef;

    // Iteration state shared by all row readers
    protected int rowIndex = -1;
    protected int batchSize = 0;
    protected boolean exhausted = false;
    protected volatile boolean closed = false;
    protected boolean initialized = false;

    // Row limit: 0 = unlimited
    protected long maxRows;
    private long emittedRows;

    /// File name for exception context. Set by subclass constructors.
    protected String fileName;

    // Cached flat arrays for direct access (bypasses dataView virtual dispatch)
    private Object[] flatValueArrays;
    private BitSet[] flatNulls;
    private boolean flatFastPath;
    // Empty BitSet sentinel for required (non-nullable) columns, avoids null checks in accessors
    private static final BitSet NO_NULLS = new BitSet(0);
    // Cached name-to-projected-index mapping for named fast path (built once)
    private StringToIntMap nameCache;

    // Maps schema column index to projected array index for record-level filtering (built once)
    private int[] columnMapping;

    // Whether record-level filtering is active (computed once per batch in cacheFlatBatch)
    private boolean recordFilterActive;
    private boolean recordFilterWarningEmitted;

    /// Computes a batch size that keeps all column arrays for one batch within the L2 cache.
    ///
    /// Each batch allocates one primitive array per projected column. The total memory for a
    /// batch is approximately `batchSize * sum(bytesPerColumn)`. This method sizes the batch
    /// so that total stays under the target (6 MB), clamped to [`16 384`, `524 288`]
    /// rows.
    ///
    /// For example, 3 projected DOUBLE columns (8 bytes each = 24 bytes/row) yields
    /// `6 MB / 24 = 262 144` rows per batch.
    static int computeOptimalBatchSize(ProjectedSchema projectedSchema) {
        // Initally target 6 MB (fits comfortably in L2 cache)
        long targetBytes = 6L * 1024 * 1024;
        int minBatch = 16384;
        int maxBatch = 524288;

        int bytesPerRow = 0;
        for (int i = 0; i < projectedSchema.getProjectedColumnCount(); i++) {
            bytesPerRow += columnByteWidth(projectedSchema.getProjectedColumn(i));
        }

        if (bytesPerRow == 0) {
            bytesPerRow = 8;
        }

        int batchSize = (int) (targetBytes / bytesPerRow);
        return Math.max(minBatch, Math.min(maxBatch, batchSize));
    }

    /// Returns the estimated byte width of a single value for the given column's physical type.
    /// Variable-length types use a 16-byte estimate (pointer + average payload).
    private static int columnByteWidth(ColumnSchema col) {
        return switch (col.type()) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case BOOLEAN -> 1;
            case INT96 -> 12;
            case BYTE_ARRAY -> 16;
            case FIXED_LEN_BYTE_ARRAY -> col.typeLength() != null ? col.typeLength() : 16;
        };
    }

    /// Ensures the reader is initialized. Called by metadata methods that may be
    /// invoked before iteration starts.
    protected abstract void initialize();

    /// Loads the next batch of data.
    /// @return true if a batch was loaded, false if no more data
    protected abstract boolean loadNextBatch();

    /// Populates cached flat arrays from the current batch data for direct access.
    /// This eliminates virtual dispatch through BatchDataView for primitive accessors.
    private void cacheFlatBatch() {
        FlatColumnData[] flatColumnData = dataView.getFlatColumnData();
        if (flatColumnData == null) {
            flatFastPath = false;
            return;
        }
        flatFastPath = true;
        int columns = flatColumnData.length;
        if (flatValueArrays == null || flatValueArrays.length != columns) {
            flatValueArrays = new Object[columns];
            flatNulls = new BitSet[columns];
        }
        for (int i = 0; i < columns; i++) {
            BitSet nulls = flatColumnData[i].nulls();
            flatNulls[i] = nulls != null ? nulls : NO_NULLS;
            flatValueArrays[i] = extractValueArray(flatColumnData[i]);
        }
        // Build name cache once for named fast path
        if (nameCache == null) {
            int fieldCount = dataView.getFieldCount();
            nameCache = new StringToIntMap(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                nameCache.put(dataView.getFieldName(i), i);
            }
        }

        // Build column mapping once for record-level filtering
        if (columnMapping == null && filterPredicate != null) {
            columnMapping = buildColumnMapping();
        }

        recordFilterActive = filterPredicate != null && flatFastPath && nameCache != null;
        if (filterPredicate != null && !recordFilterActive && !recordFilterWarningEmitted) {
            recordFilterWarningEmitted = true;
            LOG.log(System.Logger.Level.WARNING,
                    "Record-level filtering is not active because the schema contains nested columns (structs, lists, or maps). " +
                    "Row-group and page-level filtering still apply, but non-matching rows within surviving pages will not be filtered out.");
        }
    }

    private static Object extractValueArray(FlatColumnData flatColumnData) {
        return switch (flatColumnData) {
            case FlatColumnData.LongColumn lc -> lc.values();
            case FlatColumnData.DoubleColumn dc -> dc.values();
            case FlatColumnData.IntColumn ic -> ic.values();
            case FlatColumnData.FloatColumn fc -> fc.values();
            case FlatColumnData.BooleanColumn bc -> bc.values();
            case FlatColumnData.ByteArrayColumn bac -> bac.values();
        };
    }

    // ==================== Iteration Control ====================

    @Override
    public boolean hasNext() {
        try {
            return hasNextInternal();
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    private boolean hasNextInternal() {
        if (closed || exhausted) {
            return false;
        }
        if (maxRows > 0 && emittedRows >= maxRows) {
            exhausted = true;
            return false;
        }
        if (!initialized) {
            initialize();
            if (!exhausted) {
                cacheFlatBatch();
            }
            if (exhausted) {
                return false;
            }
            return recordFilterActive ? hasNextMatch() : rowIndex + 1 < batchSize;
        }
        if (rowIndex + 1 < batchSize) {
            return recordFilterActive ? hasNextMatch() : true;
        }
        boolean loaded = loadNextBatch();
        if (loaded) {
            cacheFlatBatch();
        }
        return loaded && (recordFilterActive ? hasNextMatch() : true);
    }

    @Override
    public void next() {
        try {
            nextInternal();
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    private void nextInternal() {
        if (!initialized) {
            initialize();
            cacheFlatBatch();
        }
        if (pendingMatchRow >= 0) {
            // hasNext() already found the next matching row
            rowIndex = pendingMatchRow;
            pendingMatchRow = -1;
        }
        else if (recordFilterActive) {
            // next() called without hasNext() — scan for next match
            hasNextMatch();
            rowIndex = pendingMatchRow;
            pendingMatchRow = -1;
        }
        else {
            rowIndex++;
        }
        dataView.setRowIndex(rowIndex);
        emittedRows++;
    }

    /// Row index of the next matching row, found by `hasNextMatch()` and consumed by `next()`.
    /// A value of -1 means no pending match (next() must scan or advance normally).
    private int pendingMatchRow = -1;

    /// Pre-computed set of matching rows for the current batch. Computed once per batch
    /// by `RecordFilterEvaluator.matchBatch()` and queried via `nextSetBit()` for each
    /// `hasNextMatch()` call. Reset to null on batch transitions.
    private BitSet matchingRowsInBatch;

    // Record-level filter counters for JFR reporting
    private long totalRecords;
    private long recordsKept;

    /// Scans forward from `rowIndex + 1` to find the next row matching the filter.
    /// Loads new batches as needed. Returns true if a match is found.
    /// Must only be called when `recordFilterActive` is true.
    private boolean hasNextMatch() {
        while (true) {
            // Compute match mask for current batch if not yet done
            if (matchingRowsInBatch == null) {
                matchingRowsInBatch = RecordFilterEvaluator.matchBatch(filterPredicate, batchSize,
                        flatValueArrays, flatNulls, columnMapping);
                totalRecords += batchSize;
                recordsKept += matchingRowsInBatch.cardinality();
            }

            // Find the next matching row after current position
            int nextMatchingRow = matchingRowsInBatch.nextSetBit(rowIndex + 1);
            if (nextMatchingRow >= 0 && nextMatchingRow < batchSize) {
                pendingMatchRow = nextMatchingRow;
                return true;
            }

            // Current batch exhausted — load next
            matchingRowsInBatch = null;
            if (!loadNextBatch()) {
                exhausted = true;
                emitRecordFilterEvent();
                return false;
            }
            cacheFlatBatch();
            rowIndex = -1;
        }
    }

    /// Emits a JFR event summarizing record-level filtering results.
    private void emitRecordFilterEvent() {
        if (totalRecords > 0) {
            RecordFilterEvent event = new RecordFilterEvent();
            event.totalRecords = totalRecords;
            event.recordsKept = recordsKept;
            event.recordsSkipped = totalRecords - recordsKept;
            event.commit();
        }
    }

    /// Builds a mapping from schema column index to projected array index.
    /// Uses [ProjectedSchema] to invert the projected -> original mapping.
    /// Entries for non-projected columns are set to -1.
    private int[] buildColumnMapping() {
        if (projectedSchemaRef == null) {
            return new int[0];
        }
        int projectedCount = projectedSchemaRef.getProjectedColumnCount();
        // Find the max original index to size the array
        int maxOriginalIndex = 0;
        for (int i = 0; i < projectedCount; i++) {
            maxOriginalIndex = Math.max(maxOriginalIndex, projectedSchemaRef.toOriginalIndex(i));
        }
        int[] mapping = new int[maxOriginalIndex + 1];
        java.util.Arrays.fill(mapping, -1);
        for (int projectedIndex = 0; projectedIndex < projectedCount; projectedIndex++) {
            int originalIndex = projectedSchemaRef.toOriginalIndex(projectedIndex);
            mapping[originalIndex] = projectedIndex;
        }
        return mapping;
    }

    /// Returns the current file name for exception context. Subclasses may override
    /// to provide a dynamic file name (e.g. for multi-file readers).
    protected String getCurrentFileName() {
        return fileName;
    }

    /// Enriches a runtime exception with the current file name. Delegates to
    /// [ExceptionContext.addFileContext] which preserves the original exception
    /// type and cause chain.
    ///
    /// Every public method in this class wraps its body in a try-catch that calls
    /// this method. The pattern has zero cost on the happy path — the JIT compiles
    /// the try block normally and generates the catch as a cold side-exit.
    private RuntimeException wrapWithFileContext(RuntimeException e) {
        return ExceptionContext.addFileContext(getCurrentFileName(), e);
    }

    private void throwNullColumn(String name) {
        throw new NullPointerException("Column '" + name + "' is null at row " + rowIndex);
    }

    private void throwNullColumn(int columnIndex) {
        throw new NullPointerException("Column '" + dataView.getFieldName(columnIndex) + "' is null at row " + rowIndex);
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        try {
            if (flatFastPath) {
                int idx = nameCache.get(name);
                if (idx >= 0 && flatValueArrays[idx] instanceof int[]) {
                    if (flatNulls[idx].get(rowIndex)) {
                        throwNullColumn(name);
                    }
                    return ((int[]) flatValueArrays[idx])[rowIndex];
                }
            }
            return dataView.getInt(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public int getInt(int columnIndex) {
        try {
            if (flatFastPath) {
                if (flatNulls[columnIndex].get(rowIndex)) {
                    throwNullColumn(columnIndex);
                }
                return ((int[]) flatValueArrays[columnIndex])[rowIndex];
            }
            return dataView.getInt(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public long getLong(String name) {
        try {
            if (flatFastPath) {
                int idx = nameCache.get(name);
                if (idx >= 0 && flatValueArrays[idx] instanceof long[]) {
                    if (flatNulls[idx].get(rowIndex)) {
                        throwNullColumn(name);
                    }
                    return ((long[]) flatValueArrays[idx])[rowIndex];
                }
            }
            return dataView.getLong(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public long getLong(int columnIndex) {
        try {
            if (flatFastPath) {
                if (flatNulls[columnIndex].get(rowIndex)) {
                    throwNullColumn(columnIndex);
                }
                return ((long[]) flatValueArrays[columnIndex])[rowIndex];
            }
            return dataView.getLong(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public float getFloat(String name) {
        try {
            if (flatFastPath) {
                int idx = nameCache.get(name);
                if (idx >= 0 && flatValueArrays[idx] instanceof float[]) {
                    if (flatNulls[idx].get(rowIndex)) {
                        throwNullColumn(name);
                    }
                    return ((float[]) flatValueArrays[idx])[rowIndex];
                }
            }
            return dataView.getFloat(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public float getFloat(int columnIndex) {
        try {
            if (flatFastPath) {
                if (flatNulls[columnIndex].get(rowIndex)) {
                    throwNullColumn(columnIndex);
                }
                return ((float[]) flatValueArrays[columnIndex])[rowIndex];
            }
            return dataView.getFloat(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public double getDouble(String name) {
        try {
            if (flatFastPath) {
                int idx = nameCache.get(name);
                if (idx >= 0 && flatValueArrays[idx] instanceof double[]) {
                    if (flatNulls[idx].get(rowIndex)) {
                        throwNullColumn(name);
                    }
                    return ((double[]) flatValueArrays[idx])[rowIndex];
                }
            }
            return dataView.getDouble(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public double getDouble(int columnIndex) {
        try {
            if (flatFastPath) {
                if (flatNulls[columnIndex].get(rowIndex)) {
                    throwNullColumn(columnIndex);
                }
                return ((double[]) flatValueArrays[columnIndex])[rowIndex];
            }
            return dataView.getDouble(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public boolean getBoolean(String name) {
        try {
            if (flatFastPath) {
                int idx = nameCache.get(name);
                if (idx >= 0 && flatValueArrays[idx] instanceof boolean[]) {
                    if (flatNulls[idx].get(rowIndex)) {
                        throwNullColumn(name);
                    }
                    return ((boolean[]) flatValueArrays[idx])[rowIndex];
                }
            }
            return dataView.getBoolean(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        try {
            if (flatFastPath) {
                if (flatNulls[columnIndex].get(rowIndex)) {
                    throwNullColumn(columnIndex);
                }
                return ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
            }
            return dataView.getBoolean(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        try {
            return dataView.getString(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public String getString(int columnIndex) {
        try {
            return dataView.getString(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public byte[] getBinary(String name) {
        try {
            return dataView.getBinary(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public byte[] getBinary(int columnIndex) {
        try {
            return dataView.getBinary(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public LocalDate getDate(String name) {
        try {
            return dataView.getDate(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        try {
            return dataView.getDate(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public LocalTime getTime(String name) {
        try {
            return dataView.getTime(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public LocalTime getTime(int columnIndex) {
        try {
            return dataView.getTime(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public Instant getTimestamp(String name) {
        try {
            return dataView.getTimestamp(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public Instant getTimestamp(int columnIndex) {
        try {
            return dataView.getTimestamp(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public BigDecimal getDecimal(String name) {
        try {
            return dataView.getDecimal(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public BigDecimal getDecimal(int columnIndex) {
        try {
            return dataView.getDecimal(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public UUID getUuid(String name) {
        try {
            return dataView.getUuid(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public UUID getUuid(int columnIndex) {
        try {
            return dataView.getUuid(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Nested Type Accessors (by name) ====================

    @Override
    public PqStruct getStruct(String name) {
        try {
            return dataView.getStruct(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqIntList getListOfInts(String name) {
        try {
            return dataView.getListOfInts(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        try {
            return dataView.getListOfLongs(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        try {
            return dataView.getListOfDoubles(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqList getList(String name) {
        try {
            return dataView.getList(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqMap getMap(String name) {
        try {
            return dataView.getMap(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Nested Type Accessors (by index) ====================

    @Override
    public PqStruct getStruct(int columnIndex) {
        try {
            return dataView.getStruct(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqIntList getListOfInts(int columnIndex) {
        try {
            return dataView.getListOfInts(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqLongList getListOfLongs(int columnIndex) {
        try {
            return dataView.getListOfLongs(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqDoubleList getListOfDoubles(int columnIndex) {
        try {
            return dataView.getListOfDoubles(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqList getList(int columnIndex) {
        try {
            return dataView.getList(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public PqMap getMap(int columnIndex) {
        try {
            return dataView.getMap(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        try {
            return dataView.getValue(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public Object getValue(int columnIndex) {
        try {
            return dataView.getValue(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        try {
            if (flatFastPath) {
                int idx = nameCache.get(name);
                if (idx >= 0) {
                    return flatNulls[idx].get(rowIndex);
                }
            }
            return dataView.isNull(name);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public boolean isNull(int columnIndex) {
        try {
            if (flatFastPath) {
                return flatNulls[columnIndex].get(rowIndex);
            }
            return dataView.isNull(columnIndex);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public int getFieldCount() {
        try {
            initialize();
            return dataView.getFieldCount();
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    @Override
    public String getFieldName(int index) {
        try {
            initialize();
            return dataView.getFieldName(index);
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }
}
