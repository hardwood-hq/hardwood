/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.morling.hardwood.internal.reader.BatchDataView;
import dev.morling.hardwood.internal.reader.FlatColumnData;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqStruct;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Base class for RowReader implementations providing iteration control and accessor methods.
 * Subclasses must implement {@link #initialize()}, {@link #loadNextBatch()}, and {@link #close()}.
 */
abstract class AbstractRowReader implements RowReader {

    protected BatchDataView dataView;

    // Iteration state shared by all row readers
    protected int rowIndex = -1;
    protected int batchSize = 0;
    protected boolean exhausted = false;
    protected volatile boolean closed = false;
    protected boolean initialized = false;

    // Cached flat arrays for direct access (bypasses dataView virtual dispatch)
    private Object[] flatValueArrays;
    private BitSet[] flatNulls;
    private boolean flatFastPath;

    /**
     * Computes a batch size that keeps all column arrays for one batch within the L2 cache.
     *
     * <p>Each batch allocates one primitive array per projected column. The total memory for a
     * batch is approximately {@code batchSize * sum(bytesPerColumn)}. This method sizes the batch
     * so that total stays under the target (6 MB), clamped to [{@code 16 384}, {@code 524 288}]
     * rows.</p>
     *
     * <p>For example, 3 projected DOUBLE columns (8 bytes each = 24 bytes/row) yields
     * {@code 6 MB / 24 = 262 144} rows per batch.</p>
     */
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

    /**
     * Returns the estimated byte width of a single value for the given column's physical type.
     * Variable-length types use a 16-byte estimate (pointer + average payload).
     */
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

    /**
     * Ensures the reader is initialized. Called by metadata methods that may be
     * invoked before iteration starts.
     */
    protected abstract void initialize();

    /**
     * Loads the next batch of data.
     * @return true if a batch was loaded, false if no more data
     */
    protected abstract boolean loadNextBatch();

    /**
     * Populates cached flat arrays from the current batch data for direct access.
     * This eliminates virtual dispatch through BatchDataView for primitive accessors.
     */
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
            flatNulls[i] = flatColumnData[i].nulls();
            flatValueArrays[i] = extractValueArray(flatColumnData[i]);
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
        if (closed || exhausted) {
            return false;
        }
        if (!initialized) {
            initialize();
            if (!exhausted) {
                cacheFlatBatch();
            }
            // Re-check after initialization since it loads the first batch
            return !exhausted && rowIndex + 1 < batchSize;
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        boolean loaded = loadNextBatch();
        if (loaded) {
            cacheFlatBatch();
        }
        return loaded;
    }

    @Override
    public void next() {
        if (!initialized) {
            initialize();
            cacheFlatBatch();
        }
        rowIndex++;
        dataView.setRowIndex(rowIndex);
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        return dataView.getInt(name);
    }

    @Override
    public int getInt(int columnIndex) {
        if (flatFastPath) {
            return ((int[]) flatValueArrays[columnIndex])[rowIndex];
        }
        return dataView.getInt(columnIndex);
    }

    @Override
    public long getLong(String name) {
        return dataView.getLong(name);
    }

    @Override
    public long getLong(int columnIndex) {
        if (flatFastPath) {
            return ((long[]) flatValueArrays[columnIndex])[rowIndex];
        }
        return dataView.getLong(columnIndex);
    }

    @Override
    public float getFloat(String name) {
        return dataView.getFloat(name);
    }

    @Override
    public float getFloat(int columnIndex) {
        if (flatFastPath) {
            return ((float[]) flatValueArrays[columnIndex])[rowIndex];
        }
        return dataView.getFloat(columnIndex);
    }

    @Override
    public double getDouble(String name) {
        return dataView.getDouble(name);
    }

    @Override
    public double getDouble(int columnIndex) {
        if (flatFastPath) {
            return ((double[]) flatValueArrays[columnIndex])[rowIndex];
        }
        return dataView.getDouble(columnIndex);
    }

    @Override
    public boolean getBoolean(String name) {
        return dataView.getBoolean(name);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        if (flatFastPath) {
            return ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
        }
        return dataView.getBoolean(columnIndex);
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        return dataView.getString(name);
    }

    @Override
    public String getString(int columnIndex) {
        return dataView.getString(columnIndex);
    }

    @Override
    public byte[] getBinary(String name) {
        return dataView.getBinary(name);
    }

    @Override
    public byte[] getBinary(int columnIndex) {
        return dataView.getBinary(columnIndex);
    }

    @Override
    public LocalDate getDate(String name) {
        return dataView.getDate(name);
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        return dataView.getDate(columnIndex);
    }

    @Override
    public LocalTime getTime(String name) {
        return dataView.getTime(name);
    }

    @Override
    public LocalTime getTime(int columnIndex) {
        return dataView.getTime(columnIndex);
    }

    @Override
    public Instant getTimestamp(String name) {
        return dataView.getTimestamp(name);
    }

    @Override
    public Instant getTimestamp(int columnIndex) {
        return dataView.getTimestamp(columnIndex);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return dataView.getDecimal(name);
    }

    @Override
    public BigDecimal getDecimal(int columnIndex) {
        return dataView.getDecimal(columnIndex);
    }

    @Override
    public UUID getUuid(String name) {
        return dataView.getUuid(name);
    }

    @Override
    public UUID getUuid(int columnIndex) {
        return dataView.getUuid(columnIndex);
    }

    // ==================== Nested Type Accessors (by name) ====================

    @Override
    public PqStruct getStruct(String name) {
        return dataView.getStruct(name);
    }

    @Override
    public PqIntList getListOfInts(String name) {
        return dataView.getListOfInts(name);
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        return dataView.getListOfLongs(name);
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        return dataView.getListOfDoubles(name);
    }

    @Override
    public PqList getList(String name) {
        return dataView.getList(name);
    }

    @Override
    public PqMap getMap(String name) {
        return dataView.getMap(name);
    }

    // ==================== Nested Type Accessors (by index) ====================

    @Override
    public PqStruct getStruct(int columnIndex) {
        return dataView.getStruct(columnIndex);
    }

    @Override
    public PqIntList getListOfInts(int columnIndex) {
        return dataView.getListOfInts(columnIndex);
    }

    @Override
    public PqLongList getListOfLongs(int columnIndex) {
        return dataView.getListOfLongs(columnIndex);
    }

    @Override
    public PqDoubleList getListOfDoubles(int columnIndex) {
        return dataView.getListOfDoubles(columnIndex);
    }

    @Override
    public PqList getList(int columnIndex) {
        return dataView.getList(columnIndex);
    }

    @Override
    public PqMap getMap(int columnIndex) {
        return dataView.getMap(columnIndex);
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        return dataView.getValue(name);
    }

    @Override
    public Object getValue(int columnIndex) {
        return dataView.getValue(columnIndex);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        return dataView.isNull(name);
    }

    @Override
    public boolean isNull(int columnIndex) {
        if (flatFastPath) {
            BitSet n = flatNulls[columnIndex];
            return n != null && n.get(rowIndex);
        }
        return dataView.isNull(columnIndex);
    }

    @Override
    public int getFieldCount() {
        initialize();
        return dataView.getFieldCount();
    }

    @Override
    public String getFieldName(int index) {
        initialize();
        return dataView.getFieldName(index);
    }
}
