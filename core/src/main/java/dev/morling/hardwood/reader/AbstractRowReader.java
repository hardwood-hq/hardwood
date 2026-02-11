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
import java.util.NoSuchElementException;
import java.util.UUID;

import dev.morling.hardwood.internal.reader.BatchDataView;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqStruct;

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

    // ==================== Iteration Control ====================

    @Override
    public boolean hasNext() {
        if (closed || exhausted) {
            return false;
        }
        if (!initialized) {
            initialize();
            // Re-check after initialization since it loads the first batch
            return !exhausted && rowIndex + 1 < batchSize;
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        return loadNextBatch();
    }

    @Override
    public void next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more rows available");
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
        return dataView.getInt(columnIndex);
    }

    @Override
    public long getLong(String name) {
        return dataView.getLong(name);
    }

    @Override
    public long getLong(int columnIndex) {
        return dataView.getLong(columnIndex);
    }

    @Override
    public float getFloat(String name) {
        return dataView.getFloat(name);
    }

    @Override
    public float getFloat(int columnIndex) {
        return dataView.getFloat(columnIndex);
    }

    @Override
    public double getDouble(String name) {
        return dataView.getDouble(name);
    }

    @Override
    public double getDouble(int columnIndex) {
        return dataView.getDouble(columnIndex);
    }

    @Override
    public boolean getBoolean(String name) {
        return dataView.getBoolean(name);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
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
