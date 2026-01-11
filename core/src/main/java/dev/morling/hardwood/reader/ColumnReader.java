/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.internal.reader.ColumnBatch;
import dev.morling.hardwood.internal.reader.PageReader;
import dev.morling.hardwood.internal.reader.RawColumnBatch;
import dev.morling.hardwood.internal.reader.SimpleColumnBatch;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reader for a column chunk.
 */
public class ColumnReader {

    private final ColumnSchema column;
    private final ColumnMetaData columnMetaData;
    private final PageReader pageReader;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;
    private final long totalValues;

    // State for incremental reading
    private PageReader.Page currentPage;
    private int currentPagePosition;
    private long valuesRead;

    // Lookahead buffer (single value)
    private ValueWithLevels lookahead;

    public ColumnReader(RandomAccessFile file, ColumnSchema column, ColumnChunk columnChunk) throws IOException {
        this.column = column;
        this.columnMetaData = columnChunk.metaData();
        this.pageReader = new PageReader(file, columnMetaData, column);
        this.maxDefinitionLevel = column.getMaxDefinitionLevel();
        this.maxRepetitionLevel = column.getMaxRepetitionLevel();
        this.totalValues = columnMetaData.numValues();
    }

    /**
     * Read all values from this column chunk.
     * Null values are represented as null in the returned list.
     */
    public List<Object> readAll() throws IOException {
        List<Object> allValues = new ArrayList<>();

        while (hasNext()) {
            allValues.add(readNext());
        }

        return allValues;
    }

    /**
     * Check if there are more values to read from this column.
     */
    public boolean hasNext() {
        return lookahead != null || valuesRead < totalValues;
    }

    /**
     * Read the next value from this column.
     * For columns with repetition levels (lists), returns a List containing all elements.
     * Returns null for optional fields when the value is not present.
     */
    public Object readNext() throws IOException {
        if (!hasNext()) {
            throw new IllegalStateException("No more values to read");
        }

        // For columns with repetition levels, assemble a list
        if (maxRepetitionLevel > 0) {
            return readList();
        }

        // Simple case - no repetition levels
        return readSingleValue();
    }

    /**
     * Read a single value (no list assembly needed).
     */
    private Object readSingleValue() throws IOException {
        ensurePageLoaded();

        Object value;
        if (maxDefinitionLevel == 0) {
            // Required field - all values present
            value = currentPage.values()[currentPagePosition];
        }
        else {
            // Optional field - check definition level
            if (currentPage.definitionLevels()[currentPagePosition] == maxDefinitionLevel) {
                value = currentPage.values()[currentPagePosition];
            }
            else {
                value = null;
            }
        }

        currentPagePosition++;
        valuesRead++;
        return value;
    }

    /**
     * Read a list by consuming values until the next rep=0.
     * Uses a stack-based algorithm that works for any nesting depth.
     *
     * For list<list<list<T>>> with maxRep=3:
     *   rep=0: new record (start fresh)
     *   rep=1: new list at depth 1
     *   rep=2: new list at depth 2
     *   rep=3: continue list at depth 2 (add element)
     *
     * The repetition level tells us "at which level did we start a new list?"
     * Lower rep = deeper restart (closer to root).
     */
    @SuppressWarnings("unchecked")
    private List<?> readList() throws IOException {
        ValueWithLevels first = nextValue();
        if (first == null || first.defLevel == 0) {
            return null;
        }

        // Stack of lists at each nesting level (index 0 = outermost)
        int depth = maxRepetitionLevel;
        List<Object>[] stack = new List[depth];
        stack[0] = new ArrayList<>();

        // Empty list (def=1 means outermost list exists but has no elements)
        if (first.defLevel == 1) {
            return stack[0];
        }

        // Initialize nested lists for the first value
        for (int i = 1; i < depth; i++) {
            stack[i] = new ArrayList<>();
            stack[i - 1].add(stack[i]);
        }

        // Add first value if present
        if (first.defLevel == maxDefinitionLevel) {
            stack[depth - 1].add(convertValue(first.value));
        }

        // Continue reading while rep > 0 (same record)
        while (peekRepLevel() > 0) {
            ValueWithLevels v = nextValue();

            // Create new lists from v.repLevel to the deepest level
            for (int i = v.repLevel; i < depth; i++) {
                stack[i] = new ArrayList<>();
                stack[i - 1].add(stack[i]);
            }

            // Add value if present
            if (v.defLevel == maxDefinitionLevel) {
                stack[depth - 1].add(convertValue(v.value));
            }
        }

        return stack[0];
    }

    /**
     * Convert a raw value to its logical type representation.
     */
    private Object convertValue(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        return LogicalTypeConverter.convert(rawValue, column.type(), column.logicalType());
    }

    /**
     * Read the next value with its levels. Uses lookahead if available.
     */
    private ValueWithLevels nextValue() throws IOException {
        if (lookahead != null) {
            ValueWithLevels result = lookahead;
            lookahead = null;
            return result;
        }
        return readFromPage();
    }

    /**
     * Peek at the next repetition level without consuming the value.
     * Returns -1 if no more values (which will fail the > 0 check in callers).
     */
    private int peekRepLevel() throws IOException {
        if (lookahead == null) {
            lookahead = readFromPage();
        }
        return lookahead != null ? lookahead.repLevel : -1;
    }

    /**
     * Read a single value with levels from the current page.
     */
    private ValueWithLevels readFromPage() throws IOException {
        ensurePageLoaded();
        if (currentPage == null || currentPagePosition >= currentPage.numValues()) {
            return null;
        }

        int defLevel = maxDefinitionLevel > 0 ? currentPage.definitionLevels()[currentPagePosition] : 0;
        int repLevel = maxRepetitionLevel > 0 && currentPage.repetitionLevels() != null
                ? currentPage.repetitionLevels()[currentPagePosition]
                : 0;
        Object value = currentPage.values()[currentPagePosition];

        currentPagePosition++;
        valuesRead++;
        return new ValueWithLevels(value, defLevel, repLevel);
    }

    /**
     * Ensure a page is loaded.
     */
    private void ensurePageLoaded() throws IOException {
        if (currentPage == null || currentPagePosition >= currentPage.numValues()) {
            currentPage = pageReader.readPage();
            currentPagePosition = 0;
        }
    }

    private record ValueWithLevels(Object value, int defLevel, int repLevel) {
    }

    /**
     * Read a batch of values from this column.
     * Returns up to batchSize values, or fewer if end of column is reached.
     */
    ColumnBatch readBatch(int batchSize) throws IOException {
        return readBatch(batchSize, false);
    }

    /**
     * Read a batch of values from this column.
     *
     * @param batchSize maximum number of records to read
     * @param rawMode if true, returns raw values with levels instead of pre-assembled lists
     *                (used for multi-column list assembly)
     */
    ColumnBatch readBatch(int batchSize, boolean rawMode) throws IOException {
        if (rawMode && maxRepetitionLevel > 0) {
            return readBatchRaw(batchSize);
        }

        Object[] values = new Object[batchSize];
        int count = 0;

        while (count < batchSize && hasNext()) {
            values[count++] = readNext();
        }

        return new SimpleColumnBatch(values, count, column);
    }

    /**
     * Read a batch in raw mode, returning individual values with their levels.
     * Used for list-of-struct assembly where we need to correlate across columns.
     */
    private RawColumnBatch readBatchRaw(int batchSize) throws IOException {
        List<Object> rawValues = new ArrayList<>();
        List<Integer> defLevels = new ArrayList<>();
        List<Integer> repLevels = new ArrayList<>();
        int recordCount = 0;

        while (recordCount < batchSize && hasNext()) {
            // Read all values for this record (until next rep=0)
            ValueWithLevels v = nextValue();
            if (v == null) {
                break;
            }

            rawValues.add(v.value);
            defLevels.add(v.defLevel);
            repLevels.add(v.repLevel);

            // Continue reading while rep > 0 (same record)
            while (peekRepLevel() > 0) {
                v = nextValue();
                rawValues.add(v.value);
                defLevels.add(v.defLevel);
                repLevels.add(v.repLevel);
            }

            recordCount++;
        }

        return new RawColumnBatch(
                rawValues.toArray(),
                defLevels.stream().mapToInt(Integer::intValue).toArray(),
                repLevels.stream().mapToInt(Integer::intValue).toArray(),
                recordCount,
                column);
    }

    public ColumnSchema getColumnSchema() {
        return column;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }
}
