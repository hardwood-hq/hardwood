/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.reader.FileMappingEvent;
import dev.hardwood.internal.reader.Page;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/**
 * Reader for a column chunk.
 * <p>
 * Uses the same infrastructure as RowReader: PageScanner for page scanning
 * and PageCursor for async prefetching.
 * </p>
 */
public class ColumnReader implements Closeable {

    private final ColumnSchema column;
    private final ColumnMetaData columnMetaData;
    private final PageCursor pageCursor;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;
    private final long totalValues;
    private final FileChannel channel;

    // State for incremental reading
    private Page currentPage;
    private int currentPagePosition;
    private long valuesRead;

    // Lookahead buffer (single value)
    private ValueWithLevels lookahead;

    public ColumnReader(Path path, ColumnSchema column, ColumnChunk columnChunk,
                        HardwoodContext context) throws IOException {
        this.column = column;
        this.columnMetaData = columnChunk.metaData();
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.totalValues = columnMetaData.numValues();

        // Open a dedicated FileChannel for this column chunk
        this.channel = FileChannel.open(path, StandardOpenOption.READ);

        // Calculate chunk bounds and create mapping
        Long dictOffset = columnMetaData.dictionaryPageOffset();
        long chunkStart = (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : columnMetaData.dataPageOffset();
        long chunkSize = columnMetaData.totalCompressedSize();

        FileMappingEvent event = new FileMappingEvent();
        event.begin();

        MappedByteBuffer mapping = channel.map(FileChannel.MapMode.READ_ONLY, chunkStart, chunkSize);

        event.path = path.toString();
        event.offset = chunkStart;
        event.size = chunkSize;
        event.column = column.name();
        event.commit();

        // Scan pages using PageScanner
        PageScanner scanner = new PageScanner(column, columnChunk, context, mapping, chunkStart);
        List<PageInfo> pageInfos = scanner.scanPages();

        // Create PageCursor for async prefetching
        this.pageCursor = new PageCursor(pageInfos, context);
    }

    @Override
    public void close() throws IOException {
        channel.close();
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
            value = getPageValue(currentPage, currentPagePosition);
        }
        else {
            // Optional field - check definition level
            if (currentPage.definitionLevels()[currentPagePosition] == maxDefinitionLevel) {
                value = getPageValue(currentPage, currentPagePosition);
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
        if (currentPage == null || currentPagePosition >= currentPage.size()) {
            return null;
        }

        int defLevel = maxDefinitionLevel > 0 ? currentPage.definitionLevels()[currentPagePosition] : 0;
        int repLevel = maxRepetitionLevel > 0 && currentPage.repetitionLevels() != null
                ? currentPage.repetitionLevels()[currentPagePosition]
                : 0;
        Object value = getPageValue(currentPage, currentPagePosition);

        currentPagePosition++;
        valuesRead++;
        return new ValueWithLevels(value, defLevel, repLevel);
    }

    /**
     * Get a value from a page at a given index using typed accessors.
     */
    private static Object getPageValue(Page page, int index) {
        return switch (page) {
            case Page.BooleanPage p -> p.get(index);
            case Page.IntPage p -> p.get(index);
            case Page.LongPage p -> p.get(index);
            case Page.FloatPage p -> p.get(index);
            case Page.DoublePage p -> p.get(index);
            case Page.ByteArrayPage p -> p.get(index);
        };
    }

    /**
     * Ensure a page is loaded.
     */
    private void ensurePageLoaded() throws IOException {
        if (currentPage == null || currentPagePosition >= currentPage.size()) {
            if (pageCursor.hasNext()) {
                currentPage = pageCursor.nextPage();
                currentPagePosition = 0;
            }
        }
    }

    private record ValueWithLevels(Object value, int defLevel, int repLevel) {
    }

    public ColumnSchema getColumnSchema() {
        return column;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }
}
