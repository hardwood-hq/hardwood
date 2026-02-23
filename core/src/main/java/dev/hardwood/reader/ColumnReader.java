/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.ColumnAssemblyBuffer;
import dev.hardwood.internal.reader.ColumnValueIterator;
import dev.hardwood.internal.reader.FileManager;
import dev.hardwood.internal.reader.FlatColumnData;
import dev.hardwood.internal.reader.NestedColumnData;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/**
 * Batch-oriented column reader for reading a single column across all row groups.
 * <p>
 * Provides typed primitive arrays for zero-boxing access. For nested/repeated columns,
 * multi-level offsets and per-level null bitmaps enable efficient traversal without
 * per-row virtual dispatch.
 * </p>
 *
 * <h3>Flat column usage:</h3>
 * <pre>{@code
 * try (ColumnReader reader = fileReader.createColumnReader("fare_amount")) {
 *     while (reader.nextBatch()) {
 *         int count = reader.getRecordCount();
 *         double[] values = reader.getDoubles();
 *         BitSet nulls = reader.getElementNulls();
 *         for (int i = 0; i < count; i++) {
 *             if (nulls == null || !nulls.get(i)) sum += values[i];
 *         }
 *     }
 * }
 * }</pre>
 *
 * <h3>Simple list usage (nestingDepth=1):</h3>
 * <pre>{@code
 * try (ColumnReader reader = fileReader.createColumnReader("fare_components")) {
 *     while (reader.nextBatch()) {
 *         int recordCount = reader.getRecordCount();
 *         int valueCount = reader.getValueCount();
 *         double[] values = reader.getDoubles();
 *         int[] offsets = reader.getOffsets(0);
 *         BitSet recordNulls = reader.getLevelNulls(0);
 *         BitSet elementNulls = reader.getElementNulls();
 *         for (int r = 0; r < recordCount; r++) {
 *             if (recordNulls != null && recordNulls.get(r)) continue;
 *             int start = offsets[r];
 *             int end = (r + 1 < recordCount) ? offsets[r + 1] : valueCount;
 *             for (int i = start; i < end; i++) {
 *                 if (elementNulls == null || !elementNulls.get(i)) sum += values[i];
 *             }
 *         }
 *     }
 * }
 * }</pre>
 */
public class ColumnReader implements AutoCloseable {

    static final int DEFAULT_BATCH_SIZE = 262_144;

    private final ColumnSchema column;
    private final int maxRepetitionLevel;
    private final ColumnValueIterator iterator;
    private final int batchSize;

    // Current batch state
    private TypedColumnData currentBatch;
    private boolean exhausted;

    // Computed nested data (lazily populated per batch)
    private int[][] multiLevelOffsets;
    private BitSet[] levelNulls;
    private BitSet elementNulls;
    private boolean nestedDataComputed;

    /**
     * Single-file constructor. Delegates to the multi-file constructor with no FileManager.
     */
    ColumnReader(ColumnSchema column, List<PageInfo> pageInfos, HardwoodContext context, int batchSize) {
        this(column, pageInfos, context, batchSize, null, -1, null);
    }

    /**
     * Full constructor. When {@code fileManager} is non-null, creates a {@link PageCursor}
     * with cross-file prefetching — matching the pattern used by {@link MultiFileRowReader}.
     */
    ColumnReader(ColumnSchema column, List<PageInfo> pageInfos, HardwoodContext context,
                 int batchSize, FileManager fileManager, int projectedColumnIndex, String fileName) {
        this.column = column;
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.batchSize = batchSize;

        boolean flat = maxRepetitionLevel == 0;

        ColumnAssemblyBuffer assemblyBuffer = null;
        if (flat) {
            assemblyBuffer = new ColumnAssemblyBuffer(column, batchSize);
        }

        PageCursor pageCursor = new PageCursor(
                pageInfos, context, fileManager, projectedColumnIndex, fileName, assemblyBuffer);
        this.iterator = new ColumnValueIterator(pageCursor, column, flat);
    }

    // ==================== Batch Iteration ====================

    /**
     * Advance to the next batch.
     *
     * @return true if a batch is available, false if exhausted
     */
    public boolean nextBatch() {
        if (exhausted) {
            return false;
        }

        currentBatch = iterator.readBatch(batchSize);

        if (currentBatch.recordCount() == 0) {
            exhausted = true;
            currentBatch = null;
            return false;
        }

        // Reset lazy nested computation
        nestedDataComputed = false;
        multiLevelOffsets = null;
        levelNulls = null;
        elementNulls = null;

        return true;
    }

    /**
     * Number of top-level records in the current batch.
     */
    public int getRecordCount() {
        checkBatchAvailable();
        return currentBatch.recordCount();
    }

    /**
     * Total number of leaf values in the current batch.
     * For flat columns, this equals {@link #getRecordCount()}.
     */
    public int getValueCount() {
        checkBatchAvailable();
        return currentBatch.valueCount();
    }

    // ==================== Typed Value Arrays ====================

    public int[] getInts() {
        checkBatchAvailable();
        return switch (currentBatch) {
            case FlatColumnData.IntColumn c -> c.values();
            case NestedColumnData.IntColumn c -> c.values();
            default -> throw typeMismatch("int");
        };
    }

    public long[] getLongs() {
        checkBatchAvailable();
        return switch (currentBatch) {
            case FlatColumnData.LongColumn c -> c.values();
            case NestedColumnData.LongColumn c -> c.values();
            default -> throw typeMismatch("long");
        };
    }

    public float[] getFloats() {
        checkBatchAvailable();
        return switch (currentBatch) {
            case FlatColumnData.FloatColumn c -> c.values();
            case NestedColumnData.FloatColumn c -> c.values();
            default -> throw typeMismatch("float");
        };
    }

    public double[] getDoubles() {
        checkBatchAvailable();
        return switch (currentBatch) {
            case FlatColumnData.DoubleColumn c -> c.values();
            case NestedColumnData.DoubleColumn c -> c.values();
            default -> throw typeMismatch("double");
        };
    }

    public boolean[] getBooleans() {
        checkBatchAvailable();
        return switch (currentBatch) {
            case FlatColumnData.BooleanColumn c -> c.values();
            case NestedColumnData.BooleanColumn c -> c.values();
            default -> throw typeMismatch("boolean");
        };
    }

    public byte[][] getBinaries() {
        checkBatchAvailable();
        return switch (currentBatch) {
            case FlatColumnData.ByteArrayColumn c -> c.values();
            case NestedColumnData.ByteArrayColumn c -> c.values();
            default -> throw typeMismatch("byte[]");
        };
    }

    // ==================== Logical Type Accessors ====================

    /**
     * String values for STRING/JSON/BSON logical type columns.
     * Converts the underlying byte arrays to UTF-8 strings.
     * Null values are represented as null entries in the array.
     *
     * @return String array with converted values
     * @throws IllegalStateException if the column is not a BYTE_ARRAY type
     */
    public String[] getStrings() {
        byte[][] raw = getBinaries();
        int count = currentBatch.valueCount();
        BitSet nulls = getElementNulls();
        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            if (nulls != null && nulls.get(i)) {
                result[i] = null;
            }
            else {
                result[i] = new String(raw[i], StandardCharsets.UTF_8);
            }
        }
        return result;
    }

    // ==================== Null Handling ====================

    /**
     * Null bitmap over leaf values. For flat columns this doubles as record-level nulls.
     *
     * @return BitSet where set bits indicate null values, or null if all elements are required
     */
    public BitSet getElementNulls() {
        checkBatchAvailable();
        if (currentBatch instanceof FlatColumnData flat) {
            return flat.nulls();
        }
        ensureNestedDataComputed();
        return elementNulls;
    }

    /**
     * Null bitmap at a given nesting level. Only valid for nested columns
     * ({@code 0 <= level < getNestingDepth()}).
     *
     * @param level the nesting level (0 = outermost group)
     * @return BitSet where set bits indicate null groups, or null if that level is required
     */
    public BitSet getLevelNulls(int level) {
        checkBatchAvailable();
        checkNestedLevel(level);
        ensureNestedDataComputed();
        return levelNulls[level];
    }

    // ==================== Offsets for Repeated Columns ====================

    /**
     * Nesting depth: 0 for flat, maxRepetitionLevel for nested.
     */
    public int getNestingDepth() {
        return maxRepetitionLevel;
    }

    /**
     * Offset array for a given nesting level. Maps items at level k to positions
     * in the next level (or leaf values for the innermost level).
     *
     * @param level the nesting level (0-indexed)
     * @return offset array for the given level
     */
    public int[] getOffsets(int level) {
        checkBatchAvailable();
        checkNestedLevel(level);
        ensureNestedDataComputed();
        return multiLevelOffsets[level];
    }

    // ==================== Metadata ====================

    public ColumnSchema getColumnSchema() {
        return column;
    }

    @Override
    public void close() {
        // No resources to close; PageCursor/assembly buffer clean up via GC
    }

    // ==================== Internal ====================

    private void checkBatchAvailable() {
        if (currentBatch == null) {
            throw new IllegalStateException("No batch available. Call nextBatch() first.");
        }
    }

    private void checkNestedLevel(int level) {
        if (maxRepetitionLevel == 0) {
            throw new IllegalStateException("Not valid for flat columns (nestingDepth=0)");
        }
        if (level < 0 || level >= maxRepetitionLevel) {
            throw new IndexOutOfBoundsException(
                    "Level " + level + " out of range [0, " + maxRepetitionLevel + ")");
        }
    }

    private IllegalStateException typeMismatch(String expected) {
        return new IllegalStateException(
                "Column '" + column.name() + "' is " + column.type() + ", not " + expected);
    }

    /**
     * Compute multi-level offsets and per-level null bitmaps from the nested batch data.
     */
    private void ensureNestedDataComputed() {
        if (nestedDataComputed) {
            return;
        }
        nestedDataComputed = true;

        if (!(currentBatch instanceof NestedColumnData nested)) {
            return;
        }

        int[] repLevels = nested.repetitionLevels();
        int[] defLevels = nested.definitionLevels();
        int valueCount = nested.valueCount();
        int recordCount = nested.recordCount();
        int maxDefLevel = nested.maxDefinitionLevel();

        if (repLevels == null || valueCount == 0) {
            multiLevelOffsets = new int[maxRepetitionLevel][];
            levelNulls = new BitSet[maxRepetitionLevel];
            elementNulls = NestedLevelComputer.computeElementNulls(defLevels, valueCount, maxDefLevel);
            return;
        }

        multiLevelOffsets = NestedLevelComputer.computeMultiLevelOffsets(
                repLevels, valueCount, recordCount, maxRepetitionLevel);
        elementNulls = NestedLevelComputer.computeElementNulls(defLevels, valueCount, maxDefLevel);
        levelNulls = NestedLevelComputer.computeLevelNulls(
                defLevels, repLevels, valueCount, maxDefLevel, maxRepetitionLevel);
    }

    // ==================== Factory ====================

    /**
     * Create a ColumnReader for a named column, scanning pages across all row groups.
     */
    static ColumnReader create(String columnName, FileSchema schema,
                               MappedByteBuffer fileMapping, List<RowGroup> rowGroups,
                               HardwoodContext context) {
        ColumnSchema columnSchema = schema.getColumn(columnName);
        return create(columnSchema, schema, fileMapping, rowGroups, context);
    }

    /**
     * Create a ColumnReader for a column by index, scanning pages across all row groups.
     */
    static ColumnReader create(int columnIndex, FileSchema schema,
                               MappedByteBuffer fileMapping, List<RowGroup> rowGroups,
                               HardwoodContext context) {
        ColumnSchema columnSchema = schema.getColumn(columnIndex);
        return create(columnSchema, schema, fileMapping, rowGroups, context);
    }

    /**
     * Create a ColumnReader for a given ColumnSchema, scanning pages across all row groups.
     */
    @SuppressWarnings("unchecked")
    private static ColumnReader create(ColumnSchema columnSchema, FileSchema schema,
                                       MappedByteBuffer fileMapping, List<RowGroup> rowGroups,
                                       HardwoodContext context) {
        int originalIndex = columnSchema.columnIndex();

        // Scan pages for this column across all row groups in parallel
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[rowGroups.size()];

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            final int rg = rgIdx;
            scanFutures[rg] = CompletableFuture.supplyAsync(() -> {
                ColumnChunk columnChunk = rowGroups.get(rg).columns().get(originalIndex);
                PageScanner scanner = new PageScanner(columnSchema, columnChunk, context, fileMapping, 0);
                try {
                    return scanner.scanPages();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(
                            "Failed to scan pages for column " + columnSchema.name(), e);
                }
            }, context.executor());
        }

        CompletableFuture.allOf(scanFutures).join();

        List<PageInfo> allPages = new ArrayList<>();
        for (CompletableFuture<List<PageInfo>> future : scanFutures) {
            allPages.addAll(future.join());
        }

        return new ColumnReader(columnSchema, allPages, context, DEFAULT_BATCH_SIZE);
    }
}
