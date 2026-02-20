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
            elementNulls = computeElementNulls(defLevels, valueCount, maxDefLevel);
            return;
        }

        multiLevelOffsets = computeMultiLevelOffsets(repLevels, valueCount, recordCount, maxRepetitionLevel);
        computeNullBitmaps(defLevels, repLevels, valueCount, maxDefLevel, maxRepetitionLevel);
    }

    /**
     * Compute multi-level offset arrays from repetition levels.
     * <p>
     * For maxRepLevel=1 (simple list): one offset array mapping records to value positions.
     * For maxRepLevel=N (nested list): N offset arrays, chained.
     * Level k boundary: positions where repLevel[i] <= k.
     */
    static int[][] computeMultiLevelOffsets(int[] repLevels, int valueCount,
                                            int recordCount, int maxRepLevel) {
        if (maxRepLevel == 1) {
            // Simple list: single offset array from record offsets
            // repLevel == 0 starts a new record
            int[] offsets = new int[recordCount];
            int recordIdx = 0;
            for (int i = 0; i < valueCount; i++) {
                if (repLevels[i] == 0) {
                    if (recordIdx < recordCount) {
                        offsets[recordIdx] = i;
                    }
                    recordIdx++;
                }
            }
            return new int[][] { offsets };
        }

        // General case: multi-level offsets
        // First pass: count items at each level
        int[] itemCounts = new int[maxRepLevel];
        for (int i = 0; i < valueCount; i++) {
            int rep = repLevels[i];
            // A value with repLevel <= k starts a new item at level k
            for (int k = rep; k < maxRepLevel; k++) {
                itemCounts[k]++;
            }
        }

        // Allocate offset arrays
        int[][] offsets = new int[maxRepLevel][];
        for (int k = 0; k < maxRepLevel; k++) {
            offsets[k] = new int[itemCounts[k]];
        }

        // Second pass: fill offsets
        // offsets[k] maps level-k items to level-(k+1) item indices (or value indices for last)
        int[] itemIndices = new int[maxRepLevel]; // current item index at each level
        int[] nextLevelPositions = new int[maxRepLevel]; // next position in the child level

        for (int i = 0; i < valueCount; i++) {
            int rep = repLevels[i];

            // For each level from rep to maxRepLevel-1, start a new item
            for (int k = rep; k < maxRepLevel; k++) {
                int idx = itemIndices[k];
                if (k == maxRepLevel - 1) {
                    // Innermost level: offset points to value position
                    offsets[k][idx] = i;
                } else {
                    // Intermediate level: offset points to child level item index
                    offsets[k][idx] = itemIndices[k + 1];
                }
                itemIndices[k]++;
            }
        }

        return offsets;
    }

    /**
     * Compute null bitmaps for element level and each nesting level.
     */
    private void computeNullBitmaps(int[] defLevels, int[] repLevels,
                                    int valueCount, int maxDefLevel, int maxRepLevel) {
        // Element nulls: leaf values where defLevel < maxDefLevel
        elementNulls = computeElementNulls(defLevels, valueCount, maxDefLevel);

        // Level nulls: for each nesting level k, a null at that level means
        // the definition level at a group boundary is below the threshold for that level.
        // The def level threshold for level k is: k + 1 (since each repeated/optional
        // group adds one def level, and the root starts at 0).
        // A group at level k is null when defLevel < defLevelThreshold(k).
        levelNulls = new BitSet[maxRepLevel];

        for (int k = 0; k < maxRepLevel; k++) {
            // The def level needed for level k to be non-null:
            // For a list schema like: optional group (def=1) -> repeated group (def=2) -> element (def=3)
            // Level 0 null = record-level null = defLevel at boundary < 1
            // The threshold is (k + 1) for simple schemas but we need to derive from the
            // actual schema path. Since maxDefLevel includes all optional/repeated ancestors,
            // and maxRepLevel is the count of repeated levels:
            // defThreshold for level k = maxDefLevel - maxRepLevel + k
            // This works because the last maxRepLevel def levels correspond to the repeated groups,
            // and we need at least (maxDefLevel - maxRepLevel + k + 1) to be non-null at level k.
            int defThreshold = maxDefLevel - maxRepLevel + k + 1;
            BitSet nullBits = null;

            int itemIdx = 0;
            for (int i = 0; i < valueCount; i++) {
                if (repLevels[i] <= k) {
                    // This is a boundary for level k
                    if (defLevels[i] < defThreshold) {
                        if (nullBits == null) {
                            nullBits = new BitSet();
                        }
                        nullBits.set(itemIdx);
                    }
                    itemIdx++;
                }
            }

            levelNulls[k] = nullBits;
        }
    }

    /**
     * Compute element-level null bitmap.
     */
    private static BitSet computeElementNulls(int[] defLevels, int valueCount, int maxDefLevel) {
        if (defLevels == null || maxDefLevel == 0) {
            return null; // All required — no nulls possible
        }
        BitSet nulls = null;
        for (int i = 0; i < valueCount; i++) {
            if (defLevels[i] < maxDefLevel) {
                if (nulls == null) {
                    nulls = new BitSet(valueCount);
                }
                nulls.set(i);
            }
        }
        return nulls;
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
