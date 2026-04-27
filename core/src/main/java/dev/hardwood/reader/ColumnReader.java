/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.FlatColumnWorker;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.NestedColumnWorker;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageSource;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Batch-oriented column reader for reading a single column across all row groups.
///
/// Provides typed primitive arrays for zero-boxing access. For nested/repeated columns,
/// multi-level offsets and per-level null bitmaps enable efficient traversal without
/// per-row virtual dispatch.
///
/// **Flat column usage:**
/// ```java
/// try (ColumnReader reader = fileReader.columnReader("fare_amount")) {
///     while (reader.nextBatch()) {
///         int count = reader.getRecordCount();
///         double[] values = reader.getDoubles();
///         BitSet nulls = reader.getElementNulls();
///         for (int i = 0; i < count; i++) {
///             if (nulls == null || !nulls.get(i)) sum += values[i];
///         }
///     }
/// }
/// ```
///
/// **Simple list usage (nestingDepth=1):**
/// ```java
/// try (ColumnReader reader = fileReader.columnReader("fare_components")) {
///     while (reader.nextBatch()) {
///         int recordCount = reader.getRecordCount();
///         int valueCount = reader.getValueCount();
///         double[] values = reader.getDoubles();
///         int[] offsets = reader.getOffsets(0);
///         BitSet recordNulls = reader.getLevelNulls(0);
///         BitSet elementNulls = reader.getElementNulls();
///         for (int r = 0; r < recordCount; r++) {
///             if (recordNulls != null && recordNulls.get(r)) continue;
///             int start = offsets[r];
///             int end = (r + 1 < recordCount) ? offsets[r + 1] : valueCount;
///             for (int i = start; i < end; i++) {
///                 if (elementNulls == null || !elementNulls.get(i)) sum += values[i];
///             }
///         }
///     }
/// }
/// ```
public class ColumnReader implements AutoCloseable {

    static final int DEFAULT_BATCH_SIZE = 262_144;

    private final ColumnSchema column;
    private final int maxRepetitionLevel;
    private final int maxDefinitionLevel;
    private final boolean nested;
    private final BatchExchange<BatchExchange.Batch> flatBuffer;
    private final BatchExchange<NestedBatch> nestedBuffer;
    private final AutoCloseable columnWorker;
    private final RowGroupIterator rowGroupIterator;
    private final int[] levelNullThresholds;

    // Current batch state (flat uses BatchExchange.Batch, nested uses NestedBatch)
    private BatchExchange.Batch currentFlatBatch;
    private NestedBatch currentNestedBatch;
    private int recordCount;
    private int valueCount;
    private boolean exhausted;

    // Computed nested data (lazily populated per batch)
    private int[][] multiLevelOffsets;
    private BitSet[] levelNulls;
    private BitSet elementNulls;
    private boolean nestedDataComputed;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;

    @SuppressWarnings("unchecked")
    private ColumnReader(ColumnSchema column, boolean nested,
                         BatchExchange<?> buffer, AutoCloseable columnWorker,
                         RowGroupIterator rowGroupIterator, int[] levelNullThresholds) {
        this.column = column;
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.nested = nested;
        this.flatBuffer = nested ? null : (BatchExchange<BatchExchange.Batch>) buffer;
        this.nestedBuffer = nested ? (BatchExchange<NestedBatch>) buffer : null;
        this.columnWorker = columnWorker;
        this.rowGroupIterator = rowGroupIterator;
        this.levelNullThresholds = levelNullThresholds;
    }

    static ColumnReader forFlat(ColumnSchema column, BatchExchange<BatchExchange.Batch> flatBuffer,
                                AutoCloseable columnWorker, RowGroupIterator rowGroupIterator) {
        return new ColumnReader(column, false, flatBuffer, columnWorker, rowGroupIterator, null);
    }

    static ColumnReader forNested(ColumnSchema column, BatchExchange<NestedBatch> nestedBuffer,
                                  AutoCloseable columnWorker, RowGroupIterator rowGroupIterator,
                                  int[] levelNullThresholds) {
        return new ColumnReader(column, true, nestedBuffer, columnWorker, rowGroupIterator, levelNullThresholds);
    }

    // ==================== Batch Iteration ====================

    /// Advance to the next batch.
    ///
    /// @return true if a batch is available, false if exhausted
    public boolean nextBatch() {
        if (exhausted) {
            return false;
        }

        if (nested) {
            NestedBatch batch = pollNestedBatch();
            if (batch == null || batch.recordCount == 0) {
                nestedBuffer.checkError();
                exhausted = true;
                currentFlatBatch = null;
                currentNestedBatch = null;
                return false;
            }
            currentNestedBatch = batch;
            currentFlatBatch = null;
            recordCount = batch.recordCount;
            valueCount = batch.valueCount;
            currentFileName = batch.fileName;
        }
        else {
            BatchExchange.Batch batch = pollFlatBatch();
            if (batch == null || batch.recordCount == 0) {
                flatBuffer.checkError();
                exhausted = true;
                currentFlatBatch = null;
                currentNestedBatch = null;
                return false;
            }
            currentFlatBatch = batch;
            currentNestedBatch = null;
            recordCount = batch.recordCount;
            valueCount = batch.recordCount;
            currentFileName = batch.fileName;
        }

        // Reset lazy nested computation
        nestedDataComputed = false;
        multiLevelOffsets = null;
        levelNulls = null;
        elementNulls = null;

        return true;
    }

    /// Polls the flat [BatchExchange] for the next batch.
    private BatchExchange.Batch pollFlatBatch() {
        currentFlatBatch = null;
        try {
            return flatBuffer.poll();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /// Polls the nested [BatchExchange] for the next batch.
    private NestedBatch pollNestedBatch() {
        currentNestedBatch = null;
        try {
            return nestedBuffer.poll();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /// Number of top-level records in the current batch.
    public int getRecordCount() {
        checkBatchAvailable();
        return recordCount;
    }

    /// Total number of leaf values in the current batch.
    /// For flat columns, this equals [#getRecordCount()].
    public int getValueCount() {
        checkBatchAvailable();
        return valueCount;
    }

    // ==================== Typed Value Arrays ====================

    public int[] getInts() {
        checkBatchAvailable();
        if (!(currentValues() instanceof int[] a)) {
            throw typeMismatch("int");
        }
        return a;
    }

    public long[] getLongs() {
        checkBatchAvailable();
        if (!(currentValues() instanceof long[] a)) {
            throw typeMismatch("long");
        }
        return a;
    }

    public float[] getFloats() {
        checkBatchAvailable();
        if (!(currentValues() instanceof float[] a)) {
            throw typeMismatch("float");
        }
        return a;
    }

    public double[] getDoubles() {
        checkBatchAvailable();
        if (!(currentValues() instanceof double[] a)) {
            throw typeMismatch("double");
        }
        return a;
    }

    public boolean[] getBooleans() {
        checkBatchAvailable();
        if (!(currentValues() instanceof boolean[] a)) {
            throw typeMismatch("boolean");
        }
        return a;
    }

    public byte[][] getBinaries() {
        checkBatchAvailable();
        if (!(currentValues() instanceof byte[][] a)) {
            throw typeMismatch("byte[]");
        }
        return a;
    }

    // ==================== Logical Type Accessors ====================

    /// String values for STRING and JSON logical type columns.
    /// Converts the underlying byte arrays to UTF-8 strings.
    /// Null values are represented as null entries in the array.
    /// BSON columns are not string-decoded; use [#getBinaries()] for those.
    ///
    /// @return String array with converted values
    /// @throws IllegalStateException if the column is not a BYTE_ARRAY type
    public String[] getStrings() {
        byte[][] raw = getBinaries();
        BitSet nulls = getElementNulls();
        String[] result = new String[valueCount];
        for (int i = 0; i < valueCount; i++) {
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

    /// Null bitmap over leaf values. For flat columns this doubles as record-level nulls.
    ///
    /// @return BitSet where set bits indicate null values, or null if all elements are required
    public BitSet getElementNulls() {
        checkBatchAvailable();
        if (!nested) {
            return currentFlatBatch.nulls;
        }
        ensureNestedDataComputed();
        return elementNulls;
    }

    /// Null bitmap at a given nesting level. Only valid for nested columns
    /// (`0 <= level < getNestingDepth()`).
    ///
    /// @param level the nesting level (0 = outermost group)
    /// @return BitSet where set bits indicate null groups, or null if that level is required
    public BitSet getLevelNulls(int level) {
        checkBatchAvailable();
        checkNestedLevel(level);
        ensureNestedDataComputed();
        return levelNulls[level];
    }

    // ==================== Offsets for Repeated Columns ====================

    /// Nesting depth: 0 for flat, maxRepetitionLevel for nested.
    public int getNestingDepth() {
        return maxRepetitionLevel;
    }

    /// Offset array for a given nesting level. Maps items at level k to positions
    /// in the next level (or leaf values for the innermost level).
    ///
    /// @param level the nesting level (0-indexed)
    /// @return offset array for the given level
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
        if (columnWorker != null) {
            try {
                columnWorker.close();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to close column worker", e);
            }
        }
        if (rowGroupIterator != null) {
            rowGroupIterator.close();
        }
    }

    // ==================== Internal ====================

    private String prefix() {
        return ExceptionContext.filePrefix(currentFileName);
    }

    /// Returns the values array from the current batch (flat or nested).
    private Object currentValues() {
        if (currentFlatBatch != null) {
            return currentFlatBatch.values;
        }
        return currentNestedBatch.values;
    }

    private void checkBatchAvailable() {
        if (currentFlatBatch == null && currentNestedBatch == null) {
            throw new IllegalStateException(prefix() + "No batch available. Call nextBatch() first.");
        }
    }

    private void checkNestedLevel(int level) {
        if (maxRepetitionLevel == 0) {
            throw new IllegalStateException(prefix() + "Not valid for flat columns (nestingDepth=0)");
        }
        if (level < 0 || level >= maxRepetitionLevel) {
            throw new IndexOutOfBoundsException(prefix()
                    + "Level " + level + " out of range [0, " + maxRepetitionLevel + ")");
        }
    }

    private IllegalStateException typeMismatch(String expected) {
        return new IllegalStateException(prefix()
                + "Column '" + column.name() + "' is " + column.type() + ", not " + expected);
    }

    /// Reads pre-computed index structures from the [NestedBatch].
    /// The drain thread computes these before publishing.
    private void ensureNestedDataComputed() {
        if (nestedDataComputed) {
            return;
        }
        nestedDataComputed = true;

        elementNulls = currentNestedBatch.elementNulls;
        multiLevelOffsets = currentNestedBatch.multiLevelOffsets;
        levelNulls = currentNestedBatch.levelNulls;

        // Provide empty arrays when fields are null (non-repeated columns)
        if (multiLevelOffsets == null) {
            multiLevelOffsets = new int[maxRepetitionLevel][];
        }
        if (levelNulls == null) {
            levelNulls = new BitSet[maxRepetitionLevel];
        }
    }

    // ==================== Factory ====================

    /// Create a ColumnReader for a named column, scanning pages across all row groups.
    static ColumnReader create(String columnName, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context) {
        ColumnSchema columnSchema = schema.getColumn(columnName);
        return create(columnSchema, schema, inputFile, rowGroups, context, null);
    }

    /// Create a ColumnReader for a named column with page-level filtering.
    ///
    /// @param filter resolved predicate, or `null` for no filtering.
    static ColumnReader create(String columnName, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context, ResolvedPredicate filter) {
        ColumnSchema columnSchema = schema.getColumn(columnName);
        return create(columnSchema, schema, inputFile, rowGroups, context, filter);
    }

    /// Create a ColumnReader for a column by index, scanning pages across all row groups.
    static ColumnReader create(int columnIndex, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context) {
        ColumnSchema columnSchema = schema.getColumn(columnIndex);
        return create(columnSchema, schema, inputFile, rowGroups, context, null);
    }

    /// Create a ColumnReader for a column by index with page-level filtering.
    ///
    /// @param filter resolved predicate, or `null` for no filtering.
    static ColumnReader create(int columnIndex, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context, ResolvedPredicate filter) {
        ColumnSchema columnSchema = schema.getColumn(columnIndex);
        return create(columnSchema, schema, inputFile, rowGroups, context, filter);
    }

    /// Create a ColumnReader for a given ColumnSchema using the v3 pipeline.
    private static ColumnReader create(ColumnSchema columnSchema, FileSchema schema,
                                       InputFile inputFile, List<RowGroup> rowGroups,
                                       HardwoodContextImpl context, ResolvedPredicate filter) {
        // Create a single-column projection using the full field path
        // to avoid ambiguity with duplicate leaf names in nested schemas.
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema,
                dev.hardwood.schema.ColumnProjection.columns(columnSchema.fieldPath().toString()));

        RowGroupIterator rowGroupIterator = new RowGroupIterator(
                List.of(inputFile), context, 0);
        rowGroupIterator.setFirstFile(schema, rowGroups);
        rowGroupIterator.initialize(projectedSchema, filter);

        return createFromIterator(columnSchema, schema, rowGroupIterator, context, 0, rowGroupIterator);
    }

    /// Creates a ColumnReader from a pre-configured RowGroupIterator.
    /// Used by both single-file and multi-file paths.
    ///
    /// @param projectedColumnIndex the column's index within the projected schema
    /// @param ownedIterator if non-null, the ColumnReader takes ownership and closes
    ///        it on [#close()]. Pass `null` when the caller manages the iterator lifecycle.
    static ColumnReader createFromIterator(ColumnSchema columnSchema, FileSchema schema,
                                           RowGroupIterator rowGroupIterator,
                                           HardwoodContextImpl context,
                                           int projectedColumnIndex,
                                           RowGroupIterator ownedIterator) {
        boolean nested = columnSchema.maxRepetitionLevel() > 0;

        PageSource pageSource = new PageSource(rowGroupIterator, projectedColumnIndex);
        dev.hardwood.metadata.PhysicalType physType = columnSchema.type();

        if (nested) {
            BatchExchange<NestedBatch> nestedBuf = BatchExchange.detaching(
                    columnSchema.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(physType, DEFAULT_BATCH_SIZE);
                        return b;
                    });
            int[] thresholds = NestedLevelComputer.computeLevelNullThresholds(
                    schema.getRootNode(), columnSchema.columnIndex());
            NestedColumnWorker nestedWorker = new NestedColumnWorker(
                    pageSource, nestedBuf, columnSchema, DEFAULT_BATCH_SIZE,
                    context.decompressorFactory(), context.executor(), 0,
                    thresholds);
            nestedWorker.start();
            return ColumnReader.forNested(columnSchema, nestedBuf, nestedWorker, ownedIterator, thresholds);
        }
        else {
            BatchExchange<BatchExchange.Batch> flatBuf = BatchExchange.detaching(
                    columnSchema.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(physType, DEFAULT_BATCH_SIZE);
                        return b;
                    });
            FlatColumnWorker flatWorker = new FlatColumnWorker(
                    pageSource, flatBuf, columnSchema, DEFAULT_BATCH_SIZE,
                    context.decompressorFactory(), context.executor(), 0);
            flatWorker.start();
            return ColumnReader.forFlat(columnSchema, flatBuf, flatWorker, ownedIterator);
        }
    }
}
