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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.ChunkRange;
import dev.hardwood.internal.reader.ColumnAssemblyBuffer;
import dev.hardwood.internal.reader.ColumnIndexBuffers;
import dev.hardwood.internal.reader.ColumnValueIterator;
import dev.hardwood.internal.reader.FileManager;
import dev.hardwood.internal.reader.FlatColumnData;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.NestedColumnData;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageRange;
import dev.hardwood.internal.reader.PageRangeData;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.RowGroupPageSource;
import dev.hardwood.internal.reader.RowRanges;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.internal.reader.WindowedChunkReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Batch-oriented column reader for reading a single column across all row groups.
///
/// Provides typed primitive arrays for zero-boxing access. For nested/repeated columns,
/// multi-level offsets and per-level null bitmaps enable efficient traversal without
/// per-row virtual dispatch.
///
/// **Flat column usage:**
/// ```java
/// try (ColumnReader reader = fileReader.createColumnReader("fare_amount")) {
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
/// try (ColumnReader reader = fileReader.createColumnReader("fare_components")) {
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
    private final ColumnValueIterator iterator;
    private final int batchSize;
    private final int[] levelNullThresholds; // pre-computed per rep level

    // Current batch state
    private TypedColumnData currentBatch;
    private boolean exhausted;

    // Computed nested data (lazily populated per batch)
    private int[][] multiLevelOffsets;
    private BitSet[] levelNulls;
    private BitSet elementNulls;
    private boolean nestedDataComputed;

    // File name for exception context — dynamic for multi-file readers
    private final java.util.function.Supplier<String> fileNameSupplier;

    /// Single-file constructor with lazy row-group and page fetching.
    ColumnReader(ColumnSchema column, PageScanner firstScanner, HardwoodContextImpl context,
                 int batchSize, int[] levelNullThresholds,
                 RowGroupPageSource rowGroupSource, int firstRowGroupIndex,
                 int totalRowGroups, String fileName) {
        this.column = column;
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.batchSize = batchSize;
        this.levelNullThresholds = levelNullThresholds;

        boolean flat = maxRepetitionLevel == 0;

        ColumnAssemblyBuffer assemblyBuffer = null;
        if (flat) {
            assemblyBuffer = new ColumnAssemblyBuffer(column, batchSize);
        }

        PageCursor pageCursor = PageCursor.create(firstScanner, context, 0,
                fileName, assemblyBuffer, rowGroupSource, firstRowGroupIndex, totalRowGroups);
        this.iterator = new ColumnValueIterator(pageCursor, column, flat);
        this.fileNameSupplier = iterator::getCurrentFileName;
    }

    /// Multi-file constructor. When `fileManager` is non-null, creates a [PageCursor]
    /// with cross-file prefetching — matching the pattern used by [MultiFileRowReader].
    ColumnReader(ColumnSchema column, List<PageInfo> pageInfos, HardwoodContextImpl context,
                 int batchSize, int[] levelNullThresholds,
                 FileManager fileManager, int projectedColumnIndex, String fileName) {
        this.column = column;
        this.maxRepetitionLevel = column.maxRepetitionLevel();
        this.batchSize = batchSize;
        this.levelNullThresholds = levelNullThresholds;

        boolean flat = maxRepetitionLevel == 0;

        ColumnAssemblyBuffer assemblyBuffer = null;
        if (flat) {
            assemblyBuffer = new ColumnAssemblyBuffer(column, batchSize);
        }

        PageCursor pageCursor = PageCursor.create(
                pageInfos, context, fileManager, projectedColumnIndex, fileName, assemblyBuffer);
        this.iterator = new ColumnValueIterator(pageCursor, column, flat);
        this.fileNameSupplier = iterator::getCurrentFileName;
    }

    // ==================== Batch Iteration ====================

    /// Advance to the next batch.
    ///
    /// @return true if a batch is available, false if exhausted
    public boolean nextBatch() {
        try {
            return nextBatchInternal();
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    private boolean nextBatchInternal() {
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

    /// Number of top-level records in the current batch.
    public int getRecordCount() {
        try {
            checkBatchAvailable();
            return currentBatch.recordCount();
        } catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    /// Total number of leaf values in the current batch.
    /// For flat columns, this equals [#getRecordCount()].
    public int getValueCount() {
        try {
            checkBatchAvailable();
            return currentBatch.valueCount();
        } catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Typed Value Arrays ====================

    public int[] getInts() {
        try {
            checkBatchAvailable();
            return switch (currentBatch) {
                case FlatColumnData.IntColumn c -> c.values();
                case NestedColumnData.IntColumn c -> c.values();
                default -> throw typeMismatch("int");
            };
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    public long[] getLongs() {
        try {
            checkBatchAvailable();
            return switch (currentBatch) {
                case FlatColumnData.LongColumn c -> c.values();
                case NestedColumnData.LongColumn c -> c.values();
                default -> throw typeMismatch("long");
            };
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    public float[] getFloats() {
        try {
            checkBatchAvailable();
            return switch (currentBatch) {
                case FlatColumnData.FloatColumn c -> c.values();
                case NestedColumnData.FloatColumn c -> c.values();
                default -> throw typeMismatch("float");
            };
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    public double[] getDoubles() {
        try {
            checkBatchAvailable();
            return switch (currentBatch) {
                case FlatColumnData.DoubleColumn c -> c.values();
                case NestedColumnData.DoubleColumn c -> c.values();
                default -> throw typeMismatch("double");
            };
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    public boolean[] getBooleans() {
        try {
            checkBatchAvailable();
            return switch (currentBatch) {
                case FlatColumnData.BooleanColumn c -> c.values();
                case NestedColumnData.BooleanColumn c -> c.values();
                default -> throw typeMismatch("boolean");
            };
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    public byte[][] getBinaries() {
        try {
            checkBatchAvailable();
            return switch (currentBatch) {
                case FlatColumnData.ByteArrayColumn c -> c.values();
                case NestedColumnData.ByteArrayColumn c -> c.values();
                default -> throw typeMismatch("byte[]");
            };
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Logical Type Accessors ====================

    /// String values for STRING/JSON/BSON logical type columns.
    /// Converts the underlying byte arrays to UTF-8 strings.
    /// Null values are represented as null entries in the array.
    ///
    /// @return String array with converted values
    /// @throws IllegalStateException if the column is not a BYTE_ARRAY type
    public String[] getStrings() {
        try {
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
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    // ==================== Null Handling ====================

    /// Null bitmap over leaf values. For flat columns this doubles as record-level nulls.
    ///
    /// @return BitSet where set bits indicate null values, or null if all elements are required
    public BitSet getElementNulls() {
        try {
            checkBatchAvailable();
            if (currentBatch instanceof FlatColumnData flat) {
                return flat.nulls();
            }
            ensureNestedDataComputed();
            return elementNulls;
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
    }

    /// Null bitmap at a given nesting level. Only valid for nested columns
    /// (`0 <= level < getNestingDepth()`).
    ///
    /// @param level the nesting level (0 = outermost group)
    /// @return BitSet where set bits indicate null groups, or null if that level is required
    public BitSet getLevelNulls(int level) {
        try {
            checkBatchAvailable();
            checkNestedLevel(level);
            ensureNestedDataComputed();
            return levelNulls[level];
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
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
        try {
            checkBatchAvailable();
            checkNestedLevel(level);
            ensureNestedDataComputed();
            return multiLevelOffsets[level];
        }
        catch (RuntimeException e) {
            throw wrapWithFileContext(e);
        }
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

    /// Enriches a runtime exception with the current file name. Delegates to
    /// [ExceptionContext.addFileContext] which preserves the original exception
    /// type and cause chain.
    private RuntimeException wrapWithFileContext(RuntimeException e) {
        return ExceptionContext.addFileContext(fileNameSupplier.get(), e);
    }

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

    /// Compute multi-level offsets and per-level null bitmaps from the nested batch data.
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
                defLevels, repLevels, valueCount, maxRepetitionLevel, levelNullThresholds);
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
    /// @param filterPredicate resolved predicate, or `null` for no filtering.
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
    /// @param filterPredicate resolved predicate, or `null` for no filtering.
    static ColumnReader create(int columnIndex, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context, ResolvedPredicate filter) {
        ColumnSchema columnSchema = schema.getColumn(columnIndex);
        return create(columnSchema, schema, inputFile, rowGroups, context, filter);
    }

    /// Create a ColumnReader for a given ColumnSchema, scanning only the first row group.
    /// Subsequent row groups are fetched lazily by `PageCursor` via `RowGroupPageSource`.
    ///
    /// @param filterPredicate resolved predicate, or `null` for no filtering.
    private static ColumnReader create(ColumnSchema columnSchema, FileSchema schema,
                                       InputFile inputFile, List<RowGroup> rowGroups,
                                       HardwoodContextImpl context, ResolvedPredicate filter) {
        int originalIndex = columnSchema.columnIndex();
        int[] projectedColumns = new int[]{ originalIndex };
        String fileName = inputFile.name();

        // RowGroupPageSource for lazy fetching of subsequent row groups
        RowGroupPageSource rowGroupSource = (rgIndex, colIndex) ->
                createScannerForRowGroup(rgIndex, columnSchema, rowGroups, inputFile, context, filter,
                        originalIndex, fileName);

        // Create scanner for the first row group
        PageScanner firstScanner = rowGroups.isEmpty()
                ? null
                : createScannerForRowGroup(0, columnSchema, rowGroups, inputFile, context, filter,
                        originalIndex, fileName);

        int[] thresholds = null;
        if (columnSchema.maxRepetitionLevel() > 0) {
            thresholds = NestedLevelComputer.computeLevelNullThresholds(
                    schema.getRootNode(), columnSchema.columnIndex());
        }
        return new ColumnReader(columnSchema, firstScanner, context, DEFAULT_BATCH_SIZE,
                thresholds, rowGroupSource, 0, rowGroups.size(), fileName);
    }

    /// Creates a PageScanner for a single row group.
    private static PageScanner createScannerForRowGroup(int rowGroupIndex, ColumnSchema columnSchema,
                                                         List<RowGroup> rowGroups, InputFile inputFile,
                                                         HardwoodContextImpl context, ResolvedPredicate filter,
                                                         int originalIndex, String fileName) {
        if (rowGroupIndex >= rowGroups.size()) {
            return null;
        }

        RowGroup rowGroup = rowGroups.get(rowGroupIndex);

        RowGroupIndexBuffers indexBuffers;
        try {
            indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to fetch index buffers for row group " + rowGroupIndex, e);
        }

        RowRanges matchingRows = RowRanges.ALL;
        if (filter != null) {
            matchingRows = PageFilterEvaluator.computeMatchingRows(
                    filter, rowGroup, indexBuffers);
        }

        ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);

        // Try page-range I/O when filtering is active
        if (!matchingRows.isAll()) {
            ColumnIndexBuffers colBuffers = indexBuffers.forColumn(originalIndex);
            if (colBuffers != null && colBuffers.offsetIndex() != null) {
                try {
                    OffsetIndex offsetIndex = OffsetIndexReader.read(
                            new ThriftCompactReader(colBuffers.offsetIndex()));
                    List<PageRange> pageRanges = PageRange.forColumn(
                            offsetIndex, matchingRows, columnChunk, rowGroup.numRows(), ChunkRange.MAX_GAP_BYTES);
                    if (!pageRanges.isEmpty()) {
                        PageRangeData pageRangeData = PageRangeData.fetch(inputFile, pageRanges);
                        return new PageScanner(columnSchema, columnChunk, context,
                                pageRangeData, indexBuffers.forColumn(originalIndex),
                                rowGroupIndex, fileName, matchingRows, 0);
                    }
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to fetch page ranges for row group " + rowGroupIndex, e);
                }
            }
        }

        // Determine the column chunk's file offset and length
        long colChunkOffset = SingleFileRowReader.chunkStartOffset(columnChunk);
        int colChunkLength = Math.toIntExact(columnChunk.metaData().totalCompressedSize());

        // OffsetIndex path (no filter): fetch the full chunk for direct page lookup
        if (columnChunk.offsetIndexOffset() != null) {
            try {
                ByteBuffer colChunkData = inputFile.readRange(colChunkOffset, colChunkLength);
                return new PageScanner(columnSchema, columnChunk, context,
                        colChunkData, colChunkOffset,
                        indexBuffers.forColumn(originalIndex),
                        rowGroupIndex, fileName, matchingRows, 0);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch column chunk data for row group " + rowGroupIndex, e);
            }
        }

        // Sequential path (no OffsetIndex): use WindowedChunkReader for lazy fetching
        WindowedChunkReader chunkReader = new WindowedChunkReader(
                inputFile, colChunkOffset, colChunkLength);
        return new PageScanner(columnSchema, columnChunk, context,
                chunkReader, indexBuffers.forColumn(originalIndex),
                rowGroupIndex, fileName, matchingRows, 0);
    }
}
