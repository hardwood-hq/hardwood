/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Reader for one or more Parquet files.
///
/// A reader opened over a list of files exposes the schema of the first file
/// and reads rows / column batches across all files in order, with
/// cross-file prefetching handled by the underlying iterator.
///
/// ```java
/// // Single file
/// try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
///     RowReader rows = reader.rowReader();
///     // ...
/// }
///
/// // Multiple files (use Hardwood for a shared thread pool)
/// try (Hardwood hardwood = Hardwood.create();
///      ParquetFileReader reader = hardwood.openAll(files)) {
///     try (ColumnReaders cols = reader.columnReaders(
///                 ColumnProjection.columns("a", "b"))) {
///         // ...
///     }
/// }
/// ```
///
/// **Limitation:** When using the default memory-mapped [InputFile],
/// individual files must be at most 2 GB ([Integer#MAX_VALUE] bytes).
/// Larger datasets should be split across multiple files.
public class ParquetFileReader implements AutoCloseable {

    private final List<InputFile> inputFiles;
    /// Metadata of the first file. For single-file readers this is the
    /// file's metadata; for multi-file readers callers wanting per-file
    /// metadata must open each file individually.
    private final FileMetaData firstFileMetaData;
    private final FileSchema schema;
    private final HardwoodContextImpl context;
    private final boolean ownsContext;
    private final boolean ownsInputFiles;
    private final List<RowGroupIterator> rowGroupIterators = new ArrayList<>();

    private ParquetFileReader(List<InputFile> inputFiles, FileMetaData firstFileMetaData,
                              FileSchema schema, HardwoodContextImpl context,
                              boolean ownsContext, boolean ownsInputFiles) {
        this.inputFiles = inputFiles;
        this.firstFileMetaData = firstFileMetaData;
        this.schema = schema;
        this.context = context;
        this.ownsContext = ownsContext;
        this.ownsInputFiles = ownsInputFiles;
    }

    /// Open a single Parquet file with a dedicated context.
    ///
    /// Calls [InputFile#open()] and takes ownership of the file; it is
    /// closed when this reader is closed.
    public static ParquetFileReader open(InputFile inputFile) throws IOException {
        return openAll(List.of(inputFile));
    }

    /// Open a single Parquet file with a shared context.
    ///
    /// Calls [InputFile#open()] and takes ownership of the file; it is
    /// closed when this reader is closed. The caller retains ownership of
    /// the context.
    public static ParquetFileReader open(InputFile inputFile, HardwoodContext context) throws IOException {
        return openAll(List.of(inputFile), context);
    }

    /// Open multiple Parquet files with a dedicated context. The schema
    /// is read from the first file and is assumed to be common across all
    /// files. Files are opened on demand by the iterator; the first file is
    /// opened eagerly so any I/O or metadata error surfaces immediately.
    public static ParquetFileReader openAll(List<InputFile> inputFiles) throws IOException {
        return openInternal(inputFiles, HardwoodContextImpl.create(), true);
    }

    /// Open multiple Parquet files with a shared context.
    public static ParquetFileReader openAll(List<InputFile> inputFiles, HardwoodContext context) throws IOException {
        return openInternal(inputFiles, (HardwoodContextImpl) context, false);
    }

    private static ParquetFileReader openInternal(List<InputFile> inputFiles, HardwoodContextImpl context,
                                                   boolean ownsContext) throws IOException {
        if (inputFiles == null || inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        List<InputFile> files = List.copyOf(inputFiles);
        InputFile first = files.get(0);
        first.open();
        try {
            FileMetaData firstFileMetaData;
            try {
                firstFileMetaData = ParquetMetadataReader.readMetadata(first);
            }
            catch (RuntimeException e) {
                // Thrift parsing throws RuntimeExceptions (e.g. ThriftEnumLookup for
                // corrupt enum values) that escape the IOException-only contract of
                // readMetadata — enrich them with file context so they're attributable.
                throw ExceptionContext.addFileContext(first.name(), e);
            }
            FileSchema schema = FileSchema.fromSchemaElements(firstFileMetaData.schema());

            FileOpenedEvent fileOpenedEvent = new FileOpenedEvent();
            fileOpenedEvent.begin();
            fileOpenedEvent.file = first.name();
            fileOpenedEvent.fileSize = first.length();
            fileOpenedEvent.rowGroupCount = firstFileMetaData.rowGroups().size();
            fileOpenedEvent.columnCount = schema.getColumnCount();
            fileOpenedEvent.commit();

            return new ParquetFileReader(files, firstFileMetaData, schema, context, ownsContext, true);
        }
        catch (Exception e) {
            try {
                first.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    /// File metadata of the first input file. For multi-file readers,
    /// per-file metadata for files beyond the first is not exposed; open
    /// those files individually to inspect their metadata.
    public FileMetaData getFileMetaData() {
        return firstFileMetaData;
    }

    public FileSchema getFileSchema() {
        return schema;
    }

    /// `true` when this reader was opened over more than one input file.
    public boolean isMultiFile() {
        return inputFiles.size() > 1;
    }

    // ============================================================
    // RowReader
    // ============================================================

    /// Shortcut for [#buildRowReader()].build() — read every row of every
    /// column with no filter.
    public RowReader rowReader() {
        return buildRowReader().build();
    }

    /// Begin configuring a [RowReader] with optional projection, filter,
    /// and head/tail limit.
    public RowReaderBuilder buildRowReader() {
        return new RowReaderBuilder(this);
    }

    // ============================================================
    // ColumnReader (single column, single-file)
    // ============================================================

    /// Shortcut for [#buildColumnReader(String)].build() — read every row
    /// group of the named column with no filter. Single-file only.
    public ColumnReader columnReader(String columnName) {
        return buildColumnReader(columnName).build();
    }

    /// Shortcut for [#buildColumnReader(int)].build() — read every row
    /// group of the column at the given index with no filter. Single-file
    /// only.
    public ColumnReader columnReader(int columnIndex) {
        return buildColumnReader(columnIndex).build();
    }

    /// Begin configuring a single-column [ColumnReader]. Single-file only.
    public ColumnReaderBuilder buildColumnReader(String columnName) {
        return new ColumnReaderBuilder(this, columnName);
    }

    /// Begin configuring a single-column [ColumnReader] by column index.
    /// Single-file only.
    public ColumnReaderBuilder buildColumnReader(int columnIndex) {
        return new ColumnReaderBuilder(this, columnIndex);
    }

    // ============================================================
    // ColumnReaders (projection)
    // ============================================================

    /// Shortcut for [#buildColumnReaders(ColumnProjection)].build() —
    /// every row group, no filter. Works for single- and multi-file.
    public ColumnReaders columnReaders(ColumnProjection projection) {
        return buildColumnReaders(projection).build();
    }

    /// Begin configuring a [ColumnReaders] collection for batch-oriented
    /// access to a column projection. Works for single- and multi-file.
    public ColumnReadersBuilder buildColumnReaders(ColumnProjection projection) {
        return new ColumnReadersBuilder(this, projection);
    }

    // ============================================================
    // Internal builder bridges
    // ============================================================

    RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter, long maxRows) {
        return buildRowReader(projection, filter, maxRows, 0L);
    }

    RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter, long maxRows,
                             long firstRow) {
        if (firstRow == 0L) {
            return buildRowReader(projection, filter, maxRows, firstFileMetaData.rowGroups());
        }
        // Locate the row group containing `firstRow` by walking cumulative
        // RowGroup.numRows(). For multi-file readers this indexes into the
        // first file only — cross-file firstRow is out of scope.
        List<RowGroup> all = firstFileMetaData.rowGroups();
        long cumulative = 0L;
        int targetRg = all.size(); // sentinel: past-the-end yields empty reader
        long withinRg = 0L;
        for (int i = 0; i < all.size(); i++) {
            long rgRows = all.get(i).numRows();
            if (firstRow < cumulative + rgRows) {
                targetRg = i;
                withinRg = firstRow - cumulative;
                break;
            }
            cumulative += rgRows;
        }
        if (targetRg >= all.size()) {
            // firstRow at or past end of file — return an empty reader.
            return buildRowReader(projection, filter, maxRows, List.<RowGroup>of());
        }
        List<RowGroup> rowGroups = targetRg == 0
                ? all
                : all.subList(targetRg, all.size());
        // The reader yields from the start of `targetRg`. We bump maxRows
        // by the within-RG skip distance because head(N) bounds *yielded*
        // rows; the residue rows we discard via next() count too.
        long maxRowsAdjusted = maxRows == 0 ? 0 : maxRows + withinRg;
        RowReader reader = buildRowReader(projection, filter, maxRowsAdjusted, rowGroups);
        for (long i = 0; i < withinRg; i++) {
            if (!reader.hasNext()) {
                break;
            }
            reader.next();
        }
        return reader;
    }

    RowReader buildTailRowReader(ColumnProjection projection, long tailRows) {
        if (isMultiFile()) {
            throw new UnsupportedOperationException(
                    "Tail reading is not yet supported for multi-file readers");
        }
        List<RowGroup> subset = tailRowGroups(firstFileMetaData.rowGroups(), tailRows);
        long rowsInSubset = 0;
        for (RowGroup rg : subset) {
            rowsInSubset += rg.numRows();
        }
        long skip = Math.max(0, rowsInSubset - tailRows);

        RowReader reader = buildRowReader(projection, null, 0, subset);
        for (long i = 0; i < skip; i++) {
            if (!reader.hasNext()) {
                break;
            }
            reader.next();
        }
        return reader;
    }

    private RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter,
                                     long maxRows, List<RowGroup> firstFileRowGroups) {
        ResolvedPredicate resolved = filter != null
                ? FilterPredicateResolver.resolve(filter, schema) : null;

        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);

        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, maxRows);
        iterator.setFirstFile(schema, firstFileRowGroups);
        iterator.initialize(projectedSchema, resolved);
        rowGroupIterators.add(iterator);

        return RowReader.create(iterator, schema, projectedSchema, context, resolved, maxRows);
    }

    ColumnReader buildColumnReader(String columnName, FilterPredicate filter) {
        ensureSingleFile("columnReader(String)");
        InputFile inputFile = inputFiles.get(0);
        if (filter == null) {
            return ColumnReader.create(columnName, schema, inputFile, firstFileMetaData.rowGroups(), context);
        }
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return ColumnReader.create(columnName, schema, inputFile, filterRowGroups(resolved), context, resolved);
    }

    ColumnReader buildColumnReader(int columnIndex, FilterPredicate filter) {
        ensureSingleFile("columnReader(int)");
        InputFile inputFile = inputFiles.get(0);
        if (filter == null) {
            return ColumnReader.create(columnIndex, schema, inputFile, firstFileMetaData.rowGroups(), context);
        }
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return ColumnReader.create(columnIndex, schema, inputFile, filterRowGroups(resolved), context, resolved);
    }

    ColumnReaders buildColumnReaders(ColumnProjection projection, FilterPredicate filter) {
        ResolvedPredicate resolved = filter != null
                ? FilterPredicateResolver.resolve(filter, schema) : null;

        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, 0);
        iterator.setFirstFile(schema, firstFileMetaData.rowGroups());
        ProjectedSchema projected = iterator.initialize(projection, resolved);
        rowGroupIterators.add(iterator);
        return new ColumnReaders(context, iterator, schema, projected);
    }

    private void ensureSingleFile(String op) {
        if (isMultiFile()) {
            throw new UnsupportedOperationException(
                    op + " is single-file only; use columnReaders(projection) for multi-file readers");
        }
    }

    private static List<RowGroup> tailRowGroups(List<RowGroup> rowGroups, long tailRows) {
        int startIndex = rowGroups.size();
        long accumulated = 0;
        for (int i = rowGroups.size() - 1; i >= 0; i--) {
            accumulated += rowGroups.get(i).numRows();
            startIndex = i;
            if (accumulated >= tailRows) {
                break;
            }
        }
        return rowGroups.subList(startIndex, rowGroups.size());
    }

    private List<RowGroup> filterRowGroups(ResolvedPredicate filter) {
        List<RowGroup> all = firstFileMetaData.rowGroups();
        List<RowGroup> kept = all.stream()
                .filter(rg -> !RowGroupFilterEvaluator.canDropRowGroup(filter, rg))
                .toList();

        RowGroupFilterEvent event = new RowGroupFilterEvent();
        event.file = inputFiles.get(0).name();
        event.totalRowGroups = all.size();
        event.rowGroupsKept = kept.size();
        event.rowGroupsSkipped = all.size() - kept.size();
        event.commit();

        return kept;
    }

    // ============================================================
    // Nested builders
    // ============================================================

    /// Builds a [RowReader] with optional projection, filter, and head/tail
    /// row limit.
    ///
    /// Obtained from [ParquetFileReader#buildRowReader()]. Each setter returns
    /// the builder for chaining; [#build()] consumes the configuration and
    /// creates the reader. The builder is not reusable after `build()`.
    ///
    /// ```java
    /// RowReader reader = file.buildRowReader()
    ///         .projection(ColumnProjection.columns("id", "name"))
    ///         .filter(FilterPredicate.eq("status", "active"))
    ///         .head(1000)
    ///         .build();
    /// ```
    public static final class RowReaderBuilder {

        private final ParquetFileReader fileReader;
        private ColumnProjection projection = ColumnProjection.all();
        private FilterPredicate filter;
        /// Positive: return at most `headRows` rows from the start.
        /// Zero (default): no limit.
        private long headRows;
        /// Positive: return at most `tailRows` rows from the end.
        /// Zero (default): no limit. Mutually exclusive with `headRows`.
        private long tailRows;
        /// Zero (default): start from row 0. Positive: skip rows before the
        /// given absolute row index, opening only the row group(s) needed
        /// to reach it. Mutually exclusive with `tailRows`.
        private long firstRow;

        private RowReaderBuilder(ParquetFileReader fileReader) {
            this.fileReader = fileReader;
        }

        /// Restrict reading to the given columns. Default: all columns.
        public RowReaderBuilder projection(ColumnProjection projection) {
            if (projection == null) {
                throw new IllegalArgumentException("projection must not be null");
            }
            this.projection = projection;
            return this;
        }

        /// Apply a row-group / record-level filter predicate. Default: no filter.
        public RowReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Limit to the first `maxRows` rows. Mutually exclusive with [#tail].
        public RowReaderBuilder head(long maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("head row count must be positive: " + maxRows);
            }
            this.headRows = maxRows;
            return this;
        }

        /// Limit to the last `tailRows` rows. Row groups that do not overlap
        /// the tail are skipped entirely, so pages for earlier row groups are
        /// never fetched or decoded — useful on remote backends. Mutually
        /// exclusive with [#head], [#filter], and [#firstRow]. Single-file only.
        public RowReaderBuilder tail(long tailRows) {
            if (tailRows <= 0) {
                throw new IllegalArgumentException("tail row count must be positive: " + tailRows);
            }
            this.tailRows = tailRows;
            return this;
        }

        /// Begin reading from the given absolute row index. Earlier row groups
        /// are not opened — their pages are not fetched or decoded — so this
        /// is an O(1 RG) seek on remote backends, in contrast to walking
        /// next() from row 0.
        ///
        /// On files with an OffsetIndex, the underlying [IndexedFetchPlan]
        /// further narrows the per-column fetch to pages from the page
        /// containing `firstRow` onwards. Files without an OffsetIndex fall
        /// back to walking the row group from its start (skipped rows are
        /// decoded internally and not yielded to the caller).
        ///
        /// `firstRow == 0` is the no-op default. `firstRow == totalRows`
        /// produces an empty reader. Indexes into the *first* file's rows for
        /// multi-file readers; cross-file `firstRow` is out of scope. Mutually
        /// exclusive with [#tail]; composes with [#head] for a bounded
        /// `[firstRow, firstRow + maxRows)` window.
        public RowReaderBuilder firstRow(long firstRow) {
            if (firstRow < 0) {
                throw new IllegalArgumentException("firstRow must be non-negative: " + firstRow);
            }
            this.firstRow = firstRow;
            return this;
        }

        public RowReader build() {
            if (headRows > 0 && tailRows > 0) {
                throw new IllegalArgumentException("head and tail are mutually exclusive");
            }
            if (tailRows > 0 && filter != null) {
                throw new IllegalArgumentException("tail cannot be combined with a filter: "
                        + "the set of matching rows is not known from row-group statistics alone");
            }
            if (tailRows > 0 && firstRow > 0) {
                throw new IllegalArgumentException("tail and firstRow are mutually exclusive");
            }
            if (tailRows > 0) {
                return fileReader.buildTailRowReader(projection, tailRows);
            }
            return fileReader.buildRowReader(projection, filter, headRows, firstRow);
        }
    }

    /// Builds a single-column [ColumnReader] with an optional filter.
    ///
    /// Obtained from [ParquetFileReader#buildColumnReader(String)] or
    /// [ParquetFileReader#buildColumnReader(int)]. Single-file only —
    /// multi-file readers must use [ParquetFileReader#buildColumnReaders]
    /// with a projection.
    ///
    /// ```java
    /// ColumnReader col = file.buildColumnReader("id")
    ///         .filter(FilterPredicate.lt("id", 1000L))
    ///         .build();
    /// ```
    public static final class ColumnReaderBuilder {

        private final ParquetFileReader fileReader;
        private final String columnName;
        private final int columnIndex;
        private final boolean byName;
        private FilterPredicate filter;

        private ColumnReaderBuilder(ParquetFileReader fileReader, String columnName) {
            this.fileReader = fileReader;
            this.columnName = columnName;
            this.columnIndex = -1;
            this.byName = true;
        }

        private ColumnReaderBuilder(ParquetFileReader fileReader, int columnIndex) {
            this.fileReader = fileReader;
            this.columnName = null;
            this.columnIndex = columnIndex;
            this.byName = false;
        }

        /// Apply a row-group / page-level filter predicate. Default: no filter.
        public ColumnReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        public ColumnReader build() {
            if (byName) {
                return fileReader.buildColumnReader(columnName, filter);
            }
            return fileReader.buildColumnReader(columnIndex, filter);
        }
    }

    /// Builds a [ColumnReaders] collection for batch-oriented access to a
    /// projection of columns.
    ///
    /// Obtained from [ParquetFileReader#buildColumnReaders(ColumnProjection)].
    /// Works for both single- and multi-file readers; the underlying iterator
    /// handles cross-file prefetch transparently.
    ///
    /// ```java
    /// try (ColumnReaders cols = file.buildColumnReaders(ColumnProjection.columns("a", "b"))
    ///         .filter(FilterPredicate.eq("a", 7))
    ///         .build()) {
    ///     ColumnReader a = cols.getColumnReader("a");
    ///     // ...
    /// }
    /// ```
    public static final class ColumnReadersBuilder {

        private final ParquetFileReader fileReader;
        private final ColumnProjection projection;
        private FilterPredicate filter;

        private ColumnReadersBuilder(ParquetFileReader fileReader, ColumnProjection projection) {
            if (projection == null) {
                throw new IllegalArgumentException("projection must not be null");
            }
            this.fileReader = fileReader;
            this.projection = projection;
        }

        /// Apply a row-group statistics filter. Default: no filter.
        public ColumnReadersBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        public ColumnReaders build() {
            return fileReader.buildColumnReaders(projection, filter);
        }
    }

    @Override
    public void close() throws IOException {
        for (RowGroupIterator iterator : rowGroupIterators) {
            iterator.close();
        }
        rowGroupIterators.clear();

        if (ownsContext) {
            context.close();
        }

        if (ownsInputFiles) {
            IOException firstFailure = null;
            for (InputFile file : inputFiles) {
                try {
                    file.close();
                }
                catch (IOException e) {
                    if (firstFailure == null) {
                        firstFailure = e;
                    }
                    else {
                        firstFailure.addSuppressed(e);
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
        }
    }
}
