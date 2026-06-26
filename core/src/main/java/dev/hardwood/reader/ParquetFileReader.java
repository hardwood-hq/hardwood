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
import java.util.LinkedHashSet;
import java.util.List;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.BatchSizing;
import dev.hardwood.internal.reader.FlatRowReader;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.NestedRowReader;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.jfr.RowGroupByteRangeFilterEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

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
/// **Limitation:** When using the default memory-mapped [InputFile], the file
/// itself may be arbitrarily large, but each individual column chunk must be at
/// most 2 GB ([Integer#MAX_VALUE] bytes) of compressed data. The in-memory and
/// object-store backends have a 2 GB limit on the whole file.
public class ParquetFileReader implements AutoCloseable {

    /// Sentinel used by the column-reader builders to mean "no explicit batch
    /// size set": the size is then resolved from the projected column widths via
    /// [BatchSizing#computeOptimalBatchSize(dev.hardwood.internal.schema.ProjectedSchema)]
    /// at build time. Never reaches the workers — [#resolveBatchSize] turns it
    /// into a concrete positive count.
    private static final int AUTO_BATCH_SIZE = 0;

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
    public static ParquetFileReader openAll(List<? extends InputFile> inputFiles) throws IOException {
        return openInternal(inputFiles, HardwoodContextImpl.create(), true);
    }

    /// Open multiple Parquet files with a shared context.
    public static ParquetFileReader openAll(List<? extends InputFile> inputFiles, HardwoodContext context) throws IOException {
        return openInternal(inputFiles, (HardwoodContextImpl) context, false);
    }

    private static ParquetFileReader openInternal(List<? extends InputFile> inputFiles, HardwoodContextImpl context,
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
        return buildRowReader(projection, filter, null, maxRows, 0L);
    }

    RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter,
                             RowGroupPredicate rowGroupFilter, long maxRows, long skip) {
        // Apply the row-group predicate (e.g. byte-range) up front so `skip` indexes
        // into the kept sequence — a caller doing split-aware reading can seek inside *its*
        // split. Stats-based row-group dropping (via FilterPredicate) stays inside the
        // RowGroupIterator.
        List<RowGroup> filteredRowGroups = filterRowGroups(rowGroupFilter);

        if (skip == 0L) {
            return buildRowReader(projection, filter, maxRows, filteredRowGroups);
        }

        if (filter != null) {
            // Logical OFFSET over the matched relation (SQL OFFSET), symmetric with
            // head() being LIMIT (#538). Row-group statistics bound values, not match
            // counts, so we cannot seek by physical row position: build over the full
            // (row-group-filtered) relation, cap the underlying reader at `skip + head`
            // matched rows, then discard the first `skip` matches. A `skip` past the end
            // of the matched relation simply yields an exhausted (empty) reader.
            long matchedCap = maxRows == 0 ? 0L : Math.addExact(maxRows, skip);
            RowReader reader = buildRowReader(projection, filter, matchedCap, filteredRowGroups);
            return discardLeadingRows(reader, skip);
        }

        RowReader reader = buildRowReader(projection, filter, maxRows, filteredRowGroups, 0L, skip);
        long firstRowGroupSkip = rowGroupIterators.get(rowGroupIterators.size() - 1).firstRowGroupSkip();
        return discardLeadingRows(reader, firstRowGroupSkip);
    }

    /// Advances `reader` past its first `count` rows via `next()` and returns it,
    /// positioned at the first row the caller should see. Used to apply both the
    /// physical within-row-group residue (no-filter `skip`) and the logical `OFFSET`
    /// (filtered `skip`). Stops early if the reader is exhausted before `count`. If
    /// iteration fails, the partially built reader is closed before the error
    /// propagates, so its worker pipeline never leaks.
    private static RowReader discardLeadingRows(RowReader reader, long count) {
        try {
            for (long i = 0; i < count; i++) {
                if (!reader.hasNext()) {
                    break;
                }
                reader.next();
            }
            return reader;
        }
        catch (RuntimeException e) {
            try {
                reader.close();
            }
            catch (RuntimeException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
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

        // Build the iterator first (with tailSkip=0); its
        // SharedRowGroupMetadata cache is what surfaces the gate decision.
        // Probing through the iterator avoids the prior duplicate probe
        // where canFastSkipTail and computeFetchPlans both ran the same
        // page-format check per row group.
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection, true);
        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, 0L, 0L);
        iterator.setFirstFile(schema, subset);
        iterator.initialize(projectedSchema, null);
        rowGroupIterators.add(iterator);

        boolean fastSkip = (skip == 0) || iterator.canFastSkipAllRowGroups();
        if (fastSkip && skip > 0) {
            iterator.setTailSkip(skip);
        }

        RowReader reader = createRowReader(iterator, schema, projectedSchema, context, null, 0);

        if (!fastSkip) {
            // Fallback: at least one projected column closes the per-page
            // mask gate (a nested-v1 column without an OffsetIndex). Decode
            // every leading row and discard it to preserve cross-column
            // row alignment.
            return discardLeadingRows(reader, skip);
        }
        return reader;
    }

    private RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter,
                                     long maxRows, List<RowGroup> firstFileRowGroups) {
        return buildRowReader(projection, filter, maxRows, firstFileRowGroups, 0L);
    }

    private RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter,
                                     long maxRows, List<RowGroup> firstFileRowGroups,
                                     long tailSkip) {
        return buildRowReader(projection, filter, maxRows, firstFileRowGroups, tailSkip, 0L);
    }

    private RowReader buildRowReader(ColumnProjection projection, FilterPredicate filter,
                                     long maxRows, List<RowGroup> firstFileRowGroups,
                                     long tailSkip, long physicalSkip) {
        ResolvedPredicate resolved = filter != null
                ? FilterPredicateResolver.resolve(filter, schema, firstFileMetaData.columnOrders()) : null;

        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection, true);

        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, maxRows, tailSkip, physicalSkip);
        iterator.setFirstFile(schema, firstFileRowGroups);
        iterator.initialize(projectedSchema, resolved);
        rowGroupIterators.add(iterator);

        long readerMaxRows = maxRows == 0 ? 0 : Math.addExact(maxRows, iterator.firstRowGroupSkip());
        return createRowReader(iterator, schema, projectedSchema, context, resolved, readerMaxRows);
    }

    /// Creates a [RowReader] for the given pipeline components.
    ///
    /// Selects [dev.hardwood.internal.reader.FlatRowReader] for flat schemas and
    /// [dev.hardwood.internal.reader.NestedRowReader] for nested schemas.
    /// Wraps with [dev.hardwood.internal.reader.FilteredRowReader] when a filter is present.
    ///
    /// @param rowGroupIterator initialized iterator over row groups
    /// @param schema file schema
    /// @param projectedSchema column projection
    /// @param context hardwood context
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited)
    private RowReader createRowReader (RowGroupIterator rowGroupIterator,
                            FileSchema schema,
                            ProjectedSchema projectedSchema,
                            HardwoodContextImpl context,
                            ResolvedPredicate filter,
                            long maxRows) {
        if (schema.isFlatSchema()) {
            return FlatRowReader.create(rowGroupIterator, schema, projectedSchema, context, filter, maxRows);
        }
        else {
            return NestedRowReader.create(rowGroupIterator, schema, projectedSchema, context, filter, maxRows);
        }
    }

    ColumnReader buildColumnReader(String columnName, FilterPredicate filter) {
        return buildColumnReader(columnName, filter, null, AUTO_BATCH_SIZE);
    }

    ColumnReader buildColumnReader(
            String columnName, FilterPredicate filter, RowGroupPredicate rowGroupFilter, int batchSize) {
        ensureSingleFile("columnReader(String)");
        if (filter != null) {
            // Exact filtering routes through the shared filtered-projection
            // engine and exposes the single requested column.
            return buildColumnReaders(ColumnProjection.columns(columnName), filter, rowGroupFilter, batchSize)
                    .getColumnReader(0);
        }
        InputFile inputFile = inputFiles.get(0);
        List<RowGroup> rowGroups = filterRowGroups(rowGroupFilter);
        return ColumnReader.create(columnName, schema, inputFile, rowGroups, context, null, batchSize);
    }

    ColumnReader buildColumnReader(int columnIndex, FilterPredicate filter) {
        return buildColumnReader(columnIndex, filter, null, AUTO_BATCH_SIZE);
    }

    ColumnReader buildColumnReader(
            int columnIndex, FilterPredicate filter, RowGroupPredicate rowGroupFilter, int batchSize) {
        ensureSingleFile("columnReader(int)");
        if (filter != null) {
            String columnName = schema.getColumn(columnIndex).fieldPath().toString();
            return buildColumnReaders(ColumnProjection.columns(columnName), filter, rowGroupFilter, batchSize)
                    .getColumnReader(0);
        }
        InputFile inputFile = inputFiles.get(0);
        List<RowGroup> rowGroups = filterRowGroups(rowGroupFilter);
        return ColumnReader.create(columnIndex, schema, inputFile, rowGroups, context, null, batchSize);
    }

    ColumnReaders buildColumnReaders(ColumnProjection projection, FilterPredicate filter) {
        return buildColumnReaders(projection, filter, null, AUTO_BATCH_SIZE);
    }

    ColumnReaders buildColumnReaders(
            ColumnProjection projection,
            FilterPredicate filter,
            RowGroupPredicate rowGroupFilter,
            int batchSize) {
        ResolvedPredicate resolved = filter != null
                ? FilterPredicateResolver.resolve(filter, schema, firstFileMetaData.columnOrders()) : null;
        List<RowGroup> rowGroups = filterRowGroups(rowGroupFilter);

        if (resolved == null) {
            RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, 0);
            iterator.setFirstFile(schema, rowGroups);
            ProjectedSchema projected = iterator.initialize(projection, null);
            rowGroupIterators.add(iterator);
            return new ColumnReaders(context, iterator, schema, projected,
                    resolveBatchSize(batchSize, projected));
        }

        // Exact filtering (#624): decode the payload columns *and* the predicate
        // columns through one shared iterator (single iterator ⇒ all columns
        // stay row-aligned regardless of per-column page-skip capability), then
        // compact each exposed column to the matching records per batch.
        ColumnProjection augmented = augmentWithPredicateColumns(projection, resolved);
        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, 0);
        iterator.setFirstFile(schema, rowGroups);
        ProjectedSchema augProjected = iterator.initialize(augmented, resolved);
        ProjectedSchema payloadProjected = ProjectedSchema.create(schema, projection);
        rowGroupIterators.add(iterator);
        // Size against the augmented projection — the predicate columns allocate
        // per-batch arrays too, so they count toward the byte budget.
        return ColumnReaders.filtered(
                context, iterator, schema, augProjected, payloadProjected, resolved,
                resolveBatchSize(batchSize, augProjected));
    }

    /// Resolves a requested batch size to a concrete record count. A positive
    /// `requested` (set explicitly via the builders' `batchSize(int)`) is used
    /// verbatim; the [#AUTO_BATCH_SIZE] sentinel is turned into a byte-budgeted
    /// size derived from the projected column widths, the same logic the
    /// `RowReader` path uses, so both regimes agree.
    private static int resolveBatchSize(int requested, ProjectedSchema projected) {
        return requested > 0 ? requested : BatchSizing.computeOptimalBatchSize(projected);
    }

    /// Builds the union of `projection` and the predicate's leaf columns so the
    /// predicate columns are decoded even when the caller did not project them.
    /// A `projectsAll()` projection already covers them.
    private ColumnProjection augmentWithPredicateColumns(
            ColumnProjection projection, ResolvedPredicate resolved) {
        if (projection.projectsAll()) {
            return projection;
        }
        LinkedHashSet<String> names = new LinkedHashSet<>(
                projection.getProjectedColumnNames());
        names.addAll(SelectionEngine.predicateColumnPaths(resolved, schema));
        return ColumnProjection.columns(names.toArray(new String[0]));
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

    /// Pre-filter row groups by an optional byte-range [RowGroupPredicate]. Statistics-based
    /// dropping (via a [FilterPredicate]) is not applied here — it stays inside the
    /// [RowGroupIterator], which applies it per file. Returns all row groups unchanged when no
    /// `rowGroupFilter` is given.
    ///
    /// Doing the byte-range check here, before the iterator's statistics evaluation, preserves
    /// the cheap-first ordering: the byte-range predicate is both cheaper (a midpoint compare)
    /// and more selective (one shard out of N) in split-aware reads, so the rejected majority of
    /// row groups never reach the more expensive statistics check downstream.
    private List<RowGroup> filterRowGroups(RowGroupPredicate rowGroupFilter) {
        List<RowGroup> all = firstFileMetaData.rowGroups();
        if (rowGroupFilter == null) {
            return all;
        }
        List<RowGroup> kept = all.stream()
                .filter(rg -> matches(rg, rowGroupFilter))
                .toList();

        RowGroupByteRangeFilterEvent event = new RowGroupByteRangeFilterEvent();
        event.file = inputFiles.get(0).name();
        event.totalRowGroups = all.size();
        event.rowGroupsKept = kept.size();
        event.rowGroupsSkipped = all.size() - kept.size();
        event.commit();

        return kept;
    }

    /// Evaluate a [RowGroupPredicate] against one row group.
    private static boolean matches(RowGroup rg, RowGroupPredicate p) {
        return switch (p) {
            case RowGroupPredicate.ByteRange b -> {
                long mid = rowGroupMidpoint(rg);
                yield mid >= b.startInclusive() && mid < b.endExclusive();
            }
            case RowGroupPredicate.And a -> {
                for (RowGroupPredicate child : a.children()) {
                    if (!matches(rg, child)) {
                        yield false;
                    }
                }
                yield true;
            }
        };
    }

    private static long rowGroupMidpoint(RowGroup rg) {
        List<dev.hardwood.metadata.ColumnChunk> columns = rg.columns();
        long start = columns.get(0).chunkStartOffset();
        long compressed = 0;
        for (dev.hardwood.metadata.ColumnChunk chunk : columns) {
            compressed += chunk.metaData().totalCompressedSize();
        }
        return start + compressed / 2;
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
        private RowGroupPredicate rowGroupFilter;
        /// Positive: return at most `headRows` rows from the start.
        /// Zero (default): no limit.
        private long headRows;
        /// Positive: return at most `tailRows` rows from the end.
        /// Zero (default): no limit. Mutually exclusive with `headRows`.
        private long tailRows;
        /// Zero (default): start from row 0. Positive: SQL `OFFSET` — a physical
        /// absolute row index without a filter, or a logical offset over matched
        /// rows with one. Mutually exclusive with `tailRows`.
        private long skip;

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

        /// Apply a column-statistics / record-level filter predicate. Default: no filter.
        public RowReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Apply a row-group selection predicate (e.g. byte-range, for split-aware reading).
        /// Default: read every row group. Combines with [#filter(FilterPredicate)] via
        /// intersection: a row group is read if and only if it passes both.
        ///
        /// Composes with [#head(long)] and [#skip(long)] over the *filtered* row-group
        /// sequence — `skip(N)` skips `N` rows of the kept set, `head(N)` caps at `N`
        /// rows of the kept set. Mutually exclusive with [#tail(long)] (tail mode requires
        /// a known total row count, which row-group filtering invalidates).
        public RowReaderBuilder filter(RowGroupPredicate rowGroupFilter) {
            this.rowGroupFilter = rowGroupFilter;
            return this;
        }

        /// Limit to the first `maxRows` rows. When combined with [#filter(FilterPredicate)],
        /// the cap is on the number of *matching* rows, not the number scanned — the
        /// reader keeps scanning until `maxRows` rows satisfy the predicate or the input
        /// is exhausted (SQL `LIMIT` over the filtered relation). Mutually exclusive with
        /// [#tail].
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
        /// exclusive with [#head], [#filter], and [#skip]. Single-file only.
        public RowReaderBuilder tail(long tailRows) {
            if (tailRows <= 0) {
                throw new IllegalArgumentException("tail row count must be positive: " + tailRows);
            }
            this.tailRows = tailRows;
            return this;
        }

        /// Skip leading rows before reading — SQL `OFFSET`. Its meaning depends on
        /// whether a [#filter(FilterPredicate)] is present:
        ///
        /// - **Without a filter:** a physical absolute row index over the concatenated
        ///   input relation. Earlier row groups are not opened — an O(1 row-group)
        ///   seek on remote backends (the leading residue within the target row group
        ///   is still decoded). For a multi-file reader, footers of skipped files are
        ///   read until the target file is found, but skipped files' data pages are
        ///   not fetched or decoded. `skip >= totalRows` yields an empty reader.
        /// - **With a filter:** a *logical* offset over the matched rows — discards the
        ///   first `n` rows matching the predicate, symmetric with [#head] as `LIMIT`.
        ///   The O(1) seek does not apply: the reader decodes earlier groups to count
        ///   matches (groups proven non-matching by statistics are still pruned). A
        ///   `skip` past the match count yields an empty reader, counting across *all*
        ///   files in order.
        ///
        /// `skip == 0` is the no-op default. Mutually exclusive with [#tail]; composes
        /// with [#head] (`skip(n).head(k)` is `OFFSET n LIMIT k`) and with
        /// [#filter(RowGroupPredicate)] over the kept row-group sequence.
        public RowReaderBuilder skip(long skip) {
            if (skip < 0) {
                throw new IllegalArgumentException("skip must be non-negative: " + skip);
            }
            this.skip = skip;
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
            if (tailRows > 0 && rowGroupFilter != null) {
                throw new IllegalArgumentException(
                        "tail cannot be combined with a row-group filter: "
                                + "tail mode requires a known total row count, which row-group "
                                + "filtering invalidates");
            }
            if (tailRows > 0 && skip > 0) {
                throw new IllegalArgumentException("tail and skip are mutually exclusive");
            }
            if (tailRows > 0) {
                return fileReader.buildTailRowReader(projection, tailRows);
            }
            return fileReader.buildRowReader(
                    projection, filter, rowGroupFilter, headRows, skip);
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
        private RowGroupPredicate rowGroupFilter;
        private int batchSize = AUTO_BATCH_SIZE;

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

        /// Apply a filter predicate. The built reader returns **only** the rows
        /// matching `filter` — exact, with no client-side residual: a direct
        /// aggregate over the output is correct. Row groups and pages proven
        /// non-matching by statistics are skipped; the surviving rows are then
        /// filtered exactly. The predicate may reference this column, another
        /// column, or a column that is not otherwise read. Default: no filter.
        public ColumnReaderBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Apply a row-group selection predicate (e.g. byte-range, for split-aware reading).
        /// Default: read every row group. Combines with [#filter(FilterPredicate)] via
        /// intersection: a row group is read if and only if it passes both.
        public ColumnReaderBuilder filter(RowGroupPredicate rowGroupFilter) {
            this.rowGroupFilter = rowGroupFilter;
            return this;
        }

        /// Set the maximum number of records to return in each batch.
        ///
        /// When unset, the batch size is chosen adaptively from the column's
        /// physical width so the per-batch arrays stay within the CPU cache —
        /// the same byte-budgeted sizing the [RowReader] path uses — rather
        /// than a fixed record count. Set this explicitly to override.
        public ColumnReaderBuilder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
            }
            this.batchSize = batchSize;
            return this;
        }

        public ColumnReader build() {
            if (byName) {
                return fileReader.buildColumnReader(columnName, filter, rowGroupFilter, batchSize);
            }
            return fileReader.buildColumnReader(columnIndex, filter, rowGroupFilter, batchSize);
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
        private RowGroupPredicate rowGroupFilter;
        private int batchSize = AUTO_BATCH_SIZE;

        private ColumnReadersBuilder(ParquetFileReader fileReader, ColumnProjection projection) {
            if (projection == null) {
                throw new IllegalArgumentException("projection must not be null");
            }
            this.fileReader = fileReader;
            this.projection = projection;
        }

        /// Apply a filter predicate. Every column in the projection returns
        /// **only** the rows matching `filter` — exact, row-aligned across
        /// columns, with no client-side residual. Row groups and pages proven
        /// non-matching by statistics are skipped; the surviving rows are then
        /// filtered exactly. The predicate may reference a projected column or a
        /// column that is not part of the projection. Default: no filter.
        public ColumnReadersBuilder filter(FilterPredicate filter) {
            this.filter = filter;
            return this;
        }

        /// Apply a row-group selection predicate (e.g. byte-range, for split-aware reading).
        /// Default: read every row group. Combines with [#filter(FilterPredicate)] via
        /// intersection: a row group is read if and only if it passes both.
        public ColumnReadersBuilder filter(RowGroupPredicate rowGroupFilter) {
            this.rowGroupFilter = rowGroupFilter;
            return this;
        }

        /// Set the maximum number of records to return in each batch for all columns.
        ///
        /// When unset, the batch size is chosen adaptively from the projected
        /// columns' physical widths so the per-batch arrays stay within the CPU
        /// cache — the same byte-budgeted sizing the [RowReader] path uses —
        /// rather than a fixed record count. Set this explicitly to override.
        public ColumnReadersBuilder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive: " + batchSize);
            }
            this.batchSize = batchSize;
            return this;
        }

        public ColumnReaders build() {
            return fileReader.buildColumnReaders(projection, filter, rowGroupFilter, batchSize);
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
