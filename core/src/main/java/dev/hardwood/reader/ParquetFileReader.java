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

/// Reader for individual Parquet files.
///
/// For single-file usage:
/// ```java
/// try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
///     RowReader rows = reader.createRowReader();
///     // ...
/// }
/// ```
///
/// For multi-file usage with shared thread pool, use [dev.hardwood.Hardwood].
///
/// **Limitation:** When using the default memory-mapped [InputFile],
/// individual files must be at most 2 GB ([Integer#MAX_VALUE] bytes).
/// Larger datasets should be split across multiple files and read via
/// [MultiFileParquetReader].
public class ParquetFileReader implements AutoCloseable {

    private final InputFile inputFile;
    private final FileMetaData fileMetaData;
    private final HardwoodContextImpl context;
    private final boolean ownsContext;
    private final boolean ownsInputFile;
    private final List<RowGroupIterator> rowGroupIterators = new ArrayList<>();

    private ParquetFileReader(InputFile inputFile, FileMetaData fileMetaData,
                              HardwoodContextImpl context, boolean ownsContext, boolean ownsInputFile) {
        this.inputFile = inputFile;
        this.fileMetaData = fileMetaData;
        this.context = context;
        this.ownsContext = ownsContext;
        this.ownsInputFile = ownsInputFile;
    }

    /// Open a Parquet file from an [InputFile] with a dedicated context.
    ///
    /// This method calls [InputFile#open()] and takes ownership of the file;
    /// it will be closed when this reader is closed.
    public static ParquetFileReader open(InputFile inputFile) throws IOException {
        inputFile.open();
        try {
            return openInternal(inputFile, HardwoodContextImpl.create(), true, true);
        }
        catch (Exception e) {
            try {
                inputFile.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    /// Open a Parquet file from an [InputFile] with a shared context.
    ///
    /// This method calls [InputFile#open()] and takes ownership of the file;
    /// it will be closed when this reader is closed. The caller retains ownership
    /// of the context.
    public static ParquetFileReader open(InputFile inputFile, HardwoodContext context) throws IOException {
        inputFile.open();
        try {
            return openInternal(inputFile, (HardwoodContextImpl) context, false, true);
        }
        catch (Exception e) {
            try {
                inputFile.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    private static ParquetFileReader openInternal(InputFile inputFile, HardwoodContextImpl context,
                                                   boolean ownsContext, boolean ownsInputFile) throws IOException {
        FileOpenedEvent fileOpenedEvent = new FileOpenedEvent();
        fileOpenedEvent.begin();

        FileMetaData fileMetaData = ParquetMetadataReader.readMetadata(inputFile);
        FileSchema fileSchema = FileSchema.fromSchemaElements(fileMetaData.schema());

        fileOpenedEvent.file = inputFile.name();
        fileOpenedEvent.fileSize = inputFile.length();
        fileOpenedEvent.rowGroupCount = fileMetaData.rowGroups().size();
        fileOpenedEvent.columnCount = fileSchema.getColumnCount();
        fileOpenedEvent.commit();

        return new ParquetFileReader(inputFile, fileMetaData, context, ownsContext, ownsInputFile);
    }

    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    public FileSchema getFileSchema() {
        return FileSchema.fromSchemaElements(fileMetaData.schema());
    }

    /// Create a ColumnReader for a named column, spanning all row groups.
    public ColumnReader createColumnReader(String columnName) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnName, schema, inputFile, fileMetaData.rowGroups(), context);
    }

    /// Create a ColumnReader for a named column, spanning only row groups that match the filter.
    ///
    /// @param columnName the column to read
    /// @param filter predicate for row group filtering based on statistics
    public ColumnReader createColumnReader(String columnName, FilterPredicate filter) {
        FileSchema schema = getFileSchema();
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return ColumnReader.create(columnName, schema, inputFile, filterRowGroups(resolved), context, resolved);
    }

    /// Create a ColumnReader for a column by index, spanning all row groups.
    public ColumnReader createColumnReader(int columnIndex) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnIndex, schema, inputFile, fileMetaData.rowGroups(), context);
    }

    /// Create a ColumnReader for a column by index, spanning only row groups that match the filter.
    ///
    /// @param columnIndex the column index to read
    /// @param filter predicate for row group filtering based on statistics
    public ColumnReader createColumnReader(int columnIndex, FilterPredicate filter) {
        FileSchema schema = getFileSchema();
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return ColumnReader.create(columnIndex, schema, inputFile, filterRowGroups(resolved), context, resolved);
    }

    /// Create a RowReader that iterates over all rows in all row groups.
    public RowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    /// Create a RowReader with a filter, iterating over all columns but only matching row groups.
    ///
    /// @param filter predicate for row group filtering based on statistics
    public RowReader createRowReader(FilterPredicate filter) {
        return createRowReader(ColumnProjection.all(), filter);
    }

    /// Create a RowReader that iterates over selected columns in all row groups.
    ///
    /// @param projection specifies which columns to read
    /// @return a RowReader for the selected columns
    public RowReader createRowReader(ColumnProjection projection) {
        return createRowReaderInternal(projection, null, 0, fileMetaData.rowGroups());
    }

    /// Create a RowReader that iterates over selected columns in only matching row groups.
    ///
    /// @param projection specifies which columns to read
    /// @param filter predicate for row group filtering based on statistics
    public RowReader createRowReader(ColumnProjection projection, FilterPredicate filter) {
        return createRowReaderInternal(projection, filter, 0, fileMetaData.rowGroups());
    }

    /// Create a RowReader that returns at most `|maxRows|` rows.
    ///
    /// The sign of `maxRows` selects the end of the file to read from:
    ///
    /// - `maxRows > 0` — **head**: return the first `maxRows` rows.
    /// - `maxRows < 0` — **tail**: return the last `-maxRows` rows. Row groups
    ///   that do not overlap the tail are skipped entirely, so pages for
    ///   earlier row groups are never fetched or decoded (useful on remote
    ///   backends like S3).
    ///
    /// Tail mode cannot currently be combined with a filter: the set of
    /// matching rows is not known from row-group statistics alone, so the
    /// reader cannot determine which row groups cover the last `-maxRows`
    /// **matching** rows without scanning the whole file.
    ///
    /// @param projection specifies which columns to read
    /// @param filter predicate for row group filtering based on statistics (must be `null` when `maxRows < 0`)
    /// @param maxRows row count with direction (must be non-zero)
    public RowReader createRowReader(ColumnProjection projection, FilterPredicate filter, long maxRows) {
        if (maxRows == 0) {
            throw new IllegalArgumentException("maxRows must be non-zero");
        }
        if (maxRows < 0) {
            if (filter != null) {
                throw new IllegalArgumentException("Tail reading (negative maxRows) cannot be combined with a filter");
            }
            return createTailRowReader(projection, -maxRows);
        }
        return createRowReaderInternal(projection, filter, maxRows, fileMetaData.rowGroups());
    }

    private RowReader createTailRowReader(ColumnProjection projection, long tailRows) {
        List<RowGroup> subset = tailRowGroups(fileMetaData.rowGroups(), tailRows);
        long rowsInSubset = 0;
        for (RowGroup rg : subset) {
            rowsInSubset += rg.numRows();
        }
        long skip = Math.max(0, rowsInSubset - tailRows);

        RowReader reader = createRowReaderInternal(projection, null, 0, subset);
        for (long i = 0; i < skip; i++) {
            if (!reader.hasNext()) {
                break;
            }
            reader.next();
        }
        return reader;
    }

    private RowReader createRowReaderInternal(ColumnProjection projection, FilterPredicate filter, long maxRows, List<RowGroup> rowGroups) {
        FileSchema schema = getFileSchema();
        ResolvedPredicate resolved = filter != null
                ? FilterPredicateResolver.resolve(filter, schema) : null;

        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);

        RowGroupIterator rowGroupIterator = new RowGroupIterator(
                List.of(inputFile), context, maxRows);
        rowGroupIterator.setFirstFile(schema, rowGroups);
        rowGroupIterator.initialize(projectedSchema, resolved);
        rowGroupIterators.add(rowGroupIterator);

        return RowReader.create(rowGroupIterator, schema, projectedSchema, context, resolved, maxRows);
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
        List<RowGroup> allRowGroups = fileMetaData.rowGroups();
        List<RowGroup> filtered = allRowGroups.stream()
                .filter(rg -> !RowGroupFilterEvaluator.canDropRowGroup(filter, rg))
                .toList();

        RowGroupFilterEvent event = new RowGroupFilterEvent();
        event.file = inputFile.name();
        event.totalRowGroups = allRowGroups.size();
        event.rowGroupsKept = filtered.size();
        event.rowGroupsSkipped = allRowGroups.size() - filtered.size();
        event.commit();

        return filtered;
    }

    @Override
    public void close() throws IOException {
        for (RowGroupIterator iterator : rowGroupIterators) {
            iterator.close();
        }
        rowGroupIterators.clear();

        // Only close context if we created it
        // When opened via Hardwood, the context is closed when Hardwood is closed
        if (ownsContext) {
            context.close();
        }

        // Only close InputFile if we own it
        if (ownsInputFile) {
            inputFile.close();
        }
    }

}
