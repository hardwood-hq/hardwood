/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Entry point for reading multiple Parquet files with cross-file prefetching.
///
/// This is the multi-file equivalent of [ParquetFileReader]. It opens the
/// first file, reads the schema, and lets you choose between row-oriented or
/// column-oriented access with a specific column projection.
///
/// Usage:
/// ```java
/// try (Hardwood hardwood = Hardwood.create();
///      MultiFileParquetReader reader = hardwood.openAll(files)) {
///
///     FileSchema schema = reader.getFileSchema();
///
///     // Row-oriented access:
///     try (MultiFileRowReader rows = reader.createRowReader(
///             ColumnProjection.columns("col1", "col2"))) { ... }
///
///     // Column-oriented access:
///     try (MultiFileColumnReaders columns = reader.createColumnReaders(
///             ColumnProjection.columns("col1", "col2"))) { ... }
/// }
/// ```
public class MultiFileParquetReader implements AutoCloseable {

    private final HardwoodContextImpl context;
    private final List<InputFile> inputFiles;
    private final FileSchema schema;
    private final List<RowGroup> firstFileRowGroups;

    // RowGroupIterators created for row readers (need to be closed)
    private final java.util.List<RowGroupIterator> rowGroupIterators = new java.util.ArrayList<>();

    /// Creates a MultiFileParquetReader for the given [InputFile] instances.
    ///
    /// The files will be opened automatically as needed. Closing this reader
    /// closes all the files.
    ///
    /// @param inputFiles the input files to read (must not be empty)
    /// @param context the shared context
    /// @throws IOException if the first file cannot be opened or read
    public MultiFileParquetReader(List<InputFile> inputFiles, HardwoodContextImpl context) throws IOException {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.context = context;
        this.inputFiles = new java.util.ArrayList<>(inputFiles);
        InputFile first = inputFiles.get(0);
        first.open();
        FileMetaData metaData = ParquetMetadataReader.readMetadata(first);
        this.schema = FileSchema.fromSchemaElements(metaData.schema());
        this.firstFileRowGroups = metaData.rowGroups();
    }

    /// Get the file schema (common across all files).
    public FileSchema getFileSchema() {
        return schema;
    }

    /// Create a row reader that iterates over all rows in all files.
    public RowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    /// Create a row reader with a filter, iterating over all columns but only matching row groups.
    ///
    /// @param filter predicate for row group filtering based on statistics
    public RowReader createRowReader(FilterPredicate filter) {
        return createRowReader(ColumnProjection.all(), filter);
    }

    /// Create a row reader that iterates over selected columns in all files.
    ///
    /// @param projection specifies which columns to read
    public RowReader createRowReader(ColumnProjection projection) {
        return createRowReaderInternal(projection, null);
    }

    /// Create a row reader that iterates over selected columns in only matching row groups.
    ///
    /// @param projection specifies which columns to read
    /// @param filter predicate for row group and record-level filtering
    public RowReader createRowReader(ColumnProjection projection, FilterPredicate filter) {
        return createRowReaderInternal(projection, filter);
    }

    private RowReader createRowReaderInternal(ColumnProjection projection, FilterPredicate filter) {
        ResolvedPredicate resolved = filter != null
                ? FilterPredicateResolver.resolve(filter, schema) : null;

        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, 0);
        iterator.setFirstFile(schema, firstFileRowGroups);
        ProjectedSchema projected = iterator.initialize(projection, resolved);
        rowGroupIterators.add(iterator);

        return RowReader.create(iterator, schema, projected, context, resolved, 0);
    }


    /// Create column readers for batch-oriented access to the requested columns.
    ///
    /// @param projection specifies which columns to read
    public MultiFileColumnReaders createColumnReaders(ColumnProjection projection) {
        return createColumnReadersInternal(projection, null);
    }

    /// Create column readers for batch-oriented access to the requested columns,
    /// skipping row groups that don't match the filter.
    ///
    /// @param projection specifies which columns to read
    /// @param filter predicate for row group filtering based on statistics
    public MultiFileColumnReaders createColumnReaders(ColumnProjection projection, FilterPredicate filter) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return createColumnReadersInternal(projection, resolved);
    }

    private MultiFileColumnReaders createColumnReadersInternal(ColumnProjection projection,
                                                                ResolvedPredicate resolved) {
        RowGroupIterator iterator = new RowGroupIterator(inputFiles, context, 0);
        iterator.setFirstFile(schema, firstFileRowGroups);
        ProjectedSchema projected = iterator.initialize(projection, resolved);
        rowGroupIterators.add(iterator);
        return new MultiFileColumnReaders(context, iterator, schema, projected);
    }

    @Override
    public void close() {
        for (RowGroupIterator iterator : rowGroupIterators) {
            iterator.close();
        }
        rowGroupIterators.clear();
    }
}
