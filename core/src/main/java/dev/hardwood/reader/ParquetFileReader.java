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

import dev.hardwood.Hardwood;
import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.filter.RowGroupFilter;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * Reader for individual Parquet files.
 *
 * <p>For single-file usage:</p>
 * <pre>{@code
 * try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
 *     RowReader rows = reader.createRowReader();
 *     // ...
 * }
 * }</pre>
 *
 * <p>For multi-file usage with shared thread pool, use {@link Hardwood}.</p>
 *
 * <p><b>Limitation:</b> When using the default memory-mapped {@link InputFile},
 * individual files must be at most 2 GB ({@link Integer#MAX_VALUE} bytes).
 * Larger datasets should be split across multiple files and read via
 * {@link MultiFileParquetReader}.</p>
 */
public class ParquetFileReader implements AutoCloseable {

    private final InputFile inputFile;
    private final FileMetaData fileMetaData;
    private final HardwoodContextImpl context;
    private final boolean ownsContext;
    private final boolean ownsInputFile;

    private ParquetFileReader(InputFile inputFile, FileMetaData fileMetaData,
                              HardwoodContextImpl context, boolean ownsContext, boolean ownsInputFile) {
        this.inputFile = inputFile;
        this.fileMetaData = fileMetaData;
        this.context = context;
        this.ownsContext = ownsContext;
        this.ownsInputFile = ownsInputFile;
    }

    /**
     * Open a Parquet file from an {@link InputFile} with a dedicated context.
     * <p>
     * This method calls {@link InputFile#open()} and takes ownership of the file;
     * it will be closed when this reader is closed.
     * </p>
     */
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

    /**
     * Open a Parquet file from an {@link InputFile} with a shared context.
     * <p>
     * This method calls {@link InputFile#open()} and takes ownership of the file;
     * it will be closed when this reader is closed. The caller retains ownership
     * of the context.
     * </p>
     */
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

    /**
     * Create a ColumnReader for a named column, spanning all row groups.
     */
    public ColumnReader createColumnReader(String columnName) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnName, schema, inputFile, fileMetaData.rowGroups(), context);
    }

    /**
     * Create a ColumnReader for a column by index, spanning all row groups.
     */
    public ColumnReader createColumnReader(int columnIndex) {
        FileSchema schema = getFileSchema();
        return ColumnReader.create(columnIndex, schema, inputFile, fileMetaData.rowGroups(), context);
    }

    /**
     * Create a RowReader that iterates over all rows in all row groups.
     */
    public RowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    /**
     * Create a RowReader with a filter that uses column statistics to skip non-matching row groups.
     *
     * @param filter predicate evaluated against per-column min/max statistics
     * @return a RowReader that only processes row groups which may contain matching rows
     */
    public RowReader createRowReader(RowGroupFilter filter) {
        return createRowReader(ColumnProjection.all(), filter);
    }

    /**
     * Create a RowReader that iterates over selected columns in all row groups.
     *
     * @param projection specifies which columns to read
     * @return a RowReader for the selected columns
     */
    public RowReader createRowReader(ColumnProjection projection) {
        FileSchema schema = getFileSchema();
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);
        return new SingleFileRowReader(schema, projectedSchema, inputFile, fileMetaData.rowGroups(), context);
    }

    /**
     * Create a RowReader with both column projection and row group filtering.
     *
     * @param projection specifies which columns to read
     * @param filter predicate evaluated against per-column min/max statistics
     * @return a RowReader that only processes row groups which may contain matching rows
     */
    public RowReader createRowReader(ColumnProjection projection, RowGroupFilter filter) {
        FileSchema schema = getFileSchema();
        List<RowGroup> filteredRowGroups = filterRowGroups(fileMetaData.rowGroups(), filter, schema);
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);
        return new SingleFileRowReader(schema, projectedSchema, inputFile, filteredRowGroups, context);
    }

    /**
     * Create a ColumnReader for a named column with row group filtering.
     */
    public ColumnReader createColumnReader(String columnName, RowGroupFilter filter) {
        FileSchema schema = getFileSchema();
        List<RowGroup> filteredRowGroups = filterRowGroups(fileMetaData.rowGroups(), filter, schema);
        return ColumnReader.create(columnName, schema, inputFile, filteredRowGroups, context);
    }

    /**
     * Create a ColumnReader for a column by index with row group filtering.
     */
    public ColumnReader createColumnReader(int columnIndex, RowGroupFilter filter) {
        FileSchema schema = getFileSchema();
        List<RowGroup> filteredRowGroups = filterRowGroups(fileMetaData.rowGroups(), filter, schema);
        return ColumnReader.create(columnIndex, schema, inputFile, filteredRowGroups, context);
    }

    private static List<RowGroup> filterRowGroups(List<RowGroup> rowGroups, RowGroupFilter filter, FileSchema schema) {
        List<RowGroup> result = new ArrayList<>(rowGroups.size());
        for (RowGroup rowGroup : rowGroups) {
            if (!filter.canDrop(rowGroup, schema)) {
                result.add(rowGroup);
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
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
