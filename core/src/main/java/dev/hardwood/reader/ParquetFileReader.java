/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;

import dev.hardwood.Hardwood;
import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.event.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
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
