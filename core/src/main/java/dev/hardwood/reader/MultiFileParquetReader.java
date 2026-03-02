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
import dev.hardwood.internal.reader.FileManager;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

/**
 * Entry point for reading multiple Parquet files with cross-file prefetching.
 * <p>
 * This is the multi-file equivalent of {@link ParquetFileReader}. It opens the
 * first file, reads the schema, and lets you choose between row-oriented or
 * column-oriented access with a specific column projection.
 * </p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (Hardwood hardwood = Hardwood.create();
 *      MultiFileParquetReader reader = hardwood.openAll(files)) {
 *
 *     FileSchema schema = reader.getFileSchema();
 *
 *     // Row-oriented access:
 *     try (MultiFileRowReader rows = reader.createRowReader(
 *             ColumnProjection.columns("col1", "col2"))) { ... }
 *
 *     // Column-oriented access:
 *     try (MultiFileColumnReaders columns = reader.createColumnReaders(
 *             ColumnProjection.columns("col1", "col2"))) { ... }
 * }
 * }</pre>
 */
public class MultiFileParquetReader implements AutoCloseable {

    private final HardwoodContextImpl context;
    private final FileManager fileManager;
    private final FileSchema schema;

    /**
     * Creates a MultiFileParquetReader for the given {@link InputFile} instances.
     * <p>
     * The files will be opened automatically as needed. Closing this reader
     * closes all the files.
     * </p>
     *
     * @param inputFiles the input files to read (must not be empty)
     * @param context the shared context
     * @throws IOException if the first file cannot be opened or read
     */
    public MultiFileParquetReader(List<InputFile> inputFiles, HardwoodContextImpl context) throws IOException {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.context = context;
        this.fileManager = new FileManager(inputFiles, context);
        try {
            this.schema = fileManager.openFirst();
        }
        catch (IOException e) {
            throw new IOException("Error reading '" + inputFiles.get(0).name() + "': " + e.getMessage(), e);
        }
        catch (Exception e) {
            throw AbstractRowReader.wrapWithFileContext(inputFiles.get(0).name(), e);
        }
    }

    /**
     * Get the file schema (common across all files).
     */
    public FileSchema getFileSchema() {
        return schema;
    }

    /**
     * Create a row reader that iterates over all rows in all files.
     */
    public MultiFileRowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    /**
     * Create a row reader that iterates over selected columns in all files.
     *
     * @param projection specifies which columns to read
     */
    public MultiFileRowReader createRowReader(ColumnProjection projection) {
        FileManager.InitResult initResult = fileManager.initialize(projection);
        return new MultiFileRowReader(context, fileManager, initResult);
    }

    /**
     * Create column readers for batch-oriented access to the requested columns.
     *
     * @param projection specifies which columns to read
     */
    public MultiFileColumnReaders createColumnReaders(ColumnProjection projection) {
        FileManager.InitResult initResult = fileManager.initialize(projection);
        return new MultiFileColumnReaders(context, fileManager, initResult);
    }

    @Override
    public void close() {
        fileManager.close();
    }
}
