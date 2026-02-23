/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

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

    private final List<Path> files;
    private final HardwoodContextImpl context;
    private final FileManager fileManager;
    private final FileSchema schema;

    public MultiFileParquetReader(List<Path> files, HardwoodContextImpl context) throws IOException {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.files = files;
        this.context = context;
        this.fileManager = new FileManager(files, context);
        this.schema = fileManager.openFirst();
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
        return new MultiFileRowReader(files, context, fileManager, initResult);
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
