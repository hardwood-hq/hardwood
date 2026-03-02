/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.util.List;

import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.ParquetFileReader;

/**
 * Entry point for reading Parquet files with a shared thread pool.
 *
 * <p>Use this when reading multiple files to share the executor across readers:</p>
 * <pre>{@code
 * try (Hardwood hardwood = Hardwood.create()) {
 *     ParquetFileReader file1 = hardwood.open(InputFile.of(path1));
 *     ParquetFileReader file2 = hardwood.open(InputFile.of(path2));
 *     // ...
 * }
 * }</pre>
 *
 * <p>For single-file usage, {@link ParquetFileReader#open(InputFile)} is simpler.</p>
 */
public class Hardwood implements AutoCloseable {

    private final HardwoodContextImpl context;

    private Hardwood(HardwoodContextImpl context) {
        this.context = context;
    }

    /**
     * Create a new Hardwood instance with a thread pool sized to available processors.
     */
    public static Hardwood create() {
        return new Hardwood(HardwoodContextImpl.create());
    }

    /**
     * Open a Parquet file from an {@link InputFile} for reading.
     * <p>
     * The file will be opened and closed automatically; closing the
     * returned reader closes the file.
     * </p>
     */
    public ParquetFileReader open(InputFile inputFile) throws IOException {
        return ParquetFileReader.open(inputFile, context);
    }

    /**
     * Open multiple Parquet files for reading with cross-file prefetching.
     * <p>
     * Returns a {@link MultiFileParquetReader} that reads the schema from the first file
     * and provides factory methods for row-oriented
     * ({@link MultiFileParquetReader#createRowReader()}) or column-oriented
     * ({@link MultiFileParquetReader#createColumnReaders(dev.hardwood.schema.ColumnProjection)}) access.
     * The files will be opened automatically as needed.
     * </p>
     *
     * @param inputFiles the input files to read (must not be empty)
     * @return a MultiFileParquetReader for the given files
     * @throws IOException if the first file cannot be opened or read
     * @throws IllegalArgumentException if the list is empty
     */
    public MultiFileParquetReader openAll(List<InputFile> inputFiles) throws IOException {
        return new MultiFileParquetReader(inputFiles, context);
    }

    @Override
    public void close() {
        context.close();
    }
}
