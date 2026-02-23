/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;

/**
 * Entry point for reading Parquet files with a shared thread pool.
 *
 * <p>Use this when reading multiple files to share the executor across readers:</p>
 * <pre>{@code
 * try (Hardwood hardwood = Hardwood.create()) {
 *     ParquetFileReader file1 = hardwood.open(path1);
 *     ParquetFileReader file2 = hardwood.open(path2);
 *     // ...
 * }
 * }</pre>
 *
 * <p>For single-file usage, {@link ParquetFileReader#open(Path)} is simpler.</p>
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
     * Create a new Hardwood instance with a thread pool of the specified size.
     */
    public static Hardwood create(int threads) {
        return new Hardwood(HardwoodContextImpl.create(threads));
    }

    /**
     * Open a Parquet file for reading.
     */
    public ParquetFileReader open(Path path) throws IOException {
        return ParquetFileReader.open(path, context);
    }

    /**
     * Open multiple Parquet files for reading with cross-file prefetching.
     * <p>
     * Returns a {@link MultiFileParquetReader} that reads the schema from the first file
     * and provides factory methods for row-oriented
     * ({@link MultiFileParquetReader#createRowReader(ColumnProjection)}) or column-oriented
     * ({@link MultiFileParquetReader#createColumnReaders(ColumnProjection)}) access.
     * </p>
     *
     * @param paths the Parquet files to read (must not be empty)
     * @return a MultiFileParquetReader for the given files
     * @throws IOException if the first file cannot be opened or read
     * @throws IllegalArgumentException if the paths list is empty
     */
    public MultiFileParquetReader openAll(List<Path> paths) throws IOException {
        return new MultiFileParquetReader(paths, context);
    }

    /**
     * Open multiple Parquet files for reading with cross-file prefetching.
     *
     * @param first the first Parquet file to read
     * @param furtherPaths additional Parquet files to read
     * @return a MultiFileParquetReader for the given files
     * @throws IOException if the first file cannot be opened or read
     */
    public MultiFileParquetReader openAll(Path first, Path... furtherPaths) throws IOException {
        Objects.requireNonNull(first, "At least one path required");

        List<Path> paths = new ArrayList<>(1 + furtherPaths.length);
        paths.add(first);
        paths.addAll(List.of(furtherPaths));
        return openAll(paths);
    }

    @Override
    public void close() {
        context.close();
    }
}
