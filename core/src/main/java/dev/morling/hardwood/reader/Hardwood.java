/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

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

    private final HardwoodContext context;

    private Hardwood(HardwoodContext context) {
        this.context = context;
    }

    /**
     * Create a new Hardwood instance with a thread pool sized to available processors.
     */
    public static Hardwood create() {
        return new Hardwood(HardwoodContext.create());
    }

    /**
     * Create a new Hardwood instance with a thread pool of the specified size.
     */
    public static Hardwood create(int threads) {
        return new Hardwood(HardwoodContext.create(threads));
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
     * This method returns a MultiFileRowReader that coordinates prefetching across file
     * boundaries. When pages from file N are running low, pages from file N+1 are already
     * being prefetched, eliminating queue misses at file transitions.
     * </p>
     *
     * @param paths the Parquet files to read (must not be empty)
     * @return a MultiFileRowReader for iterating over all rows in all files
     * @throws IOException if any file cannot be opened or read
     * @throws IllegalArgumentException if the paths list is empty
     */
    public MultiFileRowReader openAll(List<Path> paths) throws IOException {
        return openAll(paths, ColumnProjection.all());
    }

    /**
     * Open multiple Parquet files for reading with cross-file prefetching and column projection.
     * <p>
     * This method returns a MultiFileRowReader that coordinates prefetching across file
     * boundaries. When pages from file N are running low, pages from file N+1 are already
     * being prefetched, eliminating queue misses at file transitions.
     * </p>
     *
     * @param paths the Parquet files to read (must not be empty)
     * @param projection specifies which columns to read
     * @return a MultiFileRowReader for iterating over all rows in all files
     * @throws IOException if any file cannot be opened or read
     * @throws IllegalArgumentException if the paths list is empty
     */
    public MultiFileRowReader openAll(List<Path> paths, ColumnProjection projection) throws IOException {
        return new MultiFileRowReader(paths, context, projection);
    }

    /**
     * Get the executor service used by this instance.
     */
    public ExecutorService executor() {
        return context.executor();
    }

    @Override
    public void close() {
        context.close();
    }
}
