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
import dev.hardwood.reader.ParquetFileReader;

/// Entry point for reading Parquet files with a shared thread pool.
///
/// Use this when reading multiple files to share the executor across readers:
/// ```java
/// try (Hardwood hardwood = Hardwood.create()) {
///     ParquetFileReader file1 = hardwood.open(InputFile.of(path1));
///     ParquetFileReader file2 = hardwood.open(InputFile.of(path2));
///     // ...
/// }
/// ```
///
/// For single-file usage, [ParquetFileReader#open(InputFile)] is simpler.
public class Hardwood implements AutoCloseable {

    private final HardwoodContextImpl context;

    private Hardwood(HardwoodContextImpl context) {
        this.context = context;
    }

    /// Create a new Hardwood instance with a thread pool sized to available processors.
    public static Hardwood create() {
        return new Hardwood(HardwoodContextImpl.create());
    }

    /// Open a single Parquet file. The file is opened immediately and
    /// closed when the returned reader is closed.
    public ParquetFileReader open(InputFile inputFile) throws IOException {
        return ParquetFileReader.open(inputFile, context);
    }

    /// Open multiple Parquet files for reading with cross-file prefetching.
    /// The schema is read from the first file. Files are opened on demand
    /// by the iterator and closed when the returned reader is closed.
    ///
    /// @param inputFiles the input files to read (must not be empty)
    /// @throws IOException if the first file cannot be opened or read
    /// @throws IllegalArgumentException if the list is empty
    public ParquetFileReader openAll(List<InputFile> inputFiles) throws IOException {
        return ParquetFileReader.openAll(inputFiles, context);
    }

    @Override
    public void close() {
        context.close();
    }
}
