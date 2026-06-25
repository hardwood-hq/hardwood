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
/// To control the decode parallelism, or to reuse one context (and its thread
/// pool) across many reads and standalone [ParquetFileReader]s, create the
/// [HardwoodContext] yourself and pass it in:
/// ```java
/// try (HardwoodContext context = HardwoodContext.create(4)) {
///     try (Hardwood hardwood = Hardwood.create(context)) {
///         // ...
///     }
///     // context is still open here for further use
/// }
/// ```
///
/// For single-file usage, [ParquetFileReader#open(InputFile)] is simpler.
public class Hardwood implements AutoCloseable {

    private final HardwoodContextImpl context;
    private final boolean ownsContext;

    private Hardwood(HardwoodContextImpl context, boolean ownsContext) {
        this.context = context;
        this.ownsContext = ownsContext;
    }

    /// Create a new Hardwood instance with a thread pool sized to available processors.
    /// The context is owned by this instance and closed when it is closed.
    public static Hardwood create() {
        return new Hardwood(HardwoodContextImpl.create(), true);
    }

    /// Create a new Hardwood instance backed by the given context, e.g. one
    /// created via [HardwoodContext#create(int)] to size the decode thread pool.
    ///
    /// The caller retains ownership of the context: it is **not** closed when
    /// this instance is closed, so the same context — and its thread pool — can
    /// be reused for later reads and shared with standalone [ParquetFileReader]s
    /// opened via [ParquetFileReader#open(InputFile, HardwoodContext)].
    public static Hardwood create(HardwoodContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        return new Hardwood((HardwoodContextImpl) context, false);
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
    public ParquetFileReader openAll(List<? extends InputFile> inputFiles) throws IOException {
        return ParquetFileReader.openAll(inputFiles, context);
    }

    @Override
    public void close() {
        if (ownsContext) {
            context.close();
        }
    }
}
