/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.util.concurrent.ExecutorService;

import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.writer.ParquetFileWriter;

/// Context object that manages shared resources for reading and writing Parquet files.
///
/// Holds the thread pool for parallel page decoding and column chunk encoding, the libdeflate
/// decompressor pool for native GZIP decompression, the decompressor factory,
/// and the [MetadataSource] installed through [#builder()], if any.
///
/// A context of zero threads ([#create(int)] with `0`) has no pool: a read on it decodes on the
/// thread calling the reader, as that thread asks for each batch, and a writer encodes on the
/// thread calling it. A read of files not in memory still fetches bytes ahead on virtual threads.
///
/// The context lifecycle is tied to either:
///
/// - [Hardwood] instance (for multi-file usage)
/// - [ParquetFileReader] instance (for standalone single-file usage)
/// - [ParquetFileWriter] instance (for a writer created without a context)
///
/// A context created by the caller is closed by the caller, after every reader and writer
/// using it.
public interface HardwoodContext extends AutoCloseable {

    /// Get the executor service for parallel operations.
    ///
    /// The executor runs Hardwood's own work: it decodes the pages of every read and encodes the
    /// column chunks of every write on this context, so a task submitted to it competes with
    /// them. A read waits for work only this executor's threads run, so a reader built with this
    /// context cannot be built in a task running on it: building one there fails with an
    /// [IllegalStateException].
    ///
    /// @throws UnsupportedOperationException if the context has zero threads, and so no pool
    /// @deprecated The executor is not meant to run the caller's tasks, and this method has no
    ///             replacement; it will be removed in the next major release.
    @Deprecated(since = "1.1", forRemoval = true)
    ExecutorService executor();

    /// Closes the context. The pool accepts no new task, and the tasks already submitted to it
    /// run to completion; the call waits up to five seconds for them.
    @Override
    void close();

    /// Create a new context with a thread pool sized to available processors.
    static HardwoodContext create() {
        return HardwoodContextImpl.create();
    }

    /// Create a new context with a thread pool of the specified size. With zero threads, the
    /// context has no pool, and its reads and writes run on the threads calling them.
    ///
    /// @throws IllegalArgumentException if `threads` is negative
    static HardwoodContext create(int threads) {
        return HardwoodContextImpl.create(threads);
    }

    /// Starts building a context.
    ///
    /// ```java
    /// try (HardwoodContext context = HardwoodContext.builder()
    ///         .threads(16)
    ///         .metadataSource(source)
    ///         .build()) {
    ///     // every reader opened against context takes its footers from source
    /// }
    /// ```
    @Experimental
    static Builder builder() {
        return HardwoodContextImpl.builder();
    }

    /// Builder for a [HardwoodContext].
    @Experimental
    interface Builder {

        /// Sets the number of platform threads the context uses to decompress and decode pages, to
        /// parse the footers and page indexes a read reads as it goes and scan dictionaries for a
        /// filter's values, and to encode and compress the column chunks of a writer's row groups.
        /// Defaults to [Runtime#availableProcessors()]. With zero threads, the context has no pool:
        /// that work runs on the threads calling the readers and writers.
        ///
        /// @throws IllegalArgumentException if `threads` is negative
        Builder threads(int threads);

        /// Installs a [MetadataSource] that supplies the footer of every file a reader opened
        /// against the context reads. Without one, each reader reads the footers itself.
        ///
        /// @throws NullPointerException if `source` is `null`
        Builder metadataSource(MetadataSource source);

        /// Builds the context. The caller closes it.
        HardwoodContext build();
    }
}
