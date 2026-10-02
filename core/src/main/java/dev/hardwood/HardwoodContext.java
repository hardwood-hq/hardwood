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

/// Context object that manages shared resources for Parquet file reading.
///
/// Holds the thread pool for parallel page decoding, the libdeflate
/// decompressor pool for native GZIP decompression, the decompressor factory,
/// and the [MetadataSource] installed through [#builder()], if any.
///
/// The context lifecycle is tied to either:
///
/// - [Hardwood] instance (for multi-file usage)
/// - [ParquetFileReader] instance (for standalone single-file usage)
public interface HardwoodContext extends AutoCloseable {

    /// Get the executor service for parallel operations.
    ExecutorService executor();

    @Override
    void close();

    /// Create a new context with a thread pool sized to available processors.
    static HardwoodContext create() {
        return HardwoodContextImpl.create();
    }

    /// Create a new context with a thread pool of the specified size.
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

        /// Sets the number of platform threads the context uses to decompress and decode pages.
        /// Defaults to [Runtime#availableProcessors()].
        ///
        /// @throws IllegalArgumentException if `threads` is less than 1
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
