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
import dev.hardwood.reader.MetadataSource;
import dev.hardwood.reader.ParquetFileReader;

/// Context object that manages shared resources for Parquet file reading.
///
/// Holds the thread pool for parallel page decoding, the libdeflate
/// decompressor pool for native GZIP decompression, and the decompressor factory.
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

    /// Start building a context with non-default settings.
    ///
    /// ```java
    /// try (HardwoodContext ctx = HardwoodContext.builder()
    ///         .threads(16)
    ///         .metadataSource(myCache)
    ///         .build()) {
    ///     ...
    /// }
    /// ```
    static Builder builder() {
        return HardwoodContextImpl.builder();
    }

    /// Builder for [HardwoodContext].
    interface Builder {

        /// Set the thread-pool size for parallel page decoding.
        /// Defaults to [Runtime#availableProcessors()].
        Builder threads(int threads);

        /// Install a [MetadataSource] that supplies pre-parsed footers for every
        /// [ParquetFileReader#open] and [ParquetFileReader#openAll] call made against
        /// the context. Without a source the footer is read and parsed on every open.
        Builder metadataSource(MetadataSource source);

        /// Build the context. The caller is responsible for closing it.
        HardwoodContext build();
    }
}
