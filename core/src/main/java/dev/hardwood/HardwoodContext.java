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

/**
 * Context object that manages shared resources for Parquet file reading.
 * <p>
 * Holds the thread pool for parallel page decoding, the libdeflate
 * decompressor pool for native GZIP decompression, and the decompressor factory.
 * </p>
 * <p>
 * The context lifecycle is tied to either:
 * <ul>
 *   <li>{@link Hardwood} instance (for multi-file usage)</li>
 *   <li>{@link ParquetFileReader} instance (for standalone single-file usage)</li>
 * </ul>
 * </p>
 */
public interface HardwoodContext extends AutoCloseable {

    /**
     * Get the executor service for parallel operations.
     */
    ExecutorService executor();

    @Override
    void close();

    /**
     * Create a new context with a thread pool sized to available processors.
     */
    static HardwoodContext create() {
        return HardwoodContextImpl.create();
    }

    /**
     * Create a new context with a thread pool of the specified size.
     */
    static HardwoodContext create(int threads) {
        return HardwoodContextImpl.create(threads);
    }
}
