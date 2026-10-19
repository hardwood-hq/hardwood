/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.HardwoodContext;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.libdeflate.LibdeflateLoader;
import dev.hardwood.internal.compression.libdeflate.LibdeflatePool;

/// Internal implementation of [HardwoodContext].
///
/// Holds the thread pool for parallel page decoding, the libdeflate
/// decompressor pool for native GZIP decompression, and the decompressor factory.
public class HardwoodContextImpl implements HardwoodContext {

    private static final String USE_LIBDEFLATE_PROPERTY = "hardwood.uselibdeflate";

    private final ExecutorService executor;
    private final int threads;
    private final LibdeflatePool libdeflatePool;
    private final DecompressorFactory decompressorFactory;
    private final MetadataSource metadataSource;

    private HardwoodContextImpl(ExecutorService executor, int threads, LibdeflatePool libdeflatePool,
                                 MetadataSource metadataSource) {
        this.executor = executor;
        this.threads = threads;
        this.libdeflatePool = libdeflatePool;
        this.decompressorFactory = new DecompressorFactory(libdeflatePool);
        this.metadataSource = metadataSource;
    }

    /// Create a new context with a thread pool sized to available processors.
    public static HardwoodContextImpl create() {
        return create(Runtime.getRuntime().availableProcessors());
    }

    /// Create a new context with a thread pool of the specified size.
    public static HardwoodContextImpl create(int threads) {
        return create(threads, ParquetMetadataReader.FROM_FILE);
    }

    private static HardwoodContextImpl create(int threads, MetadataSource metadataSource) {
        AtomicInteger threadCounter = new AtomicInteger(0);
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "hardwood-" + threadCounter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        ExecutorService executor = Executors.newFixedThreadPool(threads, threadFactory);
        LibdeflatePool libdeflatePool = createLibdeflatePoolIfAvailable();
        return new HardwoodContextImpl(executor, threads, libdeflatePool, metadataSource);
    }

    /// Start building a context.
    public static HardwoodContext.Builder builder() {
        return new BuilderImpl();
    }

    /// The [MetadataSource] installed on this context, or [ParquetMetadataReader#FROM_FILE]
    /// when none is.
    public MetadataSource metadataSource() {
        return metadataSource;
    }

    private static LibdeflatePool createLibdeflatePoolIfAvailable() {
        boolean useLibdeflate = !"false".equalsIgnoreCase(
                System.getProperty(USE_LIBDEFLATE_PROPERTY));

        if (!useLibdeflate || !LibdeflateLoader.isAvailable()) {
            return null;
        }
        return new LibdeflatePool();
    }

    @Override
    public ExecutorService executor() {
        return executor;
    }

    /// The number of threads in [#executor()].
    public int threads() {
        return threads;
    }

    /// Get the decompressor factory.
    public DecompressorFactory decompressorFactory() {
        return decompressorFactory;
    }

    /// Shuts the pool down, letting the tasks already queued run: a reader still reading counts
    /// each task it submitted and waits for it when it closes, and its tasks return at once once
    /// the rejection of its next submission has ended its read.
    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (libdeflatePool != null) {
            libdeflatePool.clear();
        }
    }

    /// Builder implementation.
    private static final class BuilderImpl implements HardwoodContext.Builder {

        private int threads = Runtime.getRuntime().availableProcessors();
        private MetadataSource metadataSource = ParquetMetadataReader.FROM_FILE;

        @Override
        public HardwoodContext.Builder threads(int threads) {
            if (threads < 1) {
                throw new IllegalArgumentException("threads must be at least 1, but was " + threads);
            }
            this.threads = threads;
            return this;
        }

        @Override
        public HardwoodContext.Builder metadataSource(MetadataSource source) {
            this.metadataSource = Objects.requireNonNull(source, "source");
            return this;
        }

        @Override
        public HardwoodContext build() {
            return create(threads, metadataSource);
        }
    }
}
