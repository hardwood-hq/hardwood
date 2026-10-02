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

    private static final System.Logger LOG = System.getLogger(HardwoodContextImpl.class.getName());

    private final ExecutorService executor;
    private final LibdeflatePool libdeflatePool;
    private final DecompressorFactory decompressorFactory;
    private final MetadataSource metadataSource;

    private HardwoodContextImpl(ExecutorService executor, LibdeflatePool libdeflatePool,
                                 MetadataSource metadataSource) {
        this.executor = executor;
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
        return new HardwoodContextImpl(executor, libdeflatePool, metadataSource);
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

        if (!useLibdeflate) {
            LOG.log(System.Logger.Level.DEBUG, "Libdeflate disabled via system property");
            return null;
        }

        if (!LibdeflateLoader.isAvailable()) {
            LOG.log(System.Logger.Level.DEBUG, "Libdeflate not available (requires Java 22+ and native library)");
            return null;
        }

        LOG.log(System.Logger.Level.DEBUG, "Libdeflate enabled");
        return new LibdeflatePool();
    }

    @Override
    public ExecutorService executor() {
        return executor;
    }

    /// Get the decompressor factory.
    public DecompressorFactory decompressorFactory() {
        return decompressorFactory;
    }

    @Override
    public void close() {
        executor.shutdownNow();
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
