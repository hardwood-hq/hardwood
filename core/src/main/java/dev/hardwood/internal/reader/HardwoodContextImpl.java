/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import dev.hardwood.HardwoodContext;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.DecodeExecutor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.libdeflate.LibdeflateLoader;
import dev.hardwood.internal.compression.libdeflate.LibdeflatePool;

/// Internal implementation of [HardwoodContext].
///
/// Holds the thread pool every decode of a read runs on ([DecodeExecutor]), the libdeflate
/// decompressor pool for native GZIP decompression, and the decompressor factory.
public class HardwoodContextImpl implements HardwoodContext {

    private static final String USE_LIBDEFLATE_PROPERTY = "hardwood.uselibdeflate";

    /// Told of every decode [#decode] runs, on the thread running it; for tests of where decode
    /// work runs. `null` for none.
    static volatile DecodeObserver decodeObserver;

    /// Sees each decode [#decode] runs.
    interface DecodeObserver {

        /// @param caller the thread that asked for the decode, which waits for it
        void decoding(Thread caller);
    }

    private final ExecutorService executor;
    private final DecodeExecutor decodeExecutor;
    private final int threads;
    /// The pool's threads, which a read must not run on: it waits for work only they can run.
    private final Set<Thread> poolThreads;
    private final LibdeflatePool libdeflatePool;
    private final DecompressorFactory decompressorFactory;
    private final MetadataSource metadataSource;

    private HardwoodContextImpl(ExecutorService executor, int threads, Set<Thread> poolThreads,
                                LibdeflatePool libdeflatePool, MetadataSource metadataSource) {
        this.executor = executor;
        this.decodeExecutor = new DecodeExecutor(executor, threads);
        this.threads = threads;
        this.poolThreads = poolThreads;
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

    /// Create a context whose page tasks run on `executor`, which the context takes as having
    /// `threads` threads and shuts down when it closes; for tests that decide the order a read's
    /// tasks run in.
    static HardwoodContextImpl create(ExecutorService executor, int threads) {
        return new HardwoodContextImpl(executor, threads, Set.of(), createLibdeflatePoolIfAvailable(),
                ParquetMetadataReader.FROM_FILE);
    }

    private static HardwoodContextImpl create(int threads, MetadataSource metadataSource) {
        AtomicInteger threadCounter = new AtomicInteger(0);
        Set<Thread> poolThreads = ConcurrentHashMap.newKeySet();
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "hardwood-" + threadCounter.getAndIncrement());
            t.setDaemon(true);
            poolThreads.add(t);
            return t;
        };
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                DecodeExecutor.newWorkQueue(), threadFactory);
        // A read's first decode, often its dictionary probe on the calling thread, would otherwise
        // wait for the pool's first thread to start; it starts here, while the footer is read
        executor.prestartCoreThread();
        LibdeflatePool libdeflatePool = createLibdeflatePoolIfAvailable();
        return new HardwoodContextImpl(executor, threads, poolThreads, libdeflatePool, metadataSource);
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

    /// The pool that decodes pages and encodes column chunks. Stays as the internal accessor once
    /// [HardwoodContext#executor()] is removed.
    @SuppressWarnings("removal")
    @Override
    public ExecutorService executor() {
        return executor;
    }

    /// Runs `task` on the context's pool, with a set of its decode buffers.
    ///
    /// @throws RejectedExecutionException if the context is closed
    public void execute(Consumer<DecodeBuffers> task) {
        decodeExecutor.execute(task);
    }

    /// Runs `work` on the context's pool, with a set of its decode buffers, and waits for its
    /// result. A failure of `work` is thrown as it was thrown.
    ///
    /// @throws IllegalStateException if the context is closed, or the calling thread is one of
    ///         its pool's ([#requireOffPool()])
    public <T> T decode(Function<DecodeBuffers, T> work) {
        requireOffPool();
        Thread caller = Thread.currentThread();
        try {
            return decodeExecutor.decode(buffers -> {
                DecodeObserver observer = decodeObserver;
                if (observer != null) {
                    observer.decoding(caller);
                }
                return work.apply(buffers);
            });
        }
        catch (RejectedExecutionException e) {
            throw ScheduledColumn.contextClosed(e);
        }
    }

    /// Checks that the calling thread is not one of the context's pool: a read, and a decode it
    /// waits for, need a thread of the pool the caller would be holding.
    ///
    /// @throws IllegalStateException if it is
    public void requireOffPool() {
        if (poolThreads.contains(Thread.currentThread())) {
            throw new IllegalStateException("A read cannot run on a thread of the HardwoodContext it reads with, "
                    + "whose threads it waits for: run it on another thread, or read with another context");
        }
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
