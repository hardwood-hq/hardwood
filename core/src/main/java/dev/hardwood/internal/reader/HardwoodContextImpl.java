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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import dev.hardwood.HardwoodContext;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.DecodeExecutor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.libdeflate.LibdeflateLoader;
import dev.hardwood.internal.compression.libdeflate.LibdeflatePool;

/// Internal implementation of [HardwoodContext].
///
/// Holds the thread pool every decode of a read runs on ([DecodeExecutor]), the executor its
/// speculative reads run on ([#ioExecutor()]), the libdeflate decompressor pool for native GZIP
/// decompression, and the decompressor factory.
///
/// A context of zero threads has no pool ([#hasPool()]): each read keeps its steps and tasks in a
/// queue of its own ([ReadQueue]), which the threads calling the reader run, and `compute` and
/// `decode` run in place on the calling thread.
public class HardwoodContextImpl implements HardwoodContext {

    private static final String USE_LIBDEFLATE_PROPERTY = "hardwood.uselibdeflate";

    /// Makes [#create()], [#create(int)] and the builder create contexts without a pool, whatever
    /// thread count they are given; for running the test suites on such contexts.
    private static final String ZERO_THREADS_PROPERTY = "hardwood.internal.zeroThreads";

    /// Starts each task on a virtual thread of its own.
    private static final Executor IO_EXECUTOR = task -> Thread.ofVirtual().name("hardwood-io").start(task);

    /// Told of every decode [#decode] runs, on the thread running it; for tests of where decode
    /// work runs. `null` for none.
    static volatile DecodeObserver decodeObserver;

    /// Sees each decode [#decode] runs.
    interface DecodeObserver {

        /// @param caller the thread that asked for the decode, which waits for it; the thread
        ///        running it where it runs in place
        void decoding(Thread caller);
    }

    /// Told of every computation [#compute] runs, on the thread running it; for tests of where
    /// metadata parsing runs. `null` for none.
    static volatile ComputeObserver computeObserver;

    /// Sees each computation [#compute] runs.
    interface ComputeObserver {

        /// @param what the work, as [#compute] names it
        /// @param caller the thread that asked for it, which waits for it; the thread running it
        ///        where it runs in place
        void computing(String what, Thread caller);
    }

    /// The pool, `null` on a context of zero threads.
    private final ExecutorService executor;
    private final Executor ioExecutor;
    private final DecodeExecutor decodeExecutor;
    private final int threads;
    /// The pool's threads, which a read must not run on: it waits for work only they can run.
    private final Set<Thread> poolThreads;
    private final LibdeflatePool libdeflatePool;
    private final DecompressorFactory decompressorFactory;
    /// The read-ahead its reads share.
    private final ReadAheadBuffer readAheadBuffer = new ReadAheadBuffer();
    private final MetadataSource metadataSource;
    /// Set by [#close()] on a context without a pool, whose closing the pool's shutdown cannot
    /// record.
    private volatile boolean closed;

    private HardwoodContextImpl(ExecutorService executor, int threads, Set<Thread> poolThreads,
                                Executor ioExecutor, LibdeflatePool libdeflatePool, MetadataSource metadataSource) {
        this.executor = executor;
        this.ioExecutor = ioExecutor;
        this.decodeExecutor = new DecodeExecutor(executor,
                executor != null ? threads : Runtime.getRuntime().availableProcessors());
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

    /// Create a new context with a thread pool of the specified size; with zero threads, a context
    /// without a pool, whose reads run on the threads calling the readers.
    ///
    /// @throws IllegalArgumentException if `threads` is negative
    public static HardwoodContextImpl create(int threads) {
        return create(checkThreads(threads), ParquetMetadataReader.FROM_FILE, IO_EXECUTOR);
    }

    private static int checkThreads(int threads) {
        if (threads < 0) {
            throw new IllegalArgumentException("threads must not be negative, but was " + threads);
        }
        return Boolean.getBoolean(ZERO_THREADS_PROPERTY) ? 0 : threads;
    }

    /// Create a context whose page tasks and fetch steps run on `executor`, which the context
    /// takes as having `threads` threads, run by `poolThreads`, and shuts down when it closes;
    /// for tests that decide the order a read's tasks run in.
    static HardwoodContextImpl create(ExecutorService executor, int threads, Set<Thread> poolThreads) {
        return new HardwoodContextImpl(executor, threads, poolThreads, IO_EXECUTOR, createLibdeflatePoolIfAvailable(),
                ParquetMetadataReader.FROM_FILE);
    }

    /// Create a context with a pool of `threads` threads, or without a pool for zero, whose
    /// speculative work runs on `ioExecutor`; for tests that wait for or count that work.
    static HardwoodContextImpl withIoExecutor(int threads, Executor ioExecutor) {
        return create(threads, ParquetMetadataReader.FROM_FILE, ioExecutor);
    }

    private static HardwoodContextImpl create(int threads, MetadataSource metadataSource, Executor ioExecutor) {
        LibdeflatePool libdeflatePool = createLibdeflatePoolIfAvailable();
        if (threads == 0) {
            return new HardwoodContextImpl(null, 0, Set.of(), ioExecutor, libdeflatePool, metadataSource);
        }
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
        return new HardwoodContextImpl(executor, threads, poolThreads, ioExecutor, libdeflatePool, metadataSource);
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
    ///
    /// @throws UnsupportedOperationException if the context has no pool ([#hasPool()])
    @SuppressWarnings("removal")
    @Override
    public ExecutorService executor() {
        if (executor == null) {
            throw new UnsupportedOperationException("A HardwoodContext of zero threads has no executor");
        }
        return executor;
    }

    /// Whether the context has a pool: `false` for a context of zero threads, whose reads run on
    /// the threads calling the readers and whose writers encode on the threads calling them.
    public boolean hasPool() {
        return executor != null;
    }

    /// Whether the context is closed.
    public boolean isClosed() {
        return executor != null ? executor.isShutdown() : closed;
    }

    /// The executor a read's speculative work runs on: prefetches and footer loads, which spend
    /// their time waiting for I/O. Each task gets a virtual thread of its own, so none holds a
    /// thread of the decode pool or of the JVM's common pool while it waits; decode work such a
    /// task needs goes to the pool through [#decode].
    public Executor ioExecutor() {
        return ioExecutor;
    }

    /// Runs `task` on the context's pool, with a set of its decode buffers.
    ///
    /// @throws RejectedExecutionException if the context is closed
    public void execute(Consumer<DecodeBuffers> task) {
        decodeExecutor.execute(task);
    }

    /// Runs `task` on the calling thread, with a set of the context's decode buffers: a page task
    /// a read on a context without a pool runs on the thread driving it ([ReadQueue]).
    void runInPlace(Consumer<DecodeBuffers> task) {
        decodeExecutor.runInPlace(task);
    }

    /// Runs a column's fetch step on the context's pool, at the head of its queue.
    ///
    /// @throws RejectedExecutionException if the context is closed
    void executeStep(Runnable step) {
        decodeExecutor.executeFirst(step);
    }

    /// Runs speculative CPU work on the context's pool, at the tail of its queue.
    ///
    /// @throws RejectedExecutionException if the context is closed
    void executeSpeculative(Runnable task) {
        decodeExecutor.executeLast(task);
    }

    /// Runs `work` on the context's pool, with a set of its decode buffers, and waits for its
    /// result; on a thread of the pool, such as a column's fetch step, and on a context without a
    /// pool, it runs in place. A failure of `work` is thrown as it was thrown.
    ///
    /// @throws IllegalStateException if the context is closed
    public <T> T decode(Function<DecodeBuffers, T> work) {
        Thread caller = Thread.currentThread();
        if (runsInPlace()) {
            return decodeExecutor.decodeInPlace(buffers -> {
                DecodeObserver observer = decodeObserver;
                if (observer != null) {
                    observer.decoding(caller);
                }
                return work.apply(buffers);
            });
        }
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

    /// Runs `work`, CPU work a thread off the pool needs, such as parsing a footer or a page
    /// index, on the context's pool and waits for its result, so that it runs on the threads the
    /// context bounds rather than on the caller's. On a thread of the pool, and on a context
    /// without a pool, it runs in place. Like [#decode], it runs ahead of the tasks already
    /// queued. A failure of `work` is thrown as it was thrown.
    ///
    /// @param what names the work, for [#computeObserver]
    /// @throws IllegalStateException if the context is closed
    public <T> T compute(String what, Supplier<T> work) {
        Thread caller = Thread.currentThread();
        if (runsInPlace()) {
            ComputeObserver observer = computeObserver;
            if (observer != null) {
                observer.computing(what, caller);
            }
            return work.get();
        }
        try {
            return decodeExecutor.decode(buffers -> {
                ComputeObserver observer = computeObserver;
                if (observer != null) {
                    observer.computing(what, caller);
                }
                return work.get();
            });
        }
        catch (RejectedExecutionException e) {
            throw ScheduledColumn.contextClosed(e);
        }
    }

    /// Whether [#compute] and [#decode] run in place on the calling thread: it is one of the
    /// context's pool, or the context has none.
    boolean runsInPlace() {
        return executor == null || poolThreads.contains(Thread.currentThread());
    }

    /// Runs `task`, work a thread off the pool waits for and another thread may run as well, on
    /// the context's pool, at the head of its queue, and waits for its result.
    ///
    /// @throws IllegalStateException if the context is closed
    <T> T awaitFirst(FutureTask<T> task) {
        try {
            return decodeExecutor.awaitFirst(task);
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

    /// The number of threads in [#executor()], zero for a context without a pool.
    public int threads() {
        return threads;
    }

    /// The read-ahead buffer the context's reads share.
    public ReadAheadBuffer readAheadBuffer() {
        return readAheadBuffer;
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
        closed = true;
        if (executor == null) {
            clearLibdeflatePool();
            return;
        }
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        clearLibdeflatePool();
    }

    private void clearLibdeflatePool() {
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
            if (threads < 0) {
                throw new IllegalArgumentException("threads must not be negative, but was " + threads);
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
            return create(checkThreads(threads), metadataSource, IO_EXECUTOR);
        }
    }
}
