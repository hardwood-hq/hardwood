/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool for libdeflate decompressor handles.
 * <p>
 * libdeflate decompressor instances are NOT thread-safe, so this pool
 * manages decompressor instances across threads.
 * <p>
 * Each pool instance manages its own set of native decompressor handles.
 * The pool should be closed when no longer needed to free native resources.
 */
public final class LibdeflatePool {

    private final int maxPoolSize;
    private final ConcurrentLinkedQueue<DecompressorHandle> pool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger poolSize = new AtomicInteger(0);

    /**
     * Create a new pool with default max size (2x available processors).
     */
    public LibdeflatePool() {
        this(Runtime.getRuntime().availableProcessors() * 2);
    }

    /**
     * Create a new pool with the specified max size.
     */
    public LibdeflatePool(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    static final class DecompressorHandle {
        private final MemorySegment handle;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        DecompressorHandle() {
            try {
                LibdeflateBindings bindings = LibdeflateBindings.get();
                this.handle = (MemorySegment) bindings.allocDecompressor.invokeExact();
                if (handle.equals(MemorySegment.NULL)) {
                    throw new OutOfMemoryError("Failed to allocate libdeflate decompressor");
                }
            }
            catch (Throwable t) {
                throw new RuntimeException("Failed to create libdeflate decompressor", t);
            }
        }

        MemorySegment handle() {
            if (closed.get()) {
                throw new IllegalStateException("Decompressor has been closed");
            }
            return handle;
        }

        void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    LibdeflateBindings.get().freeDecompressor.invokeExact(handle);
                }
                catch (Throwable t) {
                    // Ignore cleanup errors
                }
            }
        }
    }

    DecompressorHandle acquire() {
        DecompressorHandle handle = pool.poll();
        if (handle != null) {
            return handle;
        }
        poolSize.incrementAndGet();
        return new DecompressorHandle();
    }

    void release(DecompressorHandle handle) {
        if (poolSize.get() <= maxPoolSize) {
            pool.offer(handle);
        }
        else {
            poolSize.decrementAndGet();
            handle.close();
        }
    }

    /**
     * Clears the decompressor pool, freeing all native resources.
     * Call this when done with decompression to avoid resource leaks.
     */
    public void clear() {
        DecompressorHandle handle;
        while ((handle = pool.poll()) != null) {
            poolSize.decrementAndGet();
            handle.close();
        }
    }
}
