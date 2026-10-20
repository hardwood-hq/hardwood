/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

/// Values computed once per work item, as `ConcurrentHashMap.computeIfAbsent` computes them, with
/// no lock held while a computation runs. The computations read the file, and a
/// `ConcurrentHashMap` holds a monitor for the whole of a mapping function: on JDK 21, a virtual
/// thread blocked on a monitor holds its carrier, so the threads waiting for a computation that
/// waits for I/O could take every carrier the computing thread needs to resume.
///
/// The first caller for a key computes; a concurrent caller for that key waits for it and gets its
/// value. A computation that returns `null` or throws caches nothing: its caller gets the `null`
/// or the exception, and a caller that waited for it computes in its turn. An entry removed while
/// it is being computed is not cached; its computing caller still gets the value.
final class OnceCache<V> {

    private final ConcurrentHashMap<Integer, Entry<V>> entries = new ConcurrentHashMap<>();

    /// The value cached for `key`, `null` if none is, or if it is still being computed.
    V get(int key) {
        Entry<V> entry = entries.get(key);
        if (entry == null || !entry.future.isDone() || entry.future.isCompletedExceptionally()) {
            return null;
        }
        return entry.future.getNow(null);
    }

    /// The value cached for `key`, computing it with `compute` if none is.
    ///
    /// @throws IllegalStateException if called for `key` from within its own computation
    V computeIfAbsent(int key, IntFunction<? extends V> compute) {
        while (true) {
            Entry<V> mine = new Entry<>(Thread.currentThread());
            Entry<V> existing = entries.putIfAbsent(key, mine);
            if (existing == null) {
                return computeInto(key, mine, compute);
            }
            if (existing.computing == Thread.currentThread()) {
                throw new IllegalStateException("Recursive computation of entry " + key);
            }
            V value;
            try {
                value = existing.future.join();
            }
            catch (CompletionException e) {
                // That computation failed and left nothing cached; this caller computes in turn.
                continue;
            }
            if (value != null) {
                return value;
            }
        }
    }

    private V computeInto(int key, Entry<V> entry, IntFunction<? extends V> compute) {
        V value;
        try {
            value = compute.apply(key);
        }
        catch (RuntimeException | Error e) {
            entries.remove(key, entry);
            entry.computing = null;
            entry.future.completeExceptionally(e);
            throw e;
        }
        if (value == null) {
            entries.remove(key, entry);
        }
        entry.computing = null;
        entry.future.complete(value);
        return value;
    }

    void remove(int key) {
        entries.remove(key);
    }

    void clear() {
        entries.clear();
    }

    /// Whether no entry is cached or being computed.
    boolean isEmpty() {
        return entries.isEmpty();
    }

    /// One key's computation: the thread running it, `null` once it has finished, and its outcome.
    private static final class Entry<V> {

        /// Written before the outcome completes, so a caller that finds the outcome incomplete
        /// and this thread its own is inside the computation.
        private volatile Thread computing;
        private final CompletableFuture<V> future = new CompletableFuture<>();

        Entry(Thread computing) {
            this.computing = computing;
        }
    }
}
