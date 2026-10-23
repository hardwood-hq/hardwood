/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.InputFile;

/// A contiguous, multi-column byte range fetched in a single
/// `readRange` call. Exists to coalesce the per-column reads of a row
/// group's column chunks — the chunks are typically stored back-to-back
/// on disk, so issuing one ranged GET that spans many columns is cheaper
/// than N separate ranged GETs (one per column). See #374.
///
/// One [ChunkHandle] per column attaches to a `SharedRegion` and slices
/// out its sub-range. Its read is issued once, by the step whose walk needs it first ([#issue]),
/// and the attached handles share its bytes; a walk polls without waiting, as for a
/// [ChunkHandle].
///
/// Lifecycle: the region is alive as long as any attached handle still
/// references it. Eviction of the row group's `FetchPlan[]` drops all
/// attached handles, the region's strong references go to GC, and the
/// underlying `ByteBuffer` is reclaimed.
public final class SharedRegion implements ByteNeed {

    private final InputFile inputFile;
    private final long fileOffset;
    private final int length;
    private final String purpose;
    private final PrefetchTasks prefetchTasks;
    private volatile SharedRegion nextRegion;
    /// The read under way or done; `null` before the first one starts. Replaced only to repeat
    /// a speculative read that failed.
    private volatile RangeRead read;
    /// Set by the first [#poll] that finds the bytes present while a next region is chained,
    /// which starts that region's read. A region a pre-fetch read still pre-fetches its successor
    /// when the read reaches it, so pre-fetch stays one region ahead.
    private final AtomicBoolean nextRegionPrefetched = new AtomicBoolean();
    /// Guards the start of a read; held for no read, as the reads run on their own threads.
    private final ReentrantLock startLock = new ReentrantLock();

    /// @param prefetchTasks where the reads run for a file not in memory, so the read's owner can
    ///        wait for them before closing the file
    public SharedRegion(InputFile inputFile, long fileOffset, int length, String purpose,
                        PrefetchTasks prefetchTasks) {
        this.inputFile = inputFile;
        this.fileOffset = fileOffset;
        this.length = length;
        this.purpose = purpose;
        this.prefetchTasks = prefetchTasks;
    }

    public long fileOffset() {
        return fileOffset;
    }

    public int length() {
        return length;
    }

    /// Chains a one-ahead region for asynchronous pre-fetch. The first
    /// [#poll] that finds this region's bytes present starts the next region's read.
    public void setNextRegion(SharedRegion next) {
        this.nextRegion = next;
    }

    /// Returns a zero-copy slice covering `[absoluteOffset, absoluteOffset + sliceLength)` of
    /// bytes [#poll] returned. Throws [IllegalArgumentException] when the slice falls outside
    /// this region — coalescer bugs would otherwise surface as a generic
    /// [IndexOutOfBoundsException] from the underlying buffer with no context about which region
    /// or handle was misaligned.
    ByteBuffer sliceOf(ByteBuffer polled, long absoluteOffset, int sliceLength) {
        if (absoluteOffset < fileOffset
                || sliceLength < 0
                || absoluteOffset + sliceLength > (long) fileOffset + length) {
            throw new IllegalArgumentException(
                    "slice [" + absoluteOffset + ", " + (absoluteOffset + sliceLength)
                    + ") falls outside region [" + fileOffset + ", " + (fileOffset + length)
                    + ") (" + purpose + ")");
        }
        int rel = Math.toIntExact(absoluteOffset - fileOffset);
        return polled.slice(rel, sliceLength);
    }

    /// Returns a zero-copy slice as [#sliceOf] does, waiting for the read; for a thread off the
    /// pool.
    public ByteBuffer slice(long absoluteOffset, int sliceLength) throws IOException {
        return sliceOf(ensureFetched(), absoluteOffset, sliceLength);
    }

    /// The region's bytes, waiting for their read; for a thread off the pool. Starts the next
    /// region's read as [#poll] does.
    public ByteBuffer ensureFetched() throws IOException {
        while (true) {
            ByteBuffer buf = poll();
            if (buf != null) {
                return buf;
            }
            issue().await();
        }
    }

    /// The region's bytes, or `null` while they are not present, without waiting; see
    /// [ChunkHandle#poll].
    ///
    /// @throws IOException if the demand read failed
    public ByteBuffer poll() throws IOException {
        RangeRead current = read;
        if (current == null && RangeRead.inMemory(inputFile)) {
            current = issue();
        }
        if (current == null || !current.isDone() || current.failedSpeculatively()) {
            return null;
        }
        ByteBuffer buf = current.bytes("region");
        SharedRegion next = nextRegion;
        if (next != null && !nextRegionPrefetched.get() && nextRegionPrefetched.compareAndSet(false, true)) {
            next.startSpeculative();
        }
        return buf;
    }

    @Override
    public RangeRead issue() {
        RangeRead current = read;
        if (current != null && !current.failedSpeculatively()) {
            return current;
        }
        startLock.lock();
        try {
            current = read;
            if (current == null || current.failedSpeculatively()) {
                current = RangeRead.start(inputFile, fileOffset, length, purpose,
                        false, prefetchTasks);
                read = current;
            }
            return current;
        }
        finally {
            startLock.unlock();
        }
    }

    /// Reads the region ahead of need and, once it is read, starts the read of the next region;
    /// see [ChunkHandle#prefetch].
    void prefetch() {
        startSpeculative();
        RangeRead current = read;
        current.await();
        if (!current.failedSpeculatively()) {
            SharedRegion next = nextRegion;
            if (next != null && nextRegionPrefetched.compareAndSet(false, true)) {
                next.startSpeculative();
            }
        }
    }

    /// Starts the region's read ahead of need, unless one is under way or done.
    void startSpeculative() {
        if (read != null) {
            return;
        }
        startLock.lock();
        try {
            if (read == null) {
                read = RangeRead.start(inputFile, fileOffset, length, purpose,
                        true, prefetchTasks);
            }
        }
        finally {
            startLock.unlock();
        }
    }
}
