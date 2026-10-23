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

/// Fetch handle for a contiguous byte range in a Parquet file.
///
/// Multiple pages within a column share a `ChunkHandle` (one handle per
/// coalesced page group). The read is deferred until the bytes are first asked for. The first
/// [#poll] that finds the bytes present while a next chunk handle is chained starts the next
/// chunk's speculative read, whether this handle's bytes were read on demand or by an earlier
/// prefetch, to overlap I/O with decode.
///
/// A walk asks with [#poll], which never waits and issues no read: it returns the bytes, or
/// `null` while they are not present, and the handle is then the [ByteNeed] the step that drives
/// the walk issues ([#issue]). A thread off the pool waits for them ([#ensureFetched]).
///
/// A file whose bytes are in memory is sliced in place by [#poll], so a walk over it stops only
/// to read again bytes whose prefetch failed ([RangeRead#inMemory]).
public class ChunkHandle implements ByteNeed {

    private final InputFile inputFile;
    private final long fileOffset;
    private final int length;
    private final String purpose;
    /// Non-null when this handle is a sub-range of a coalesced
    /// cross-column region (#374). When set, the handle slices
    /// the region's buffer instead of issuing its own read, and
    /// the per-handle `nextChunk` chain is unused (pre-fetch is
    /// driven at the region level).
    private final SharedRegion region;
    /// Where the reads of a file not in memory run; `null` for a region-backed handle, which
    /// leaves its read to the region.
    private final PrefetchTasks prefetchTasks;
    private volatile ChunkHandle nextChunk;
    /// The read under way or done; `null` before the first one starts. Replaced only to repeat
    /// a speculative read that failed.
    private volatile RangeRead read;
    /// Set by the first [#poll] that finds the bytes present while a next chunk is chained, which
    /// starts that chunk's read. A handle a pre-fetch read still pre-fetches its successor when
    /// the walk reaches it, so pre-fetch stays one chunk ahead.
    private final AtomicBoolean nextChunkPrefetched = new AtomicBoolean();
    /// Guards the start of a read; held for no read, as the reads run on their own threads.
    private final ReentrantLock startLock = new ReentrantLock();

    /// Creates a chunk handle for a byte range in the given file.
    ///
    /// @param inputFile the file to read from
    /// @param fileOffset absolute file offset of the first byte
    /// @param length number of bytes in this chunk
    /// @param purpose human-readable [dev.hardwood.internal.FetchReason] tag attached to the
    ///        underlying `readRange` so fetch logs can attribute bytes to a specific
    ///        row-group / column / page-group
    /// @param prefetchTasks where the reads run for a file not in memory, so the read's owner
    ///        can wait for them before closing the file
    public ChunkHandle(InputFile inputFile, long fileOffset, int length, String purpose,
                       PrefetchTasks prefetchTasks) {
        this.inputFile = inputFile;
        this.fileOffset = fileOffset;
        this.length = length;
        this.purpose = purpose;
        this.region = null;
        this.prefetchTasks = prefetchTasks;
    }

    /// Creates a chunk handle that's a sub-range of a coalesced cross-column
    /// [SharedRegion]. Used when [RowGroupIterator] decides several columns'
    /// chunks fit cheaply into one ranged GET; each column's handle then slices
    /// the shared buffer rather than issuing its own read.
    public ChunkHandle(SharedRegion region, long fileOffset, int length, String purpose) {
        this.inputFile = null;
        this.fileOffset = fileOffset;
        this.length = length;
        this.purpose = purpose;
        this.region = region;
        this.prefetchTasks = null;
    }

    /// Returns the absolute file offset of this chunk.
    public long fileOffset() {
        return fileOffset;
    }

    /// Returns the length of this chunk in bytes.
    public int length() {
        return length;
    }

    /// Sets the next chunk handle for pre-fetching.
    public void setNextChunk(ChunkHandle next) {
        this.nextChunk = next;
    }

    /// Returns the next chunk handle, or `null` if none is chained.
    public ChunkHandle nextChunk() {
        return nextChunk;
    }

    /// This chunk's bytes, or `null` while they are not present, without waiting and without
    /// issuing a read, except for a file whose bytes are in memory, which it slices in place. The
    /// first call that finds the bytes present while a next chunk is chained starts the next
    /// chunk's speculative read.
    ///
    /// @throws IOException if the demand read failed
    public ByteBuffer poll() throws IOException {
        if (region != null) {
            ByteBuffer regionBytes = region.poll();
            if (regionBytes == null) {
                return null;
            }
            // Region-backed handles leave pre-fetch to SharedRegion.nextRegion, not to the
            // per-handle nextChunk chain, which would re-fetch bytes the region covers.
            return region.sliceOf(regionBytes, fileOffset, length);
        }
        RangeRead current = read;
        if (current == null && RangeRead.inMemory(inputFile)) {
            current = issue();
        }
        if (current == null || !current.isDone() || current.failedSpeculatively()) {
            return null;
        }
        ByteBuffer buf = current.bytes("chunk");
        ChunkHandle next = nextChunk;
        if (next != null && !nextChunkPrefetched.get() && nextChunkPrefetched.compareAndSet(false, true)) {
            next.startSpeculative();
        }
        return buf;
    }


    /// Ensures the chunk data is fetched, waiting for its read; for a thread off the pool. Starts
    /// the next chunk's read as [#poll] does.
    ///
    /// @return the fetched data buffer
    public ByteBuffer ensureFetched() throws IOException {
        while (true) {
            ByteBuffer buf = poll();
            if (buf != null) {
                return buf;
            }
            issue().await();
        }
    }

    @Override
    public RangeRead issue() {
        if (region != null) {
            return region.issue();
        }
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

    /// Starts this chunk's read ahead of need, unless one is under way or done. A failure leaves
    /// it to the demand path, which reads the chunk again and reports what fails there.
    void startSpeculative() {
        if (region != null) {
            region.startSpeculative();
            return;
        }
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

    /// Reads this chunk ahead of need and, once it is read, starts the read of the chunk chained
    /// after it, as a demand read would; for a speculative task off the pool, which waits for the
    /// first read so that whoever waits for the task waits for it too. A failure is left to the
    /// demand path.
    void prefetch() {
        if (region != null) {
            region.prefetch();
            return;
        }
        startSpeculative();
        RangeRead current = read;
        current.await();
        if (current.isDone() && !current.failedSpeculatively()) {
            ChunkHandle next = nextChunk;
            if (next != null && nextChunkPrefetched.compareAndSet(false, true)) {
                next.startSpeculative();
            }
        }
    }

    /// Slices a region from this chunk's data, waiting for it; for a thread off the pool.
    ///
    /// @param absoluteOffset absolute file offset of the region
    /// @param regionLength length of the region in bytes
    /// @return a ByteBuffer slice covering the requested region
    public ByteBuffer slice(long absoluteOffset, int regionLength) throws IOException {
        ByteBuffer buf = ensureFetched();
        int relOffset = Math.toIntExact(absoluteOffset - fileOffset);
        return buf.slice(relOffset, regionLength);
    }

    /// Slices a region from bytes [#poll] returned.
    ByteBuffer slice(ByteBuffer polled, long absoluteOffset, int regionLength) {
        return polled.slice(Math.toIntExact(absoluteOffset - fileOffset), regionLength);
    }
}
