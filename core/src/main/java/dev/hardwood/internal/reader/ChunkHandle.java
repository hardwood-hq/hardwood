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

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;

/// Lazy fetch handle for a contiguous byte range in a Parquet file.
///
/// Multiple pages within a column share a `ChunkHandle` (one handle per
/// coalesced page group). The actual `readRange()` call is deferred until
/// the first page in this chunk is accessed. The first access that finds a next
/// chunk handle chained triggers its async pre-fetch, whether this handle's bytes
/// were fetched by that access or earlier by a pre-fetch, to overlap I/O with decode.
///
/// For local files backed by memory-mapped I/O, `readRange()` returns a
/// zero-copy slice — the fetch is instant and pre-fetch is effectively a no-op.
public class ChunkHandle {

    private static final System.Logger LOG = System.getLogger(ChunkHandle.class.getName());

    private final InputFile inputFile;
    private final long fileOffset;
    private final int length;
    private final String purpose;
    /// Non-null when this handle is a sub-range of a coalesced
    /// cross-column region (#374). When set, `ensureFetched()` slices
    /// the region's buffer instead of issuing its own `readRange`, and
    /// the per-handle `nextChunk` chain is unused (pre-fetch is
    /// driven at the region level).
    private final SharedRegion region;
    /// Where the one-ahead pre-fetch runs; `null` for a region-backed handle, which
    /// leaves pre-fetch to its region.
    private final PrefetchTasks prefetchTasks;
    private volatile ChunkHandle nextChunk;
    private volatile ByteBuffer data;
    /// Set by the first [#ensureFetched] that finds a next chunk chained, which
    /// starts that chunk's pre-fetch. A handle a pre-fetch fetched still pre-fetches
    /// its successor when the read reaches it, so pre-fetch stays one chunk ahead.
    private final AtomicBoolean nextChunkPrefetched = new AtomicBoolean();

    /// Creates a chunk handle for a byte range in the given file.
    ///
    /// @param inputFile the file to read from
    /// @param fileOffset absolute file offset of the first byte
    /// @param length number of bytes in this chunk
    /// @param purpose human-readable [FetchReason] tag attached to the underlying
    ///        `readRange` so fetch logs can attribute bytes to a specific
    ///        row-group / column / page-group
    /// @param prefetchTasks where the one-ahead pre-fetch of the next chunk runs, so the
    ///        read's owner can wait for it before closing the file
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
    /// chunks fit cheaply into one ranged GET; each column's
    /// `ensureFetched()` then slices the shared buffer rather than issuing
    /// its own `readRange`.
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

    /// Ensures the chunk data is fetched. Triggers fetch on first call,
    /// returns cached data on subsequent calls. The first call made while a
    /// next chunk is chained kicks off async pre-fetch of that chunk, whether
    /// this call fetched the data or found it already fetched by a pre-fetch;
    /// a pre-fetch fetches through [#fetchData] and so does not chain further.
    ///
    /// @return the fetched data buffer
    public ByteBuffer ensureFetched() throws IOException {
        ByteBuffer buf = data;
        if (buf == null) {
            fetchData();
            buf = data;
        }
        // Region-backed handles delegate pre-fetch to SharedRegion.nextRegion,
        // not to the per-handle nextChunk chain — the chain may still be set
        // by the per-column page-group construction, but it would re-fetch
        // bytes the shared region already covers.
        if (region != null) {
            return buf;
        }
        ChunkHandle next = nextChunk;
        if (next != null && !nextChunkPrefetched.get() && nextChunkPrefetched.compareAndSet(false, true)) {
            prefetch(next);
        }
        return buf;
    }

    /// Fetches `next` on this handle's [PrefetchTasks], which carries the caller's
    /// [FetchReason] across the thread handoff; otherwise the next-chunk readRange would
    /// log as `unattributed`.
    private void prefetch(ChunkHandle next) {
        prefetchTasks.submit(() -> {
            try {
                next.fetchData();
            }
            catch (IOException e) {
                // Speculative: nothing is waiting on this, and a failed prefetch
                // leaves the handle unfetched, so the demand path fetches it again
                // and reports the failure to a caller that is waiting for it.
                // DEBUG rather than WARN so a sustained backend outage does not
                // emit one line per chunk for failures that are about to be
                // reported properly.
                LOG.log(System.Logger.Level.DEBUG,
                        "Prefetch failed for chunk at offset {0} (length {1}) in {2}",
                        next.fileOffset, next.length, next.inputFile.name(), e);
            }
        });
    }

    /// Fetches this chunk's data if not already cached. Does NOT trigger
    /// pre-fetch of the next chunk.
    ///
    /// When the handle is region-backed (`region != null`), this slices
    /// the shared region's buffer — the underlying `readRange` happens
    /// at the region level, once, no matter how many columns share it.
    private void fetchData() throws IOException {
        if (data != null) {
            return;
        }
        synchronized (this) {
            if (data != null) {
                return;
            }
            if (region != null) {
                data = region.slice(fileOffset, length);
                return;
            }
            // If the caller set an outer scope (e.g. "prefetch rg=2"), prepend it
            // so the log line shows both the calling context and the chunk identity.
            String outer = FetchReason.current();
            String composed = "unattributed".equals(outer) ? purpose : outer + " | " + purpose;
            try (FetchReason.Scope ignored = FetchReason.set(composed)) {
                data = inputFile.readRange(fileOffset, length);
            }
            catch (IOException e) {
                throw new IOException(
                        ExceptionContext.filePrefix(inputFile.name())
                        + "Failed to fetch chunk at offset " + fileOffset
                        + " (length " + length + ")", e);
            }
        }
    }

    /// Slices a region from this chunk's data.
    ///
    /// @param absoluteOffset absolute file offset of the region
    /// @param regionLength length of the region in bytes
    /// @return a ByteBuffer slice covering the requested region
    public ByteBuffer slice(long absoluteOffset, int regionLength) throws IOException {
        ByteBuffer buf = ensureFetched();
        int relOffset = Math.toIntExact(absoluteOffset - fileOffset);
        return buf.slice(relOffset, regionLength);
    }
}
