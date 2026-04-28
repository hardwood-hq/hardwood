/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;

/// Lazy fetch handle for a contiguous byte range in a Parquet file.
///
/// Multiple pages within a column share a `ChunkHandle` (one handle per
/// coalesced page group). The actual `readRange()` call is deferred until
/// the first page in this chunk is accessed. After fetching, async pre-fetch
/// of the next chunk handle is triggered to overlap I/O with decode.
///
/// For local files backed by memory-mapped I/O, `readRange()` returns a
/// zero-copy slice — the fetch is instant and pre-fetch is effectively a no-op.
public class ChunkHandle {

    private static final System.Logger LOG = System.getLogger(ChunkHandle.class.getName());

    private final InputFile inputFile;
    private final long fileOffset;
    private final int length;
    private final String purpose;
    private volatile ChunkHandle nextChunk;
    private volatile ByteBuffer data;

    /// Creates a chunk handle for a byte range in the given file.
    ///
    /// @param inputFile the file to read from
    /// @param fileOffset absolute file offset of the first byte
    /// @param length number of bytes in this chunk
    /// @param purpose human-readable [FetchReason] tag attached to the underlying
    ///        `readRange` so fetch logs can attribute bytes to a specific
    ///        row-group / column / page-group
    public ChunkHandle(InputFile inputFile, long fileOffset, int length, String purpose) {
        this.inputFile = inputFile;
        this.fileOffset = fileOffset;
        this.length = length;
        this.purpose = purpose;
    }

    /// Convenience overload for callers that haven't been wired with a purpose
    /// tag yet. Tagged as `unattributed` in fetch logs.
    public ChunkHandle(InputFile inputFile, long fileOffset, int length) {
        this(inputFile, fileOffset, length, "unattributed");
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
    /// returns cached data on subsequent calls. After fetching, kicks off
    /// async pre-fetch of the next chunk (one-ahead only — the pre-fetch
    /// does not chain further).
    ///
    /// @return the fetched data buffer
    public ByteBuffer ensureFetched() {
        ByteBuffer buf = data;
        if (buf != null) {
            return buf;
        }
        fetchData();
        // Pre-fetch next chunk asynchronously (one-ahead only — fetchData
        // does not trigger further pre-fetches)
        ChunkHandle next = nextChunk;
        if (next != null) {
            // Carry the caller's FetchReason across the thread handoff;
            // otherwise the next-chunk readRange would log as `unattributed`.
            CompletableFuture.runAsync(FetchReason.bind(next::fetchData))
                    .exceptionally(t -> {
                        // The demand-path fetch will re-attempt and surface a
                        // fresh exception if the error is sustained, so we log
                        // at DEBUG rather than WARN to avoid flooding logs
                        // during a real backend outage. Logged here so
                        // transient failures (which the retry would hide) are
                        // still diagnosable.
                        LOG.log(System.Logger.Level.DEBUG,
                                "Prefetch failed for chunk at offset {0} (length {1}) in {2}",
                                next.fileOffset, next.length, next.inputFile.name(), t);
                        return null;
                    });
        }
        return data;
    }

    /// Fetches this chunk's data if not already cached. Does NOT trigger
    /// pre-fetch of the next chunk.
    private void fetchData() {
        if (data != null) {
            return;
        }
        synchronized (this) {
            if (data != null) {
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
                throw new UncheckedIOException(
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
    public ByteBuffer slice(long absoluteOffset, int regionLength) {
        ByteBuffer buf = ensureFetched();
        int relOffset = Math.toIntExact(absoluteOffset - fileOffset);
        return buf.slice(relOffset, regionLength);
    }
}
