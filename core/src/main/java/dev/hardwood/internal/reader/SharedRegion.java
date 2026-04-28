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

/// A contiguous, multi-column byte range fetched in a single
/// `readRange` call. Exists to coalesce the per-column reads of a row
/// group's column chunks — the chunks are typically stored back-to-back
/// on disk, so issuing one ranged GET that spans many columns is cheaper
/// than N separate ranged GETs (one per column). See #374.
///
/// One [ChunkHandle] per column attaches to a `SharedRegion` and slices
/// out its sub-range on demand. The first slice triggers the underlying
/// `readRange`; subsequent slices share the cached buffer.
///
/// Lifecycle: the region is alive as long as any attached handle still
/// references it. Eviction of the row group's `FetchPlan[]` drops all
/// attached handles, the region's strong references go to GC, and the
/// underlying `ByteBuffer` is reclaimed.
public final class SharedRegion {

    private static final System.Logger LOG = System.getLogger(SharedRegion.class.getName());

    private final InputFile inputFile;
    private final long fileOffset;
    private final int length;
    private final String purpose;
    private volatile SharedRegion nextRegion;
    private volatile ByteBuffer data;

    public SharedRegion(InputFile inputFile, long fileOffset, int length, String purpose) {
        this.inputFile = inputFile;
        this.fileOffset = fileOffset;
        this.length = length;
        this.purpose = purpose;
    }

    public long fileOffset() {
        return fileOffset;
    }

    public int length() {
        return length;
    }

    /// Chains a one-ahead region for asynchronous pre-fetch. When this
    /// region's data is first fetched, the next region's fetch is
    /// kicked off on a worker thread.
    public void setNextRegion(SharedRegion next) {
        this.nextRegion = next;
    }

    /// Returns a zero-copy slice covering `[absoluteOffset,
    /// absoluteOffset + sliceLength)`. The slice must lie entirely
    /// within this region's range; the caller is responsible for
    /// upholding that invariant.
    public ByteBuffer slice(long absoluteOffset, int sliceLength) {
        ByteBuffer buf = ensureFetched();
        int rel = Math.toIntExact(absoluteOffset - fileOffset);
        return buf.slice(rel, sliceLength);
    }

    /// Ensures the region's data is fetched. First call triggers
    /// `readRange`; subsequent calls return the cached buffer.
    /// After fetching, kicks off async pre-fetch of the next region.
    public ByteBuffer ensureFetched() {
        ByteBuffer buf = data;
        if (buf != null) {
            return buf;
        }
        fetchData();
        SharedRegion next = nextRegion;
        if (next != null && next.data == null) {
            CompletableFuture.runAsync(FetchReason.bind(next::fetchData))
                    .exceptionally(t -> {
                        LOG.log(System.Logger.Level.DEBUG,
                                "Prefetch failed for region at offset {0} (length {1}) in {2}",
                                next.fileOffset, next.length, next.inputFile.name(), t);
                        return null;
                    });
        }
        return data;
    }

    private void fetchData() {
        if (data != null) {
            return;
        }
        synchronized (this) {
            if (data != null) {
                return;
            }
            String outer = FetchReason.current();
            String composed = "unattributed".equals(outer) ? purpose : outer + " | " + purpose;
            try (FetchReason.Scope ignored = FetchReason.set(composed)) {
                data = inputFile.readRange(fileOffset, length);
            }
            catch (IOException e) {
                throw new UncheckedIOException(
                        ExceptionContext.filePrefix(inputFile.name())
                        + "Failed to fetch region at offset " + fileOffset
                        + " (length " + length + ")", e);
            }
        }
    }
}
