/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.thrift.BloomFilterHeaderReader;
import dev.hardwood.internal.thrift.BloomFilterReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;

/// Parsing for a bloom filter read through the legacy (length-absent) path: a probe window
/// large enough for the header, from which the region's total length is derived.
///
/// Both the lazy source ([RowGroupBloomFilterSource]) and the prefetch planner parse through
/// this class, so the probe arithmetic — the clamp against the file length and the
/// window-completeness comparison — exists in exactly one place.
final class BloomFilterProbe {

    /// Bytes read to parse the header when `bloom_filter_length` is absent. The header is a tiny
    /// Thrift struct (an i32 plus three single-variant unions), comfortably under this bound.
    static final int HEADER_PROBE_BYTES = 64;

    private BloomFilterProbe() {
    }

    /// Bytes to read for a legacy probe window at `offset`: the fixed header window, clamped so
    /// it never runs past EOF. A Parquet file is never zero-length, so `fileLength - offset`
    /// is positive for any offset inside the file; callers with an offset at or past EOF must
    /// not call this.
    static int probeLength(long fileLength, long offset) {
        if (offset >= fileLength) {
            throw new IllegalArgumentException(
                    "Bloom filter offset " + offset + " is at or past the file length " + fileLength);
        }
        return Math.toIntExact(Math.min(fileLength - offset, HEADER_PROBE_BYTES));
    }

    /// Parses a legacy probe window positioned at the filter's start.
    ///
    /// The header is always parseable from the window. Whether the bitset fits depends on the
    /// header's declared `numBytes`: when the window covers the whole filter the result is
    /// [Result.Complete] with the finished filter, otherwise it is [Result.Oversized] with the
    /// total region length the caller must fetch.
    static Result parseWindow(ByteBuffer window) throws IOException {
        ThriftCompactReader windowReader = new ThriftCompactReader(window);
        BloomFilterHeader header = BloomFilterHeaderReader.read(windowReader);
        int totalLength = Math.addExact(windowReader.getBytesRead(), header.numBytes());
        if (totalLength <= window.remaining()) {
            // The window already covers the whole filter; slice the bitset straight from
            // where the header parse ended, reusing the parsed header instead of decoding
            // it again.
            return new Result.Complete(BloomFilterReader.readBitset(header, windowReader),
                    totalLength);
        }
        return new Result.Oversized(totalLength);
    }

    /// The outcome of parsing a legacy probe window.
    sealed interface Result {

        /// The window contained the whole filter; no further read is needed. `totalLength` is
        /// the filter's exact size in bytes.
        record Complete(BloomFilter filter, int totalLength) implements Result {
        }

        /// The bitset extends past the window; the caller must fetch the full
        /// `totalLength`-byte region.
        record Oversized(int totalLength) implements Result {
        }
    }

    /// Parses a fully fetched filter region — header plus bitset — positioned at the filter's
    /// start. Shared by the source's known-length path and the prefetch cache, so a prefetched
    /// filter validates exactly like a lazily read one.
    static BloomFilter parseComplete(ByteBuffer region) throws IOException {
        return BloomFilterReader.read(new ThriftCompactReader(region));
    }

    /// IOException is checked by the thrift readers; the prefetch path cannot propagate a
    /// checked exception, so callers that must not fail the read use this shape.
    static UncheckedIOException unchecked(IOException e, String message) {
        return new UncheckedIOException(message, e);
    }
}
