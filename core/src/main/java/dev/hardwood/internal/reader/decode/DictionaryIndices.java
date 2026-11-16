/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.util.Arrays;

import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.reader.ParquetReadException;

/// A data page's dictionary indices: for each non-null value, its index into the column chunk's
/// dictionary, or for `RLE` booleans into `{false, true}`. They are left encoded where they lie in
/// the page, an RLE/bit-packing hybrid stream at a fixed bit width, and decoded as a fill reads
/// them. Each index is called an id below, as the batch's entry ids it becomes are.
///
/// This class describes the stream and does not read it: a page's fills each read it through a
/// cursor of their own, a binary fill a [Cursor] ([#cursor()]) and a numeric fill a [SpanCursor]
/// ([#spanCursor()]).
final class DictionaryIndices {

    /// The most ids a cursor decodes into its buffer at once.
    static final int CHUNK = 1024;

    private final byte[] data;
    private final int offset;
    private final int limit;
    private final int bitWidth;
    /// How many ids the stream holds: one per non-null value of the page.
    private final int count;
    /// The entries of the dictionary the ids index, `2` for `RLE` booleans; a valid id lies in
    /// `[0, entries)`.
    private final int entries;

    private DictionaryIndices(byte[] data, int offset, int limit, int bitWidth, int count, int entries) {
        this.data = data;
        this.offset = offset;
        this.limit = limit;
        this.bitWidth = bitWidth;
        this.count = count;
        this.entries = entries;
    }

    /// The `count` ids of a dictionary page's value section `[offset, limit)` of `data`: the bit
    /// width in its first byte, the ids after it. The bit width is read and checked here.
    ///
    /// @param entries the dictionary's entries
    /// @throws ParquetReadException if the section has no bit width or one above `32`
    static DictionaryIndices ofDictionaryPage(byte[] data, int offset, int limit, int count, int entries) {
        if (offset >= limit) {
            throw new ParquetReadException("Unexpected EOF reading the dictionary index bit width");
        }
        int bitWidth = data[offset] & 0xFF;
        if (bitWidth > 32) {
            throw new ParquetReadException("Invalid dictionary index bit width: " + bitWidth
                    + ". Must be between 0 and 32");
        }
        return new DictionaryIndices(data, offset + 1, limit, bitWidth, count, entries);
    }

    /// The `count` ids of a stream at `bitWidth` in `[offset, limit)` of `data`, each in
    /// `[0, entries)`.
    static DictionaryIndices of(byte[] data, int offset, int limit, int bitWidth, int count, int entries) {
        return new DictionaryIndices(data, offset, limit, bitWidth, count, entries);
    }

    /// A reader of the ids into the caller's arrays, at the first.
    Cursor cursor() {
        return new Cursor(this);
    }

    /// A reader of the ids in spans, at the first.
    SpanCursor spanCursor() {
        return new SpanCursor(new Cursor(this));
    }

    /// A reader of a page's ids, forward only, for one fill, that decodes them straight into the
    /// caller's array ([#read]): a binary fill's, into the batch's entry ids. Every id it decodes
    /// is checked against the entries of the dictionary the ids index. [#skip] passes over ids
    /// without decoding them, decoding only those of a bit-packed group it starts or stops inside,
    /// and does not check them.
    static final class Cursor {

        private final DictionaryIndices indices;
        private final int bitWidth;
        private final int count;
        private final int entries;
        /// Whether the bit width admits an id of `entries` or more, so that ids must be checked.
        private final boolean checked;

        /// The decoder, `null` for a bit width of `0`, made on first use.
        private RleBitPackingHybridDecoder decoder;
        /// The ids decoded or passed over so far.
        private int decoded;
        /// The id of the run [#passRun] passed over last.
        private int runId;

        private Cursor(DictionaryIndices indices) {
            this.indices = indices;
            this.bitWidth = indices.bitWidth;
            this.count = indices.count;
            this.entries = indices.entries;
            this.checked = (1L << bitWidth) > entries;
        }

        /// Passes over the next `n` ids without decoding them ([RleBitPackingHybridDecoder#skip]).
        /// An id passed over is not checked.
        void skip(int n) {
            if (n == 0) {
                return;
            }
            if (n > count - decoded) {
                throw new IllegalStateException("Ids read past the page's " + count);
            }
            if (bitWidth > 0) {
                decoder().skip(n);
            }
            decoded += n;
        }

        /// Decodes the next `n` ids into `out` from `pos`, and checks them.
        void read(int[] out, int pos, int n) {
            if (n == 0) {
                return;
            }
            if (n > count - decoded) {
                throw new IllegalStateException("Ids read past the page's " + count);
            }
            if (bitWidth == 0) {
                Arrays.fill(out, pos, pos + n, 0);
            }
            else {
                decoder().readInts(out, pos, n);
            }
            decoded += n;
            if (checked) {
                check(out, pos, pos + n);
            }
        }

        /// The ids not yet decoded or passed over.
        private int left() {
            return count - decoded;
        }

        /// Passes over up to `max` ids of the run of one id the stream is inside, checking the id
        /// once, and returns how many, the id in [#runId]; `0` where the stream is inside a
        /// bit-packed run. A bit width of `0` is one run of `0`s.
        private int passRun(int max) {
            int n;
            int id;
            if (bitWidth == 0) {
                n = max;
                id = 0;
            }
            else {
                RleBitPackingHybridDecoder decoder = decoder();
                n = Math.min(max, decoder.repeatsLeft());
                if (n == 0) {
                    return 0;
                }
                id = decoder.repeatedValue();
                decoder.skip(n);
            }
            if (checked) {
                check(id);
            }
            decoded += n;
            runId = id;
            return n;
        }

        /// The decoder, made on first use.
        private RleBitPackingHybridDecoder decoder() {
            if (decoder == null) {
                decoder = new RleBitPackingHybridDecoder(indices.data, indices.offset, indices.limit - indices.offset,
                        bitWidth);
            }
            return decoder;
        }

        /// Checks that ids `[start, stop)` of `ids` lie in `[0, entries)`. The ids are compared
        /// unsigned, through their largest, so that the loop vectorizes; only an id out of range is
        /// looked for.
        ///
        /// @throws ArrayIndexOutOfBoundsException naming the first id out of range
        private void check(int[] ids, int start, int stop) {
            int max = Integer.MIN_VALUE;
            for (int i = start; i < stop; i++) {
                max = Math.max(max, ids[i] ^ Integer.MIN_VALUE);
            }
            if (max < (entries ^ Integer.MIN_VALUE)) {
                return;
            }
            for (int i = start; i < stop; i++) {
                check(ids[i]);
            }
        }

        /// Checks that `value` lies in `[0, entries)`, compared unsigned.
        ///
        /// @throws ArrayIndexOutOfBoundsException if it does not
        private void check(int value) {
            if (Integer.compareUnsigned(value, entries) >= 0) {
                throw new ArrayIndexOutOfBoundsException("Dictionary index " + value
                        + " out of bounds for a dictionary of " + entries + " entries");
            }
        }
    }

    /// A reader of a page's ids, forward only, for one fill, that hands them out a span at a time
    /// ([#take]): a numeric fill's, which looks each up in the dictionary. A span is a run of one
    /// id, or a slice of a buffer that stays in L1, never more than the caller asks for; every id
    /// handed out is checked. It reads the stream through a [Cursor].
    static final class SpanCursor {

        private final Cursor ids;
        /// The buffer, made on first use, and its ids not yet handed out, `[at, end)`: none between
        /// two of a fill's copies, since a copy takes no more ids than it uses.
        private int[] chunk;
        private int at;
        private int end;
        /// The span [#take] handed out last: a run of [#id], or ids from [#from] of [#chunk].
        private boolean run;
        private int id;
        private int from;

        private SpanCursor(Cursor ids) {
            this.ids = ids;
        }

        /// Passes over the next `n` ids without decoding them. An id passed over is not checked. No
        /// id is buffered here, since [#take] buffers no more than its caller takes.
        ///
        /// @throws IllegalStateException if an id is buffered
        void skip(int n) {
            if (at != end) {
                throw new IllegalStateException("Ids skipped with " + (end - at) + " buffered");
            }
            ids.skip(n);
        }

        /// Hands out the next ids, at least one and at most `max`, and returns how many. Where no id
        /// is buffered and the stream is inside a run of one id, they are that run's, its id checked
        /// once: [#run()] holds and [#id()] is the id. Otherwise they are ids `[from(), from() + n)`
        /// of [#chunk()], checked; an empty buffer is refilled first with at most `max` of them, and
        /// at most [#CHUNK]. What a take hands out stays valid until the next call on this cursor.
        ///
        /// @throws IllegalStateException if `max` is not positive or the page has no ids left
        int take(int max) {
            if (max <= 0) {
                throw new IllegalStateException("Ids taken in a span of " + max);
            }
            if (at == end) {
                int left = ids.left();
                if (left == 0) {
                    throw new IllegalStateException("Ids read past the page's " + ids.count);
                }
                int n = ids.passRun(Math.min(max, left));
                if (n > 0) {
                    run = true;
                    id = ids.runId;
                    return n;
                }
                refill(Math.min(CHUNK, Math.min(max, left)));
            }
            int n = Math.min(max, end - at);
            run = false;
            from = at;
            at += n;
            return n;
        }

        /// Whether the span [#take] handed out last is a run of one id.
        boolean run() {
            return run;
        }

        /// The id of the run [#take] handed out last.
        int id() {
            return id;
        }

        /// The buffer the span [#take] handed out last lies in, where it is not a run.
        int[] chunk() {
            return chunk;
        }

        /// Where the span [#take] handed out last starts in [#chunk()].
        int from() {
            return from;
        }

        /// Decodes the next `n` ids into the buffer.
        private void refill(int n) {
            if (chunk == null || chunk.length < n) {
                chunk = new int[n];
            }
            ids.read(chunk, 0, n);
            at = 0;
            end = n;
        }
    }
}
