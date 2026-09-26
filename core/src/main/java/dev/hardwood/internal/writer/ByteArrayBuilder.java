/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

/// A growable byte buffer that lends out its backing array instead of copying it.
///
/// This is the write path's one byte sink: a chunk's level streams take a byte through it per
/// entry, a `BYTE_ARRAY` column's values take their bytes through it per value, and every page
/// body is assembled in one. So it is on the per-value path of every column that has levels or
/// variable-width values, and what it costs per call is what those columns pay per value.
///
/// It is deliberately **not** a [java.io.ByteArrayOutputStream]. That class synchronizes every
/// `write`, which on this path is an uncontended monitor per level byte and per value — bought
/// for a thread-safety guarantee a buffer owned by one column chunk has no use for.
///
/// The lent array is valid only until the next write, and [#reset()] lets one builder serve
/// every page of a chunk rather than allocating and regrowing one per page.
final class ByteArrayBuilder {

    private byte[] buf;
    private int count;

    ByteArrayBuilder() {
        this(32);
    }

    ByteArrayBuilder(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Negative initial capacity: " + initialCapacity);
        }
        this.buf = new byte[initialCapacity];
    }

    /// Appends one byte, of which only the low eight bits are kept — matching the
    /// [java.io.OutputStream#write(int)] contract the level streams are written through.
    void write(int b) {
        if (count == buf.length) {
            grow(count + 1);
        }
        buf[count++] = (byte) b;
    }

    /// Appends `length` bytes of `source` starting at `offset`.
    void write(byte[] source, int offset, int length) {
        int needed = Math.addExact(count, length);
        if (needed > buf.length) {
            grow(needed);
        }
        System.arraycopy(source, offset, buf, count, length);
        count = needed;
    }

    /// Appends every byte of `source`.
    void writeBytes(byte[] source) {
        write(source, 0, source.length);
    }

    /// Empties the buffer, keeping the capacity it has grown to.
    void reset() {
        count = 0;
    }

    /// The backing array. Only the first [#length()] bytes are written data, and the array is
    /// replaced by any write that outgrows it.
    byte[] array() {
        return buf;
    }

    /// The number of bytes written so far.
    int length() {
        return count;
    }

    /// Extends the buffer by `length` bytes and returns the offset at which they begin, so an
    /// encoder can produce a section straight into the buffer that will carry it rather than
    /// into an array of its own for the caller to copy in.
    ///
    /// The reserved bytes hold whatever was there before; a caller must write all of them.
    ///
    /// Reserving may replace the backing array, so **take the offset into a local and fetch
    /// [#array()] afterwards** — `fill(array(), reserve(n))` evaluates the array reference
    /// before the growth and writes into the array that was just replaced.
    int reserve(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Cannot reserve a negative length: " + length);
        }
        int at = count;
        int needed = Math.addExact(count, length);
        if (needed > buf.length) {
            grow(needed);
        }
        count = needed;
        return at;
    }

    /// Grows the backing array to hold at least `needed` bytes, doubling up to
    /// [RowGroupBuffer#MAX_STORE_CAPACITY] so that a buffer filled a byte at a time is copied a
    /// bounded number of times however large it gets.
    private void grow(int needed) {
        if (needed > RowGroupBuffer.MAX_STORE_CAPACITY) {
            throw new IllegalStateException(
                    "A byte buffer cannot hold more than " + RowGroupBuffer.MAX_STORE_CAPACITY + " bytes");
        }
        long doubled = Math.min(2L * buf.length, RowGroupBuffer.MAX_STORE_CAPACITY);
        buf = Arrays.copyOf(buf, Math.max(needed, (int) doubled));
    }
}
