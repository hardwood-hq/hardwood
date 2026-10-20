/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.Arrays;

import dev.hardwood.Experimental;

/// The values of a `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` leaf in a [ColumnReader]'s
/// batch, from [ColumnReader#getBinaryValues()]. Value `i` is the `i`-th leaf value, `i` in
/// `[0, size())`. A null value has length 0 (see [ColumnReader#getLeafValidity()]), and
/// [#getBinary(int)] returns an empty array for it.
///
/// Value `i` occupies `[start(i), end(i))` of `buffer(i)`. A loop over a value's bytes reads that
/// range of the array:
///
/// ```java
/// BinaryValues values = column.getBinaryValues();
/// for (int i = 0; i < values.size(); i++) {
///     byte[] buffer = values.buffer(i);
///     for (int p = values.start(i); p < values.end(i); p++) {
///         consume(buffer[p]);
///     }
/// }
/// ```
///
/// `buffer(i)` is an array value `i` can be read from, valid as long as the batch is. Do not
/// write to it. Values may or may not share an array, and nothing is promised about which array
/// a value is read from or about bytes outside its range.
///
/// **This API is [Experimental]:** it may change in future releases without prior
/// deprecation.
@Experimental
public final class BinaryValues {

    private final byte[] bytes;
    private final int[] starts;
    private final int[] ends;

    /// @param starts the values' starts, of length the batch's value count
    /// @param ends the values' ends, of length the batch's value count
    BinaryValues(byte[] bytes, int[] starts, int[] ends) {
        this.bytes = bytes;
        this.starts = starts;
        this.ends = ends;
    }

    /// The number of values, [ColumnReader#getValueCount()].
    public int size() {
        return starts.length;
    }

    /// The array value `i` is read from.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`
    public byte[] buffer(int i) {
        checkValue(i);
        return bytes;
    }

    /// Where value `i` starts in [#buffer(int)], inclusive.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`
    public int start(int i) {
        return starts[i];
    }

    /// Where value `i` ends in [#buffer(int)], exclusive.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`
    public int end(int i) {
        return ends[i];
    }

    /// The length of value `i` in bytes.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`
    public int length(int i) {
        return ends[i] - starts[i];
    }

    /// Copies value `i` into `dst`, from `dstOffset` on.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`, or the value does not fit
    ///         in `dst` from `dstOffset` on
    public void copyTo(int i, byte[] dst, int dstOffset) {
        int start = starts[i];
        System.arraycopy(bytes, start, dst, dstOffset, ends[i] - start);
    }

    /// Whether value `i` equals the bytes `[offset, offset + length)` of `other`.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`, or the range is not
    ///         within `other`
    /// @throws IllegalArgumentException if `length` is negative
    public boolean contentEquals(int i, byte[] other, int offset, int length) {
        return Arrays.equals(bytes, starts[i], ends[i], other, offset, offset + length);
    }

    /// A copy of value `i`.
    ///
    /// @throws IndexOutOfBoundsException if `i` is not in `[0, size())`
    public byte[] getBinary(int i) {
        return Arrays.copyOfRange(bytes, starts[i], ends[i]);
    }

    private void checkValue(int i) {
        if (i < 0 || i >= starts.length) {
            throw new IndexOutOfBoundsException("Value " + i + " out of bounds for " + starts.length + " values");
        }
    }
}
