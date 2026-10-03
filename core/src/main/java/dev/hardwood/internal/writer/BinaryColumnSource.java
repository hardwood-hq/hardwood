/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A [ColumnSource] over a column's binary values (`BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY`). A
/// value is addressed as a slice, `arrayAt(i)[offsetAt(i), offsetAt(i) + valueBytesAt(i))`, so a
/// source over one packed buffer hands its values to the encoder without a `byte[]` per value.
public interface BinaryColumnSource extends ColumnSource {

    /// The bytes the value at `index` holds, its length prefix excluded. Where the position holds
    /// no value it is 0 or the bytes a packed source spans there, which the writer only ever sums
    /// as an upper bound. Every other column's width follows from the schema; this is the one that
    /// has to be read, and reading it is what lets the writer bound a slice before appending it.
    int valueBytesAt(int index);

    /// The array holding the value at `index`. Only called at a position that holds a value.
    byte[] arrayAt(int index);

    /// Where the value at `index` starts in [#arrayAt]. Only called at a position that holds a
    /// value.
    int offsetAt(int index);
}
