/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A read-only, bulk view over a column's `INT32` values that the writer pulls from
/// while packing pages. The writer never reads a value at a time: it asks for
/// page-sized ranges via [#copyInto], so a source implemented over a foreign columnar
/// container (an Arrow vector, an off-heap buffer) is copied in bounded, primitive
/// chunks rather than one boxed element at a time.
///
/// This is the internal seam behind the public `ColumnBatch` primitive-array setters. A
/// public `ColumnVector` SPI over the same shape — letting callers write from their own
/// containers without an intervening copy — is a later additive layer.
public interface IntColumnSource {

    /// The number of values in this source.
    int size();

    /// Copies `length` values starting at `srcPos` into `dest` starting at `destPos`.
    ///
    /// @param srcPos index of the first value to copy from this source
    /// @param dest destination array
    /// @param destPos index in `dest` at which to place the first value
    /// @param length number of values to copy
    void copyInto(int srcPos, int[] dest, int destPos, int length);
}
