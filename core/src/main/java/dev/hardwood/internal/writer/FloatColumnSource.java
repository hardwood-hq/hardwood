/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// A [ColumnSource] over a column's `FLOAT` values, read in page-sized primitive chunks.
public interface FloatColumnSource extends ColumnSource {

    /// Copies `length` values starting at `srcPos` into `dest` starting at `destPos`.
    void copyInto(int srcPos, float[] dest, int destPos, int length);
}
