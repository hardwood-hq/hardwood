/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

/// A page of one column: `rows` rows from `firstRow` on, stored in the file's bytes
/// `[offset, offset + length)`.
record Page(int column, int index, long firstRow, int rows, long offset, int length) {

    long end() {
        return offset + length;
    }

    /// Whether the page has bytes in `[from, to)`.
    boolean overlaps(long from, long to) {
        return offset < to && end() > from;
    }

    @Override
    public String toString() {
        return "c" + column + "p" + index;
    }
}
