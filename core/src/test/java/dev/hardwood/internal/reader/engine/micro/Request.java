/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

/// One read of the file's bytes `[offset, offset + length)`, which planning cut from a row group's
/// column chunks ([Planning#coalesce]): it may hold the pages of several columns, and a page may
/// straddle two requests. `firstRow` is the lowest row of a page it holds, which ranks it.
record Request(int rowGroup, int index, long offset, long length, long firstRow) {

    long end() {
        return offset + length;
    }

    @Override
    public String toString() {
        return "r" + rowGroup + "." + index;
    }
}
