/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

/// The largest array the read allocates for data whose amount grows with what it reads.
public final class ArrayBound {

    /// Half of G1's smallest region (1 MB) less a 24-byte allowance for the array header (16 bytes
    /// by default, 12 with compact object headers). G1 allocates an object larger than half a heap
    /// region as a humongous object, in whole old-generation regions; G1 picks 1 MB regions for
    /// every heap below 4 GB, and allocating such arrays as a read goes starts concurrent cycles and
    /// fills the old generation with them.
    public static final int MAX_BYTES = 512 * 1024 - 24;

    private ArrayBound() {
    }
}
