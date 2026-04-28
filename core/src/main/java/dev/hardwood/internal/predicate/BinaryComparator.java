/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;

/// Byte array comparison utilities for filter predicate evaluation.
///
/// Provides unsigned lexicographic comparison (for BYTE_ARRAY) and signed
/// two's complement comparison (for FIXED_LEN_BYTE_ARRAY decimals).
public final class BinaryComparator {

    private BinaryComparator() {
    }

    /// Compare two byte arrays lexicographically (unsigned).
    /// This matches Parquet's binary comparison semantics for BYTE_ARRAY statistics.
    ///
    /// @return negative if a < b, zero if equal, positive if a > b
    public static int compareUnsigned(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    /// Compare two same-length byte arrays as big-endian two's complement signed values.
    /// Used for `FIXED_LEN_BYTE_ARRAY` decimals where the high bit is the sign bit.
    ///
    /// @return negative if a < b, zero if equal, positive if a > b
    public static int compareSigned(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                    "Signed binary comparison requires same-length arrays: " + a.length + " vs " + b.length);
        }
        if (a.length == 0) {
            return 0;
        }
        // First byte: compare as signed to handle the sign bit
        int cmp = a[0] - b[0];
        if (cmp != 0) {
            return cmp;
        }
        // Remaining bytes: compare as unsigned
        return Arrays.compareUnsigned(a, 1, a.length, b, 1, a.length);
    }
}
