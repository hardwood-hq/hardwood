/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

/// Scalar (non-SIMD) implementation of vectorizable operations.
///
/// This is the fallback implementation used when the Vector API implementation is
/// not in effect (see [VectorSupport]). The implementations here use
/// loop unrolling and other scalar optimizations for reasonable performance.
public final class ScalarOperations implements SimdOperations {

    @Override
    public int countNonNulls(int[] defLevels, int from, int to, int maxDef) {
        int count0 = 0, count1 = 0, count2 = 0, count3 = 0;
        int i = from;

        // Process 8 elements at a time with 4 accumulators to minimize dependencies
        for (; i + 8 <= to; i += 8) {
            count0 += (defLevels[i] == maxDef ? 1 : 0) + (defLevels[i + 4] == maxDef ? 1 : 0);
            count1 += (defLevels[i + 1] == maxDef ? 1 : 0) + (defLevels[i + 5] == maxDef ? 1 : 0);
            count2 += (defLevels[i + 2] == maxDef ? 1 : 0) + (defLevels[i + 6] == maxDef ? 1 : 0);
            count3 += (defLevels[i + 3] == maxDef ? 1 : 0) + (defLevels[i + 7] == maxDef ? 1 : 0);
        }

        int count = count0 + count1 + count2 + count3;

        // Handle remaining elements
        for (; i < to; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }

        return count;
    }
}
