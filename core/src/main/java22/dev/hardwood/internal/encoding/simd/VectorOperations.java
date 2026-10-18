/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

/// SIMD implementation of vectorizable operations using Java Vector API.
///
/// This implementation uses the incubator Vector API available in Java 22+
/// to accelerate the operations of the page read. It automatically uses
/// the preferred vector size for the current CPU (128-bit, 256-bit, or 512-bit).
///
/// Operations fall back to scalar processing for tail elements that don't
/// fill a complete vector.
public final class VectorOperations implements SimdOperations {

    // Use preferred species for best performance on current CPU
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;

    private static final int INT_VECTOR_LENGTH = INT_SPECIES.length();

    // Minimum batch size to use SIMD (amortize loop overhead)
    private static final int MIN_BATCH_SIZE = INT_VECTOR_LENGTH * 2;

    @Override
    public int countNonNulls(int[] defLevels, int from, int to, int maxDef) {
        // Use scalar for small ranges
        if (to - from < MIN_BATCH_SIZE) {
            return countNonNullsScalar(defLevels, from, to, maxDef);
        }

        IntVector maxDefVec = IntVector.broadcast(INT_SPECIES, maxDef);
        int count = 0;
        int i = from;

        // Main SIMD loop
        for (; i + INT_VECTOR_LENGTH <= to; i += INT_VECTOR_LENGTH) {
            IntVector vec = IntVector.fromArray(INT_SPECIES, defLevels, i);
            VectorMask<Integer> mask = vec.eq(maxDefVec);
            count += mask.trueCount();
        }

        // Scalar tail
        return count + countNonNullsScalar(defLevels, i, to, maxDef);
    }

    private static int countNonNullsScalar(int[] defLevels, int from, int to, int maxDef) {
        int count = 0;
        for (int i = from; i < to; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }
        return count;
    }
}
