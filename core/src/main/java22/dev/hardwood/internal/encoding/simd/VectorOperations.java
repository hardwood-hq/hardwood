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
/// to accelerate common Parquet decoding operations. It automatically uses
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
    public int countNonNulls(int[] defLevels, int length, int maxDef) {
        // Use scalar for small arrays
        if (length < MIN_BATCH_SIZE) {
            return countNonNullsScalar(defLevels, length, maxDef);
        }

        IntVector maxDefVec = IntVector.broadcast(INT_SPECIES, maxDef);
        int count = 0;
        int i = 0;

        // Main SIMD loop
        for (; i + INT_VECTOR_LENGTH <= length; i += INT_VECTOR_LENGTH) {
            IntVector vec = IntVector.fromArray(INT_SPECIES, defLevels, i);
            VectorMask<Integer> mask = vec.eq(maxDefVec);
            count += mask.trueCount();
        }

        // Scalar tail
        for (; i < length; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }

        return count;
    }

    private int countNonNullsScalar(int[] defLevels, int length, int maxDef) {
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void applyDictionaryLongs(long[] output, long[] dict, int[] indices, int count) {
        if (count < MIN_BATCH_SIZE) {
            applyDictionaryLongsScalar(output, dict, indices, count);
            return;
        }

        // Dictionary lookups with gather are challenging in Vector API
        // since true gather requires indices to be in a vector.
        // For now, use unrolled scalar which is actually quite efficient
        // for dictionary lookups due to L1 cache hits on small dictionaries.
        applyDictionaryLongsScalar(output, dict, indices, count);
    }

    private void applyDictionaryLongsScalar(long[] output, long[] dict, int[] indices, int count) {
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    @Override
    public void applyDictionaryDoubles(double[] output, double[] dict, int[] indices, int count) {
        // Similar to longs - unrolled scalar is efficient for cache-friendly access
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    @Override
    public void applyDictionaryInts(int[] output, int[] dict, int[] indices, int count) {
        if (count < MIN_BATCH_SIZE) {
            applyDictionaryIntsScalar(output, dict, indices, count);
            return;
        }

        // For small dictionaries (common case), try SIMD gather-like pattern
        // using rearrange operations if dictionary fits in vector
        if (dict.length <= INT_VECTOR_LENGTH) {
            applyDictionaryIntsSmallDict(output, dict, indices, count);
            return;
        }

        applyDictionaryIntsScalar(output, dict, indices, count);
    }

    private void applyDictionaryIntsSmallDict(int[] output, int[] dict, int[] indices, int count) {
        // For dictionaries smaller than vector length, we can use rearrange
        // This is a simplified approach - full implementation would use proper shuffles

        // Pad dictionary to vector length
        int[] paddedDict = new int[INT_VECTOR_LENGTH];
        System.arraycopy(dict, 0, paddedDict, 0, dict.length);
        IntVector dictVec = IntVector.fromArray(INT_SPECIES, paddedDict, 0);

        int i = 0;
        // Process vector-length values at a time using lane extraction
        // Note: This is a simplified approach; true gather would be more complex
        for (; i + INT_VECTOR_LENGTH <= count; i += INT_VECTOR_LENGTH) {
            // Load indices and look up each one
            // (Vector API doesn't have simple gather, so we do lane-by-lane)
            for (int j = 0; j < INT_VECTOR_LENGTH; j++) {
                output[i + j] = dict[indices[i + j]];
            }
        }

        // Scalar tail
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    private void applyDictionaryIntsScalar(int[] output, int[] dict, int[] indices, int count) {
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }

    @Override
    public void applyDictionaryFloats(float[] output, float[] dict, int[] indices, int count) {
        int i = 0;
        for (; i + 4 <= count; i += 4) {
            output[i] = dict[indices[i]];
            output[i + 1] = dict[indices[i + 1]];
            output[i + 2] = dict[indices[i + 2]];
            output[i + 3] = dict[indices[i + 3]];
        }
        for (; i < count; i++) {
            output[i] = dict[indices[i]];
        }
    }
}
