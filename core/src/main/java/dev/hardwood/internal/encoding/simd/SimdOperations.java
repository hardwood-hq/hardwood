/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import java.util.BitSet;

/**
 * Interface for vectorizable operations used in Parquet decoding.
 *
 * <p>Implementations exist for scalar (fallback) and SIMD (Vector API) paths.
 * The SIMD implementation is loaded via multi-release JAR on Java 22+ when
 * Vector API is available.</p>
 */
public interface SimdOperations {

    // ==================== Definition Level Operations ====================

    /**
     * Count non-null values by counting entries where defLevels[i] == maxDef.
     *
     * @param defLevels definition levels array
     * @param maxDef maximum definition level (indicates non-null)
     * @return count of non-null values
     */
    int countNonNulls(int[] defLevels, int maxDef);

    /**
     * Mark null positions in a BitSet where defLevels[srcPos + i] < maxDefLevel.
     *
     * @param nulls BitSet to mark (may be null for required fields)
     * @param defLevels definition levels array
     * @param srcPos starting position in defLevels
     * @param destPos starting position in nulls BitSet
     * @param count number of elements to process
     * @param maxDefLevel maximum definition level
     */
    void markNulls(BitSet nulls, int[] defLevels, int srcPos, int destPos, int count, int maxDefLevel);

    // ==================== Bit Unpacking Operations ====================

    /**
     * Unpack bit-width 1 values from packed byte data.
     *
     * @param data source byte array
     * @param dataPos starting position in data
     * @param output destination int array
     * @param outPos starting position in output
     * @param count number of values to unpack (must be multiple of 8 for main loop)
     * @return number of bytes consumed from data
     */
    int unpackBitWidth1(byte[] data, int dataPos, int[] output, int outPos, int count);

    /**
     * Unpack values with bit widths 2-8 from packed byte data.
     *
     * @param data source byte array
     * @param dataPos starting position in data
     * @param output destination int array
     * @param outPos starting position in output
     * @param count number of values to unpack
     * @param bitWidth bits per value (2-8)
     * @return number of bytes consumed from data
     */
    int unpackBitWidthN(byte[] data, int dataPos, int[] output, int outPos, int count, int bitWidth);

    // ==================== Dictionary Operations ====================

    /**
     * Apply dictionary lookup for long values.
     *
     * @param output destination array for looked-up values
     * @param dict dictionary array
     * @param indices index array into dictionary
     * @param count number of values to process
     */
    void applyDictionaryLongs(long[] output, long[] dict, int[] indices, int count);

    /**
     * Apply dictionary lookup for double values.
     */
    void applyDictionaryDoubles(double[] output, double[] dict, int[] indices, int count);

    /**
     * Apply dictionary lookup for int values.
     */
    void applyDictionaryInts(int[] output, int[] dict, int[] indices, int count);

    /**
     * Apply dictionary lookup for float values.
     */
    void applyDictionaryFloats(float[] output, float[] dict, int[] indices, int count);
}
