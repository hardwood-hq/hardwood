/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

/// Interface for vectorizable operations used in Parquet decoding.
///
/// Implementations exist for scalar (fallback) and SIMD (Vector API) paths.
/// The SIMD implementation is loaded via multi-release JAR on Java 22+ when
/// Vector API is available.
public interface SimdOperations {

    // ==================== Definition Level Operations ====================

    /// Count non-null values by counting entries where `defLevels[i] == maxDef`
    /// among the first `length` entries. The length is explicit because level
    /// arrays are pooled and may run on past the page with stale entries.
    ///
    /// @param defLevels definition levels array
    /// @param length number of leading entries to count over
    /// @param maxDef maximum definition level (indicates non-null)
    /// @return count of non-null values
    int countNonNulls(int[] defLevels, int length, int maxDef);

    // ==================== Dictionary Operations ====================

    /// Apply dictionary lookup for long values.
    ///
    /// @param output destination array for looked-up values
    /// @param dict dictionary array
    /// @param indices index array into dictionary
    /// @param count number of values to process
    void applyDictionaryLongs(long[] output, long[] dict, int[] indices, int count);

    /// Apply dictionary lookup for double values.
    void applyDictionaryDoubles(double[] output, double[] dict, int[] indices, int count);

    /// Apply dictionary lookup for int values.
    void applyDictionaryInts(int[] output, int[] dict, int[] indices, int count);

    /// Apply dictionary lookup for float values.
    void applyDictionaryFloats(float[] output, float[] dict, int[] indices, int count);
}
