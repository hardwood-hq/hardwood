/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

/// Interface for the vectorisable operations of the page read.
///
/// Implementations exist for scalar (fallback) and SIMD (Vector API) paths.
/// The SIMD implementation is loaded via multi-release JAR on Java 22+ when
/// Vector API is available.
public interface SimdOperations {

    /// Counts the entries `defLevels[i] == maxDef` for `i` in `[from, to)`. The bounds are
    /// explicit because level arrays are pooled and may run on past the page with stale entries.
    ///
    /// @param defLevels definition levels array
    /// @param from first entry to count over
    /// @param to entry after the last to count over
    /// @param maxDef maximum definition level (indicates non-null)
    /// @return count of non-null values
    int countNonNulls(int[] defLevels, int from, int to, int maxDef);
}
