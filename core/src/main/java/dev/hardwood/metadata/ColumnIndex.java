/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

/// Column index for a column chunk, providing per-page min/max statistics for page-level filtering.
///
/// @param nullPages boolean list indicating which pages contain only null values
/// @param minValues per-page minimum values in the column's physical sort order
/// @param maxValues per-page maximum values in the column's physical sort order
/// @param boundaryOrder ordering of min/max values: UNORDERED, ASCENDING, or DESCENDING
/// @param nullCounts per-page null counts, or `null` if not available
/// @see <a href="https://parquet.apache.org/docs/file-format/pageindex/">File Format – Page Index</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record ColumnIndex(
        List<Boolean> nullPages,
        List<byte[]> minValues,
        List<byte[]> maxValues,
        BoundaryOrder boundaryOrder,
        List<Long> nullCounts) {

    /// Ordering of min/max values across pages.
    public enum BoundaryOrder {
        UNORDERED,
        ASCENDING,
        DESCENDING
    }

    /// Returns the number of pages described by this index.
    public int getPageCount() {
        return nullPages.size();
    }
}
