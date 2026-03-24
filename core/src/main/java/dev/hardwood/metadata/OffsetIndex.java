/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

/// Offset index for a column chunk, providing page locations for direct lookup.
///
/// @param pageLocations locations of each data page in the column chunk
/// @see <a href="https://parquet.apache.org/docs/file-format/pageindex/">File Format – Page Index</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record OffsetIndex(List<PageLocation> pageLocations) {
}
