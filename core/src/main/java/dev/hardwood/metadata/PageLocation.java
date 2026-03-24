/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Location of a data page within a column chunk.
///
/// @param offset absolute file offset of the page
/// @param compressedPageSize total page size in file including header
/// @param firstRowIndex index of the first row in this page within the row group
/// @see <a href="https://parquet.apache.org/docs/file-format/pageindex/">File Format – Page Index</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record PageLocation(long offset, int compressedPageSize, long firstRowIndex) {
}
