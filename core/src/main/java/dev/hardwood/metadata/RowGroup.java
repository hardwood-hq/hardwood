/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

/// Row group metadata.
///
/// @param columns metadata for each column chunk in this row group
/// @param totalByteSize total byte size of all uncompressed column data in this row group
/// @param numRows number of rows in this row group
/// @see <a href="https://parquet.apache.org/docs/file-format/metadata/#file-metadata">File Format – File Metadata</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record RowGroup(
        List<ColumnChunk> columns,
        long totalByteSize,
        long numRows) {
}
