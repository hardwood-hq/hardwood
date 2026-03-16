/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Column chunk metadata.
 *
 * @param metaData column metadata
 * @param offsetIndexOffset file offset of the offset index for this column chunk, or {@code null} if absent
 * @param offsetIndexLength length of the offset index in bytes, or {@code null} if absent
 * @param columnIndexOffset file offset of the column index for this column chunk, or {@code null} if absent
 * @param columnIndexLength length of the column index in bytes, or {@code null} if absent
 * @see <a href="https://parquet.apache.org/docs/file-format/data-pages/columnchunks/">File Format – Column Chunks</a>
 * @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
 */
public record ColumnChunk(
        ColumnMetaData metaData,
        Long offsetIndexOffset,
        Integer offsetIndexLength,
        Long columnIndexOffset,
        Integer columnIndexLength) {
}
