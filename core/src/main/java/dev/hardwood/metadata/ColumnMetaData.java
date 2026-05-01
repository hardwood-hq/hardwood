/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;
import java.util.Map;

/// Metadata for a column chunk.
///
/// @param type physical (storage) type of the column
/// @param encodings list of encodings used in this column chunk (including dictionary encoding if present)
/// @param pathInSchema path from the root schema to this column
/// @param codec compression codec used for pages in this column chunk
/// @param numValues total number of values (including nulls) in this column chunk
/// @param totalUncompressedSize total uncompressed byte size of all pages in this column chunk
/// @param totalCompressedSize total compressed byte size of all pages in this column chunk (as stored on disk)
/// @param keyValueMetadata application-defined key-value metadata for this column, or an empty map if absent
/// @param dataPageOffset byte offset in the file where the first data page begins
/// @param dictionaryPageOffset byte offset in the file where the dictionary page begins, or `null` if there is no dictionary page
/// @param statistics column chunk statistics (min/max values, null count, distinct count), or `null` if absent
/// @param geospatialStatistics column chunk geospatial statistics (bounding box, geospatial types), or `null` if absent
/// @see <a href="https://parquet.apache.org/docs/file-format/data-pages/columnchunks/">File Format – Column Chunks</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record ColumnMetaData(
        PhysicalType type,
        List<Encoding> encodings,
        FieldPath pathInSchema,
        CompressionCodec codec,
        long numValues,
        long totalUncompressedSize,
        long totalCompressedSize,
        Map<String, String> keyValueMetadata,
        long dataPageOffset,
        Long dictionaryPageOffset,
        Statistics statistics,
        GeospatialStatistics geospatialStatistics) {
}
