/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

import java.util.List;

/**
 * Metadata for a column chunk.
 */
public record ColumnMetaData(
        PhysicalType type,
        List<Encoding> encodings,
        List<String> pathInSchema,
        CompressionCodec codec,
        long numValues,
        long totalUncompressedSize,
        long totalCompressedSize,
        long dataPageOffset,
        Long dictionaryPageOffset) {
}
