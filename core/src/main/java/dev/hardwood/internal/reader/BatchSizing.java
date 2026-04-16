/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Computes optimal batch sizes for column data assembly.
public final class BatchSizing {

    private BatchSizing() {}

    /// Computes a batch size that keeps all column arrays for one batch within the L2 cache.
    ///
    /// Each batch allocates one primitive array per projected column. The total memory for a
    /// batch is approximately `batchSize * sum(bytesPerColumn)`. This method sizes the batch
    /// so that total stays under the target (6 MB), clamped to [`16 384`, `524 288`]
    /// rows.
    ///
    /// For example, 3 projected DOUBLE columns (8 bytes each = 24 bytes/row) yields
    /// `6 MB / 24 = 262 144` rows per batch.
    public static int computeOptimalBatchSize(ProjectedSchema projectedSchema) {
        // Initially target 6 MB (fits comfortably in L2 cache)
        long targetBytes = 6L * 1024 * 1024;
        int minBatch = 16384;
        int maxBatch = 524288;

        int bytesPerRow = 0;
        for (int i = 0; i < projectedSchema.getProjectedColumnCount(); i++) {
            bytesPerRow += columnByteWidth(projectedSchema.getProjectedColumn(i));
        }

        if (bytesPerRow == 0) {
            bytesPerRow = 8;
        }

        int batchSize = (int) (targetBytes / bytesPerRow);
        return Math.max(minBatch, Math.min(maxBatch, batchSize));
    }

    /// Returns the estimated byte width of a single value for the given column's physical type.
    /// Variable-length types use a 16-byte estimate (pointer + average payload).
    private static int columnByteWidth(ColumnSchema col) {
        return switch (col.type()) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case BOOLEAN -> 1;
            case INT96 -> 12;
            case BYTE_ARRAY -> 16;
            case FIXED_LEN_BYTE_ARRAY -> col.typeLength() != null ? col.typeLength() : 16;
        };
    }
}
