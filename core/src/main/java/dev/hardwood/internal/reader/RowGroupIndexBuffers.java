/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;

/// Index buffers for all columns in a single row group.
///
/// Created by a single `readRange()` call spanning the contiguous
/// index region in the Parquet footer. Individual column indexes are
/// accessed by their original column index via [#forColumn(int)].
public class RowGroupIndexBuffers {

    private final ColumnIndexBuffers[] columns;

    private RowGroupIndexBuffers(ColumnIndexBuffers[] columns) {
        this.columns = columns;
    }

    /// Returns the index buffers for the given original column index, or `null`
    /// if no indexes were fetched for that column.
    public ColumnIndexBuffers forColumn(int columnIndex) {
        return (columnIndex < columns.length) ? columns[columnIndex] : null;
    }

    /// Fetches all offset/column indexes for a row group in a single
    /// `readRange()` call.
    ///
    /// The index entries for all columns in a row group are stored
    /// contiguously in the Parquet footer, so one read covers them all.
    /// The cost of including non-projected columns is negligible (a few KB
    /// of extra metadata) compared to the cost of an additional round-trip.
    ///
    /// @param inputFile the file to read from
    /// @param rowGroup  the row group whose indexes to fetch
    public static RowGroupIndexBuffers fetch(InputFile inputFile,
            RowGroup rowGroup) throws IOException {

        List<ColumnChunk> allColumns = rowGroup.columns();

        long minOffset = Long.MAX_VALUE;
        long maxEnd = Long.MIN_VALUE;
        for (ColumnChunk col : allColumns) {
            if (col.offsetIndexOffset() != null) {
                minOffset = Math.min(minOffset, col.offsetIndexOffset());
                maxEnd = Math.max(maxEnd,
                        col.offsetIndexOffset() + col.offsetIndexLength());
            }
            if (col.columnIndexOffset() != null) {
                minOffset = Math.min(minOffset, col.columnIndexOffset());
                maxEnd = Math.max(maxEnd,
                        col.columnIndexOffset() + col.columnIndexLength());
            }
        }

        ColumnIndexBuffers[] result = new ColumnIndexBuffers[allColumns.size()];
        if (minOffset == Long.MAX_VALUE) {
            return new RowGroupIndexBuffers(result);
        }

        ByteBuffer indexRegion = inputFile.readRange(minOffset, Math.toIntExact(maxEnd - minOffset));

        for (int i = 0; i < allColumns.size(); i++) {
            ColumnChunk col = allColumns.get(i);
            ByteBuffer oi = null;
            ByteBuffer ci = null;
            if (col.offsetIndexOffset() != null) {
                int relOffset = Math.toIntExact(col.offsetIndexOffset() - minOffset);
                oi = indexRegion.slice(relOffset, col.offsetIndexLength());
            }
            if (col.columnIndexOffset() != null) {
                int relOffset = Math.toIntExact(col.columnIndexOffset() - minOffset);
                ci = indexRegion.slice(relOffset, col.columnIndexLength());
            }
            result[i] = new ColumnIndexBuffers(oi, ci);
        }
        return new RowGroupIndexBuffers(result);
    }
}
