/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// Buffers the column chunks of a single row group. Values are appended in aligned
/// ranges across all columns; at flush the column chunks are written contiguously in
/// schema order, each recording the file offset at which its first page lands.
public final class RowGroupBuffer {

    private final FileSchema schema;
    private final ColumnChunkBuffer[] columns;
    private int rowCount;

    /// @param schema the flat file schema
    /// @param pageValues maximum number of rows per data page
    public RowGroupBuffer(FileSchema schema, int pageValues) {
        this.schema = schema;
        this.columns = new ColumnChunkBuffer[schema.getColumnCount()];
        for (int c = 0; c < columns.length; c++) {
            columns[c] = new ColumnChunkBuffer(pageValues, schema.getColumn(c).maxDefinitionLevel());
        }
    }

    /// Appends the same row range to every column and advances the row count.
    ///
    /// @param sources one value source per column, in schema order
    /// @param shredder bound to the current batch; supplies each levelled column's
    ///        definition levels
    /// @param from index of the first row to append
    /// @param count number of rows to append
    public void appendRows(IntColumnSource[] sources, RecordShredder shredder, int from, int count) {
        for (int c = 0; c < columns.length; c++) {
            columns[c].append(sources[c], shredder, c, from, count);
        }
        rowCount += count;
    }

    /// The number of rows buffered so far.
    public int rowCount() {
        return rowCount;
    }

    /// Whether no rows have been buffered.
    public boolean isEmpty() {
        return rowCount == 0;
    }

    /// Writes the buffered column chunks to `out` in schema order and returns the row
    /// group's metadata.
    public RowGroup flushTo(OutputFile out) throws IOException {
        List<ColumnChunk> chunks = new ArrayList<>(columns.length);
        long totalByteSize = 0;
        for (int c = 0; c < columns.length; c++) {
            long dataPageOffset = out.position();
            ColumnMetaData meta = columns[c].flushTo(out, schema.getColumn(c), dataPageOffset);
            chunks.add(new ColumnChunk(meta, null, null, null, null));
            totalByteSize += meta.totalUncompressedSize();
        }
        return new RowGroup(chunks, totalByteSize, rowCount);
    }
}
