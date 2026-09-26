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
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;

/// The page-index slices a read fetched for one row group: the OffsetIndex and ColumnIndex of
/// the columns it needs, looked up by the column's ordinal in the file via [#forColumn(int)].
///
/// The slices are views of requests merged per structure by [CoalescedRanges]: ColumnIndex
/// slices only with ColumnIndex slices, OffsetIndex slices only with OffsetIndex slices.
/// [#addRanges] and [#slice] let one pair of [CoalescedRanges] serve several row groups.
public class RowGroupIndexBuffers {

    private final ColumnIndexBuffers[] columns;
    private final BitSet fetched;

    private RowGroupIndexBuffers(ColumnIndexBuffers[] columns, BitSet fetched) {
        this.columns = columns;
        this.fetched = fetched;
    }

    /// Returns the index buffers of the column at `columnIndex` in the file. A buffer is `null`
    /// where the file has no such index for the column, or where the read did not ask for that
    /// structure.
    ///
    /// @throws IllegalStateException if the read did not ask for the column's indexes
    public ColumnIndexBuffers forColumn(int columnIndex) {
        if (!fetched.get(columnIndex)) {
            throw new IllegalStateException(
                    "Page index of column " + columnIndex + " was not fetched for this read");
        }
        return columns[columnIndex];
    }

    /// Fetches the OffsetIndex and ColumnIndex of every column of a row group.
    ///
    /// @param inputFile the file to read from
    /// @param rowGroup the row group whose indexes to fetch
    public static RowGroupIndexBuffers fetch(InputFile inputFile, RowGroup rowGroup)
            throws IOException {
        BitSet all = new BitSet();
        all.set(0, rowGroup.columns().size());
        return fetch(inputFile, rowGroup, all, all);
    }

    /// Fetches the OffsetIndex of the columns in `offsetIndexColumns` and the ColumnIndex of
    /// those in `columnIndexColumns`, each structure's slices merged on their own.
    ///
    /// @param inputFile the file to read from
    /// @param rowGroup the row group whose indexes to fetch
    /// @param offsetIndexColumns the file ordinals of the columns whose OffsetIndex is needed
    /// @param columnIndexColumns the file ordinals of the columns whose ColumnIndex is needed
    public static RowGroupIndexBuffers fetch(InputFile inputFile, RowGroup rowGroup,
            BitSet offsetIndexColumns, BitSet columnIndexColumns) throws IOException {
        CoalescedRanges.Builder offsetIndexes = CoalescedRanges.builder();
        CoalescedRanges.Builder columnIndexes = CoalescedRanges.builder();
        addRanges(rowGroup, offsetIndexColumns, columnIndexColumns, offsetIndexes, columnIndexes);
        CoalescedRanges offsetIndexRanges = offsetIndexes.build();
        CoalescedRanges columnIndexRanges = columnIndexes.build();
        offsetIndexRanges.fetch(inputFile);
        columnIndexRanges.fetch(inputFile);
        return slice(rowGroup, offsetIndexColumns, columnIndexColumns, offsetIndexRanges,
                columnIndexRanges);
    }

    /// Adds the slices a row group needs to the builders of the two structures.
    ///
    /// @throws IndexOutOfBoundsException if a column ordinal lies outside the row group
    static void addRanges(RowGroup rowGroup, BitSet offsetIndexColumns, BitSet columnIndexColumns,
            CoalescedRanges.Builder offsetIndexes, CoalescedRanges.Builder columnIndexes) {
        List<ColumnChunk> chunks = rowGroup.columns();
        for (int c = offsetIndexColumns.nextSetBit(0); c >= 0; c = offsetIndexColumns.nextSetBit(c + 1)) {
            ColumnChunk chunk = chunks.get(c);
            if (chunk.offsetIndexOffset() != null) {
                offsetIndexes.add(chunk.offsetIndexOffset(), chunk.offsetIndexLength());
            }
        }
        for (int c = columnIndexColumns.nextSetBit(0); c >= 0; c = columnIndexColumns.nextSetBit(c + 1)) {
            ColumnChunk chunk = chunks.get(c);
            if (chunk.columnIndexOffset() != null) {
                columnIndexes.add(chunk.columnIndexOffset(), chunk.columnIndexLength());
            }
        }
    }

    /// Takes a row group's slices from the fetched ranges its [#addRanges] call planned.
    static RowGroupIndexBuffers slice(RowGroup rowGroup, BitSet offsetIndexColumns,
            BitSet columnIndexColumns, CoalescedRanges offsetIndexes, CoalescedRanges columnIndexes) {
        List<ColumnChunk> chunks = rowGroup.columns();
        ColumnIndexBuffers[] result = new ColumnIndexBuffers[chunks.size()];
        BitSet fetched = (BitSet) offsetIndexColumns.clone();
        fetched.or(columnIndexColumns);
        for (int c = fetched.nextSetBit(0); c >= 0; c = fetched.nextSetBit(c + 1)) {
            ColumnChunk chunk = chunks.get(c);
            ByteBuffer offsetIndex = offsetIndexColumns.get(c) && chunk.offsetIndexOffset() != null
                    ? offsetIndexes.slice(chunk.offsetIndexOffset(), chunk.offsetIndexLength())
                    : null;
            ByteBuffer columnIndex = columnIndexColumns.get(c) && chunk.columnIndexOffset() != null
                    ? columnIndexes.slice(chunk.columnIndexOffset(), chunk.columnIndexLength())
                    : null;
            result[c] = new ColumnIndexBuffers(offsetIndex, columnIndex);
        }
        return new RowGroupIndexBuffers(result, fetched);
    }
}
