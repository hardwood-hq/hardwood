/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.internal.thrift.ThriftStructBuilder;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An OffsetIndex that lists no page for a column chunk with values is malformed. A read
/// planned from it fetches none of the chunk's pages, so the column comes back short: on its
/// own it silently loses the row group's values, next to a sibling column its batches no
/// longer line up. Page filtering over such an index keeps no page, dropping the row group.
class EmptyOffsetIndexTest {

    private static final Path FIXTURE = Path.of("src/test/resources/multi_row_group_page_index.parquet");

    /// The fixture with column `id` of row group 0 given an OffsetIndex whose
    /// `page_locations` is empty, and, when `emptyColumnIndex` is set, a ColumnIndex
    /// describing no page either, so the two indexes agree on the page count.
    ///
    /// Each replacement is shorter than the struct it overwrites; the bytes after its
    /// stop field are left in place and never read.
    private static ByteBuffer withEmptyOffsetIndex(boolean emptyColumnIndex) throws Exception {
        byte[] bytes = Files.readAllBytes(FIXTURE);
        ColumnChunk chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
        }
        assertThat(chunk.metaData().numValues()).as("the chunk must have values").isPositive();

        byte[] offsetIndex = new ThriftStructBuilder()
                .field(1, FieldType.LIST).structList()
                .stop().build();
        System.arraycopy(offsetIndex, 0, bytes, Math.toIntExact(chunk.offsetIndexOffset()), offsetIndex.length);

        if (emptyColumnIndex) {
            byte[] columnIndex = new ThriftStructBuilder()
                    .field(1, FieldType.LIST).boolList()
                    .field(2, FieldType.LIST).binaryList()
                    .field(3, FieldType.LIST).binaryList()
                    .field(4, FieldType.I32).i32(0)
                    .stop().build();
            System.arraycopy(columnIndex, 0, bytes, Math.toIntExact(chunk.columnIndexOffset()), columnIndex.length);
        }
        return ByteBuffer.wrap(bytes);
    }

    @Test
    void anUnfilteredReadRejectsTheIndex() throws Exception {
        ByteBuffer file = withEmptyOffsetIndex(false);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                 RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'id'] Failed to compute the fetch plan:"
                        + " Malformed Parquet metadata: OffsetIndex.page_locations is empty but the"
                        + " column chunk has 1000 values");
    }

    @Test
    void aSingleColumnReadRejectsTheIndex() throws Exception {
        ByteBuffer file = withEmptyOffsetIndex(false);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                 ColumnReader column = reader.columnReader("id")) {
                while (column.nextBatch()) {
                    // read every row group
                }
            }
        }).isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'id'] Failed to compute the fetch plan:"
                        + " Malformed Parquet metadata: OffsetIndex.page_locations is empty but the"
                        + " column chunk has 1000 values");
    }

    /// With the ColumnIndex emptied too, the two indexes agree on the page count, so only
    /// the check against the chunk's values catches the index before page filtering keeps
    /// none of the row group.
    @Test
    void aFilteredReadRejectsTheIndex() throws Exception {
        ByteBuffer file = withEmptyOffsetIndex(true);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                 RowReader rows = reader.buildRowReader().filter(FilterPredicate.gtEq("id", 0L)).build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'id'] Failed to parse the page index of"
                        + " column 0: Malformed Parquet metadata: OffsetIndex.page_locations is empty"
                        + " but the column chunk has 1000 values");
    }
}
