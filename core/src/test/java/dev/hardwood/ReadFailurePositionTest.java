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

import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Which row group, column and page a read failure says it happened in.
///
/// The cases here are the ones where the number the reader reports and the number a
/// person would count are easy to confuse: a pruned read, where the row groups still to
/// be read are no longer the file's own; a filtered read, where the pages still to be
/// read are not the chunk's own; and a nested column, whose leaf name is not its path.
class ReadFailurePositionTest {

    private static final Path FILTER_FIXTURE =
            Path.of("src/test/resources/filter_pushdown_int.parquet");

    /// Row group 2 of `filter_pushdown_int.parquet`, corrupted so reading it fails.
    /// `id` ascends across the file, so `id > 250` prunes row groups 0 and 1 and leaves
    /// this one first in the work list — a position that is not its index in the file.
    private static ByteBuffer withCorruptRowGroup2() throws Exception {
        byte[] bytes = Files.readAllBytes(FILTER_FIXTURE);
        long corruptOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(2).columns().get(0).metaData();
            corruptOffset = meta.dataPageOffset() + 1;
        }
        bytes[Math.toIntExact(corruptOffset)] = (byte) 0xFF;
        return ByteBuffer.wrap(bytes);
    }

    /// The row group a failure names is the file's, not the read's. A reader sent to
    /// "row group 0" of a file whose row group 0 was pruned before a byte of it was read
    /// would find nothing wrong there.
    @Test
    void aPrunedReadNamesTheRowGroupTheFileHas() throws Exception {
        ByteBuffer corrupted = withCorruptRowGroup2();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.buildColumnReader("id")
                         .filter(FilterPredicate.gt("id", 250L))
                         .build()) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id', page "
                                                            + "0] PageHeader has unknown page type: -1408");
    }

    /// The same, unfiltered: nothing is pruned, so the two numbers coincide and the
    /// message reads the same. Without this the case above could pass on a reader that
    /// reported the wrong number in both.
    @Test
    void anUnprunedReadNamesTheSameRowGroup() throws Exception {
        ByteBuffer corrupted = withCorruptRowGroup2();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.columnReader("id")) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id', page "
                                                            + "0] PageHeader has unknown page type: -1408");
    }

    /// The page a failure names is its ordinal in the column chunk, not its position among
    /// the pages the filter kept. Corrupting the last of ten pages and filtering to the
    /// rows only that page holds leaves it first in the work the reader does, so a reader
    /// counting what it walked would call it page 0.
    @Test
    void aFilteredReadNamesThePageTheChunkHas() throws Exception {
        Path source = Path.of("src/test/resources/column_index_pushdown.parquet");
        byte[] bytes = Files.readAllBytes(source);

        PageLocation lastPage;
        int pageCount;
        try (InputFile in = InputFile.of(ByteBuffer.wrap(bytes));
             ParquetFileReader reader = ParquetFileReader.open(in)) {
            ColumnChunk chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
            OffsetIndex offsets = OffsetIndexReader.read(new ThriftCompactReader(
                    in.readRange(chunk.offsetIndexOffset(), chunk.offsetIndexLength())));
            pageCount = offsets.pageLocations().size();
            lastPage = offsets.pageLocations().get(pageCount - 1);
        }
        assertThat(pageCount).as("the fixture must have pages to drop").isGreaterThan(1);
        bytes[Math.toIntExact(lastPage.offset()) + 1] = (byte) 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.buildColumnReader("id")
                         .filter(FilterPredicate.gtEq("id", lastPage.firstRowIndex()))
                         .build()) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column 'id', page "
                                                            + "9] PageHeader has unknown page type: -1408");
    }

    /// Planning a work item reads the page index, the dictionary and the chunk they sit in,
    /// and a failure in there is on no page at all. The plan being walked when it happens
    /// belongs to the row group before it, so a reader that answered "which page" from it
    /// would name one that read cleanly.
    @Test
    void aFailureWhilePlanningNamesNoPage() throws Exception {
        Path source = Path.of("src/test/resources/multi_row_group_page_index.parquet");
        byte[] bytes = Files.readAllBytes(source);
        long columnIndexOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(bytes)))) {
            assertThat(reader.getFileMetaData().rowGroups())
                    .as("the fixture must have a row group to plan after the first")
                    .hasSizeGreaterThan(2);
            columnIndexOffset = reader.getFileMetaData().rowGroups().get(2).columns().get(0)
                    .columnIndexOffset();
        }
        bytes[Math.toIntExact(columnIndexOffset)] = (byte) 0xFF;
        bytes[Math.toIntExact(columnIndexOffset) + 1] = (byte) 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.buildColumnReader("id")
                         .filter(FilterPredicate.gtEq("id", 0L))
                         .build()) {
                while (column.nextBatch()) {
                    // read as far as the row group whose page index is corrupt
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 2, column 'id'] "
                                                            + "Failed to parse the page index of column "
                                                            + "0: ColumnIndex field 15 \u2014 Unknown "
                                                            + "field type: 15");
    }

    /// A column's path, not its leaf name: two leaves in one file can share a name, and
    /// the accessor is documented to tell them apart.
    @Test
    void aNestedColumnIsNamedByItsPath() throws Exception {
        Path source = Path.of("src/test/resources/nested_struct_test.parquet");
        byte[] bytes = Files.readAllBytes(source);
        long corruptOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(2).metaData();
            corruptOffset = meta.dataPageOffset() + 1;
        }
        bytes[Math.toIntExact(corruptOffset)] = (byte) 0xFF;

        ByteBuffer corrupted = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 ColumnReader column = reader.columnReader("address.city")) {
                while (column.nextBatch()) {
                    // read to the corrupt page
                }
            }
        }).isInstanceOf(ParquetReadException.class).hasMessage("[<memory>: row group 0, column "
                                                            + "'address.city', page 0] PageHeader has "
                                                            + "unknown page type: -1408");
    }
}
