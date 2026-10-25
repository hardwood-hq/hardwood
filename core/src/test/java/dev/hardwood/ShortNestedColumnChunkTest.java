/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A file whose one row group has two rows, and whose column `b` of a list of structs holds one
/// of them: a read of the column fails, alone or with the others, as its placement ends short of
/// the row group.
class ShortNestedColumnChunkTest {

    private static byte[] file;

    @BeforeAll
    static void writeFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .list("foo", RepetitionType.OPTIONAL, element -> element.struct(RepetitionType.OPTIONAL,
                        struct -> struct
                                .addColumn("a", PhysicalType.INT32, RepetitionType.OPTIONAL)
                                .addColumn("b", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                        LogicalType.string())))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int rows : new int[] { 2, 1 }) {
                for (int i = 0; i < rows; i++) {
                    writer.rowWriter().writeRow(row -> row.setList("foo", list -> list
                            .addStruct(element -> element.setInt("a", 1).setString("b", "x"))));
                }
                writer.endRowGroup();
            }
        }
        file = FooterRewriter.rewrite(InMemoryFiles.toByteArray(out),
                ShortNestedColumnChunkTest::withBOfTheSecondRowGroup);
    }

    private static final String SHORT_OF_THE_ROW_GROUP = "[<memory>: row group 0, column 'foo.list.element.b', "
            + "page 0] Batch plan mismatch: the column holds 1 of the 2 rows the plan takes from the row group";

    @Test
    void columnReadersFail() {
        assertThatThrownBy(ShortNestedColumnChunkTest::readColumns)
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(SHORT_OF_THE_ROW_GROUP);
    }

    @Test
    void columnReaderOfTheShortColumnFails() {
        assertThatThrownBy(ShortNestedColumnChunkTest::readColumnB)
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(SHORT_OF_THE_ROW_GROUP);
    }

    @Test
    void rowReaderFails() {
        assertThatThrownBy(ShortNestedColumnChunkTest::readRows)
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(SHORT_OF_THE_ROW_GROUP);
    }

    private static void readColumnB() throws IOException {
        try (ParquetFileReader reader = open();
             ColumnReader column = reader.columnReader("foo.list.element.b")) {
            while (column.nextBatch()) {
                // read to the end
            }
        }
    }

    private static void readColumns() throws IOException {
        try (ParquetFileReader reader = open();
             ColumnReaders readers = reader.columnReaders(ColumnProjection.all())) {
            while (readers.nextBatch()) {
                // read to the end
            }
        }
    }

    private static void readRows() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    private static ParquetFileReader open() throws IOException {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
    }

    /// `metaData` with only its first row group, whose chunk of column `b` is that of the second,
    /// holding one row of the first's two.
    private static FileMetaData withBOfTheSecondRowGroup(FileMetaData metaData) {
        RowGroup first = metaData.rowGroups().get(0);
        ColumnChunk shorterB = metaData.rowGroups().get(1).columns().get(1);
        return new FileMetaData(metaData.version(), metaData.schema(), first.numRows(),
                List.of(new RowGroup(List.of(first.columns().get(0), shorterB), first.totalByteSize(),
                        first.numRows())),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }
}
