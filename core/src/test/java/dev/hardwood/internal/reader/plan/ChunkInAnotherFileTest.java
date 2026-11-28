/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A row group whose chunk of one column is stored in another file, the split-file layout: a read
/// that decodes the column fails, naming the column by its ordinal in the file, as does one a byte
/// range places, and a read that neither projects nor filters on it reads the other columns.
class ChunkInAnotherFileTest {

    private static final String FAILURE = "[<memory>] Cannot read column 1 in row group 0: Column chunk stores its"
            + " data in a separate file ('elsewhere.parquet'); the split-file layout is not supported";

    @Test
    void aColumnReadOfAnotherColumnReadsIt() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file())));
             ColumnReader column = reader.buildColumnReader("a").build()) {
            List<Long> values = new ArrayList<>();
            while (column.nextBatch()) {
                for (int i = 0; i < column.getRecordCount(); i++) {
                    values.add(column.getLongs()[i]);
                }
            }
            assertThat(values).containsExactly(0L, 1L, 2L, 3L);
        }
    }

    @Test
    void aRowReadProjectingAnotherColumnReadsIt() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file())));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("a")).build()) {
            List<Long> values = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getLong("a"));
            }
            assertThat(values).containsExactly(0L, 1L, 2L, 3L);
        }
    }

    @Test
    void aReadOfTheColumnFails() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file())))) {
            assertThatThrownBy(() -> {
                try (ColumnReader column = reader.buildColumnReader("b").build()) {
                    while (column.nextBatch()) {
                        column.getLongs();
                    }
                }
            }).isExactlyInstanceOf(UnsupportedOperationException.class).hasMessage(FAILURE);
        }
    }

    @Test
    void aReadFilteringOnTheColumnFails() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file())))) {
            assertThatThrownBy(() -> {
                try (ColumnReader column = reader.buildColumnReader("a").filter(FilterPredicate.eq("b", 12L))
                        .build()) {
                    while (column.nextBatch()) {
                        column.getLongs();
                    }
                }
            }).isExactlyInstanceOf(UnsupportedOperationException.class).hasMessage(FAILURE);
        }
    }

    /// A byte range places a row group by the extents of all of its chunks, so even a read of
    /// another column cannot place it.
    @Test
    void aByteRangeCannotPlaceTheRowGroup() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file())))) {
            assertThatThrownBy(() -> reader.buildColumnReader("a")
                    .filter(RowGroupPredicate.byteRange(0, Long.MAX_VALUE)).build())
                    .isExactlyInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("[<memory>] Cannot place row group 0 in a byte range: Column chunk stores its data"
                            + " in a separate file ('elsewhere.parquet'); the split-file layout is not supported");
        }
    }

    /// One row group of four rows of required `INT64` columns `a` and `b`, `b`'s chunk stored in
    /// another file.
    private static byte[] file() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, FileSchema.builder("longs")
                .primitive("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .primitive("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build())) {
            writer.columnWriter().writeBatch(batch -> {
                batch.longs("a", new long[]{ 0, 1, 2, 3 });
                batch.longs("b", new long[]{ 10, 11, 12, 13 });
            });
        }
        return FooterRewriter.rewrite(InMemoryFiles.toByteArray(out), metaData -> {
            RowGroup rowGroup = metaData.rowGroups().get(0);
            ColumnChunk b = rowGroup.columns().get(1);
            List<ColumnChunk> columns = List.of(rowGroup.columns().get(0), new ColumnChunk(b.metaData(),
                    b.offsetIndexOffset(), b.offsetIndexLength(), b.columnIndexOffset(), b.columnIndexLength(),
                    "elsewhere.parquet"));
            return new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(),
                    List.of(new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows())),
                    metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
        });
    }
}
