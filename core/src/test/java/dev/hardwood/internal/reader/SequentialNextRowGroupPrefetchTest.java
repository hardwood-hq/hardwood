/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The first chunk of a [SequentialFetchPlan] in the next row group, which that row group's
/// prefetch fetches (#1324), is read once, whether the prefetch or the read of the row group gets
/// to it first.
class SequentialNextRowGroupPrefetchTest {

    private static final int ROWS_PER_ROW_GROUP = 1_000;

    @Test
    void theNextRowGroupsFirstStandaloneChunkIsReadOnce() throws Exception {
        // One column: its first chunk is a standalone handle.
        CountingInputFile file = read(write("a"));

        assertThat(file.reads())
                .filteredOn(read -> read.reason().contains("rg=1 col='a' seqChunk@0"))
                .hasSize(1);
    }

    @Test
    void theNextRowGroupsFirstSharedRegionIsReadOnce() throws Exception {
        // Two back-to-back columns: their first chunks are coalesced into one shared region.
        CountingInputFile file = read(write("a", "b"));

        assertThat(file.reads())
                .filteredOn(read -> read.reason().contains("rg=1 "))
                .hasSize(1);
    }

    /// Reads every row of `data`.
    private static CountingInputFile read(byte[] data) throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(data)));
        file.open();
        int rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.rowReader()) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(3);
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        assertThat(rows).isEqualTo(3 * ROWS_PER_ROW_GROUP);
        return file;
    }

    /// Three row groups of required `INT64` columns with the given names, without a page index.
    private static byte[] write(String... columns) throws Exception {
        FileSchema.Builder schemaBuilder = FileSchema.builder("prefetch");
        for (String column : columns) {
            schemaBuilder.addColumn(column, PhysicalType.INT64, RepetitionType.REQUIRED);
        }
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .rowGroupTargetRows(ROWS_PER_ROW_GROUP)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schemaBuilder.build(), config)) {
            RowWriter rowWriter = writer.rowWriter();
            for (int i = 0; i < 3 * ROWS_PER_ROW_GROUP; i++) {
                long value = i;
                rowWriter.writeRow(row -> {
                    for (String column : columns) {
                        row.setLong(column, value);
                    }
                });
            }
        }
        // Every file the writer produces has a page index; the plan under test is the one a
        // file without one takes.
        return FooterRewriter.withoutPageIndex(InMemoryFiles.toByteArray(out));
    }
}
