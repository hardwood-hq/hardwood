/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A flat page that holds fewer values than its metadata gives its placement fails the read before
/// it fills anything (READ_PIPELINE.md, Decoding): the OffsetIndex gives the first of two pages of
/// 100 values 105 rows, and no batch is published.
class PageRowCountTest {

    @ParameterizedTest
    @ValueSource(strings = { "v", "s" })
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aPageHoldingFewerValuesThanItsMetadataGivesFailsTheRead(String columnName) throws Exception {
        byte[] file = withFirstPageOf105Rows(write(), columnName);
        List<Integer> published = new ArrayList<>();

        // One decode thread decodes the pages in row order, so the first page's failure is raised
        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(1);
                 ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                 ColumnReader column = reader.buildColumnReader(columnName).batchSize(64).build()) {
                while (column.nextBatch()) {
                    published.add(column.getRecordCount());
                }
            }
        }).isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column '" + columnName + "', page 0] "
                        + "Page holds 100 values, but its metadata gives it 105 rows");
        assertThat(published).isEmpty();
    }

    @Test
    void theUnpatchedFileReadsInFull() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(write())));
             ColumnReader column = reader.buildColumnReader("s").batchSize(64).build()) {
            long rows = 0;
            while (column.nextBatch()) {
                rows += column.getRecordCount();
            }
            assertThat(rows).isEqualTo(200);
        }
    }

    /// `file` with the second page of `columnName`'s OffsetIndex starting at row 105 instead of 100.
    /// The value is encoded in the same number of bytes, so the index is patched in place.
    private static byte[] withFirstPageOf105Rows(byte[] file, String columnName) throws Exception {
        ColumnChunk chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            chunk = reader.getFileMetaData().rowGroups().getFirst().columns().stream()
                    .filter(c -> c.metaData().pathInSchema().leafName().equals(columnName))
                    .findFirst().orElseThrow();
        }
        // PageLocation.first_row_index (field 3, i64, the field after compressed_page_size) of 100,
        // a zigzag varint, followed by the struct's stop
        byte[] pattern = { 0x16, (byte) 0xC8, 0x01, 0x00 };
        int start = Math.toIntExact(chunk.offsetIndexOffset());
        int end = start + chunk.offsetIndexLength();
        int found = -1;
        for (int i = start; i + pattern.length <= end; i++) {
            if (matches(file, i, pattern)) {
                assertThat(found).as("PageLocations starting at row 100").isEqualTo(-1);
                found = i;
            }
        }
        assertThat(found).as("PageLocation starting at row 100").isNotEqualTo(-1);
        byte[] patched = file.clone();
        // 105, zigzag encoded
        patched[found + 1] = (byte) 0xD2;
        return patched;
    }

    private static boolean matches(byte[] bytes, int at, byte[] pattern) {
        for (int i = 0; i < pattern.length; i++) {
            if (bytes[at + i] != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    /// One row group of 200 rows in pages of 100: a required `INT64` column `v` and a required
    /// `BYTE_ARRAY` column `s`, both with an OffsetIndex.
    private static byte[] write() throws Exception {
        long[] longs = new long[200];
        byte[][] bytes = new byte[200][];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = i;
            bytes[i] = ("value-" + i).getBytes(StandardCharsets.UTF_8);
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        FileSchema schema = FileSchema.builder("pages")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().pageTargetRows(100).build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", longs).bytes("s", bytes));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
