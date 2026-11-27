/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A data page whose header declares a longer body than the page its chunk's OffsetIndex locates
/// fails the read with the column and page named, for a flat column and for a nested one, which
/// are decoded on different paths.
class OverstatedPageBodyTest {

    private static final int ROWS = 100;

    @Test
    void aFlatColumnsOverstatedBodyFailsNamingItsPage() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .primitive("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] values = new long[ROWS];
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, plain())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
        }

        assertOverstatedBodyFails(InMemoryFiles.toByteArray(out), "v");
    }

    @Test
    void aNestedColumnsOverstatedBodyFailsNamingItsPage() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        int[] offsets = new int[ROWS + 1];
        for (int i = 0; i <= ROWS; i++) {
            offsets[i] = i;
        }
        int[] elements = new int[ROWS];
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, plain())) {
            writer.columnWriter().writeBatch(batch -> batch.list("v", offsets).ints("v.list.element", elements));
        }

        assertOverstatedBodyFails(InMemoryFiles.toByteArray(out), "v.list.element");
    }

    private static WriterConfig plain() {
        return WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build();
    }

    /// Raises the `compressed_page_size` of the file's first data page by one, which keeps the
    /// header's length, and reads every column.
    private static void assertOverstatedBodyFails(byte[] file, String column) throws Exception {
        long pageOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            ColumnMetaData metaData = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            pageOffset = metaData.dataPageOffset();
        }
        int at = Math.toIntExact(pageOffset);
        DataPage page = DataPage.parse(ByteBuffer.wrap(file, at, file.length - at).slice(), 0);
        int body = page.header().compressedPageSize();
        // field 1 (type) and field 2 (uncompressed_page_size), then field 3's header
        int field3 = at + 2 + varintLength(file, at + 3) + 1;
        assertThat(file[field3]).as("field 3 header").isEqualTo((byte) 0x15);
        byte[] actual = zigzagVarint(body);
        byte[] overstated = zigzagVarint(body + 1);
        assertThat(overstated).hasSameSizeAs(actual);
        System.arraycopy(overstated, 0, file, field3 + 1, overstated.length);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            assertThatThrownBy(() -> {
                while (columns.nextBatch()) {
                    // read through
                }
            })
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("[<memory>: row group 0, column '" + column + "', page 0] Page header declares a body of "
                            + (body + 1) + " bytes, but its page holds " + body + " bytes after its "
                            + page.headerSize() + "-byte header");
        }
    }

    private static int varintLength(byte[] bytes, int at) {
        int length = 1;
        while ((bytes[at + length - 1] & 0x80) != 0) {
            length++;
        }
        return length;
    }

    private static byte[] zigzagVarint(int value) {
        int zigzag = (value << 1) ^ (value >> 31);
        ByteBuffer out = ByteBuffer.allocate(5);
        while ((zigzag & ~0x7F) != 0) {
            out.put((byte) ((zigzag & 0x7F) | 0x80));
            zigzag >>>= 7;
        }
        out.put((byte) zigzag);
        byte[] bytes = new byte[out.position()];
        out.flip().get(bytes);
        return bytes;
    }
}
