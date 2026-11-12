/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.util.Arrays;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// Builds a file of one required `INT64` column `v`, 1000 values in one `PLAIN` page, whose page
/// header declares an uncompressed size other than the page's.
public final class MisstatedPageFile {

    public static final int VALUES = 1_000;
    public static final int UNCOMPRESSED_SIZE = VALUES * Long.BYTES;
    /// Both encode in the same number of varint bytes as the true size, so the header keeps its
    /// length
    public static final int UNDERSTATED = UNCOMPRESSED_SIZE / 2;
    public static final int OVERSTATED = UNCOMPRESSED_SIZE + 100;

    private MisstatedPageFile() {
    }

    /// A file of `codec` whose data page header declares `declared` uncompressed bytes.
    public static byte[] build(CompressionCodec codec, int declared) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] values = new long[VALUES];
        for (int i = 0; i < VALUES; i++) {
            values[i] = i * 31L;
        }
        WriterConfig config = WriterConfig.builder()
                .codec(codec)
                .encoding(ColumnEncoding.PLAIN)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
        }
        byte[] file = InMemoryFiles.toByteArray(out);

        long pageOffset;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            pageOffset = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData().dataPageOffset();
        }
        // The page header opens with field 1 (type, i32), then field 2 (uncompressed_page_size, i32)
        int at = Math.toIntExact(pageOffset);
        assertThat(file[at]).as("field 1 header").isEqualTo((byte) 0x15);
        at += 2;
        assertThat(file[at]).as("field 2 header").isEqualTo((byte) 0x15);
        at++;
        byte[] actual = zigzagVarint(UNCOMPRESSED_SIZE);
        byte[] replacement = zigzagVarint(declared);
        assertThat(ByteBuffer.wrap(file, at, actual.length)).isEqualTo(ByteBuffer.wrap(actual));
        assertThat(replacement).hasSameSizeAs(actual);
        System.arraycopy(replacement, 0, file, at, replacement.length);
        return file;
    }

    private static byte[] zigzagVarint(int value) {
        int zigzag = (value << 1) ^ (value >> 31);
        ByteBuffer out = ByteBuffer.allocate(5);
        while ((zigzag & ~0x7F) != 0) {
            out.put((byte) ((zigzag & 0x7F) | 0x80));
            zigzag >>>= 7;
        }
        out.put((byte) zigzag);
        return Arrays.copyOf(out.array(), out.position());
    }
}
