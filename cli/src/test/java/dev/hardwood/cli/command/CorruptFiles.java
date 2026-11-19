/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.LongStream;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

/// Parquet files that fail part-way through a read.
final class CorruptFiles {

    private static final int ROWS_PER_ROW_GROUP = 200_000;

    private CorruptFiles() {
    }

    /// Writes three row groups of [#ROWS_PER_ROW_GROUP] rows each, with a single `id`
    /// column counting from 0, and damages the last page of the last row group, so a read
    /// fails its CRC check. How many rows a reader delivers before the failure depends on
    /// how far its read-ahead has got, anywhere from none to every row before the damaged page.
    static Path withBadLastRowGroup(Path dir) throws IOException {
        Path file = dir.resolve("crc-bad.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            for (int rowGroup = 0; rowGroup < 3; rowGroup++) {
                long[] ids = LongStream.range((long) rowGroup * ROWS_PER_ROW_GROUP,
                        (long) (rowGroup + 1) * ROWS_PER_ROW_GROUP).toArray();
                writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
                writer.endRowGroup();
            }
        }

        ColumnMetaData lastChunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            lastChunk = reader.getFileMetaData().rowGroups().get(2).columns().get(0).metaData();
        }
        // The chunk's last bytes are the body of its last page, past its header.
        long position = lastChunk.dataPageOffset() + lastChunk.totalCompressedSize() - 8;
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer bytes = ByteBuffer.allocate(4);
            channel.read(bytes, position);
            for (int i = 0; i < bytes.limit(); i++) {
                bytes.put(i, (byte) ~bytes.get(i));
            }
            bytes.rewind();
            channel.write(bytes, position);
        }
        return file;
    }

    /// The error a read of [#withBadLastRowGroup]'s file fails with.
    static final String CRC_FAILURE = "Error reading file: [crc-bad.parquet: row group 2, column 'id', page 9]"
            + " CRC mismatch: expected 478e649 but computed 9ea5c6df";
}
