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

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for [OutputFile#inMemory()] and the [BufferOutputFile] it returns.
///
/// What these assert is the pairing the destination exists for: the buffer
/// [BufferOutputFile#buffer()] returns can be passed to [InputFile#of(ByteBuffer)], so a file
/// can be written and read back without a filesystem.
class BufferOutputFileTest {

    private static FileSchema schema() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
    }

    private static BufferOutputFile writeThreeRows() throws Exception {
        BufferOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema())) {
            RowWriter rows = writer.rowWriter();
            for (long id = 1; id <= 3; id++) {
                long value = id;
                rows.writeRow(row -> row.setLong("id", value));
            }
        }
        return out;
    }

    @Test
    void writesAFileThatIsReadBackFromTheBuffer() throws Exception {
        BufferOutputFile out = writeThreeRows();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                RowReader rows = reader.rowReader()) {
            for (long id = 1; id <= 3; id++) {
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(id);
            }
            assertThat(rows.hasNext()).isFalse();
        }
    }

    /// The buffer contains the file and nothing else. Unused space behind the footer would
    /// break the read back, because [InputFile#of(ByteBuffer)] takes the buffer's capacity as
    /// the file's length and looks for the footer at its end.
    @Test
    void theBufferHoldsExactlyTheFileThatWasWritten() throws Exception {
        BufferOutputFile out = writeThreeRows();
        ByteBuffer buffer = out.buffer();

        assertThat(buffer.position()).isZero();
        assertThat(buffer.limit()).isEqualTo(buffer.capacity());
        assertThat(buffer.remaining()).isEqualTo(out.position());
        assertThat(InputFile.of(buffer).length()).isEqualTo(out.position());
    }

    /// Reading a returned buffer does not consume anything, so a second caller still gets the
    /// complete file.
    @Test
    void everyCallReturnsAFreshViewOfTheSameBytes() throws Exception {
        BufferOutputFile out = writeThreeRows();

        ByteBuffer first = out.buffer();
        first.position(first.limit());

        ByteBuffer second = out.buffer();
        assertThat(second.position()).isZero();
        assertThat(second.remaining()).isEqualTo(first.capacity());
    }

    /// A file is valid only once it is closed. Before that no footer has been written, so what
    /// the buffer holds is not a Parquet file and is not handed out.
    @Test
    void refusesToHandOutAFileThatIsNotFinished() throws Exception {
        BufferOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema())) {
            writer.rowWriter().writeRow(row -> row.setLong("id", 1L));

            assertThatThrownBy(out::buffer)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not closed");
        }
    }

    /// A destination is discarded when the writer could not finish a valid file at it, and is
    /// then left as if nothing had been written.
    @Test
    void refusesToHandOutADiscardedFile() throws Exception {
        BufferOutputFile out = OutputFile.inMemory();
        out.create();
        out.write(ByteBuffer.wrap(new byte[] { 'P', 'A', 'R', '1' }));
        out.discard();

        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("discarded");
    }
}
