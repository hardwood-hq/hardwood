/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import org.junit.jupiter.api.Test;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.metadata.CompressionCodec;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A codec that fails is reported as a [ParquetWriteException], unchanged on the way out.
///
/// Compressing a buffer reaches nothing, so a codec failure is not an [IOException] and is not
/// something a caller retries. It travels as it was raised: nothing wraps it on the way through
/// the [RecordShredder.LevelSink] callback it is thrown inside, and nothing unwraps it after.
///
/// All three public methods that flush reach the codec, and each is asserted rather than the one
/// that happened to be noticed. `IOException` stays on their signatures for the destination,
/// which is the only thing about a write that a second attempt can change.
class WriterCodecFailureTest {

    /// Fails on every page body. `getName` is answered so the writer is configured exactly as it
    /// would be for a working codec.
    private static final Compressor FAILING = new Compressor() {
        @Override
        public byte[] compress(byte[] data, int offset, int length) {
            throw new ParquetWriteException("codec unavailable");
        }

        @Override
        public String getName() {
            return "FAILING";
        }
    };

    /// A row group per record, so a flush happens inside the write call rather than only at
    /// close. `GZIP` names a codec that is not `UNCOMPRESSED`, which is the one value that
    /// skips compression entirely; the compressor above is what actually runs.
    private static WriterConfig flushPerRecord() {
        return WriterConfig.builder()
                .codec(CompressionCodec.GZIP)
                .rowGroupTargetRows(1)
                .build();
    }

    @Test
    void writeBatchReportsACodecFailureAsParquetWriteException() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(
                OutputFile.inMemory(), oneColumn(), flushPerRecord(), FAILING);

        assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] {1, 2})))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessage("codec unavailable");
    }

    @Test
    void writeRowReportsACodecFailureAsParquetWriteException() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(
                OutputFile.inMemory(), oneColumn(), flushPerRecord(), FAILING);
        RowWriter rows = writer.rowWriter();

        assertThatThrownBy(() -> {
            // The row writer stages records and submits them in batches, so the flush that
            // fails may be triggered by any one of them.
            for (int i = 0; i < 4096; i++) {
                int value = i;
                rows.writeRow(row -> row.setInt(0, value));
            }
            writer.close();
        }).isInstanceOf(ParquetWriteException.class)
          .hasMessage("codec unavailable");
    }

    @Test
    void closeReportsACodecFailureAsParquetWriteException() throws Exception {
        // No row-group target reached while writing, so the only flush is the one close does.
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), oneColumn(),
                WriterConfig.builder().codec(CompressionCodec.GZIP).build(), FAILING);
        writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] {1, 2, 3}));

        assertThatThrownBy(writer::close)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessage("codec unavailable");
    }
}
