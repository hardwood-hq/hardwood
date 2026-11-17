/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `DELTA_BYTE_ARRAY` pages whose values each repeat their predecessor in full: a page of a few KiB
/// that reconstructs `ROWS` values of `VALUE_BYTES`, which no header gives. A decode draws them on
/// the context's heap quota as it learns them, before it allocates them.
class ReconstructedValuesQuotaTest {

    private static final int COLUMNS = 2;
    private static final int ROWS = 512;
    private static final int VALUE_BYTES = 64 * 1024;
    /// what one page's values take reconstructed
    private static final long PAGE_VALUES_BYTES = (long) ROWS * VALUE_BYTES;
    private static final long MIB = 1024 * 1024;

    @AfterEach
    void clearQuota() {
        System.clearProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY);
    }

    /// A quota that holds one page's reconstructed values, in its task's buffer and in the chunks
    /// its fill copies them into, beside the other column's batch, but not two pages' decodes at
    /// once: on a pool, the page decoded beside the one need forces waits for that one to learn
    /// its values, then gives up for lack of room and decodes again once it fits; the read returns
    /// every row.
    @ParameterizedTest
    @ValueSource(ints = { 4, 0 })
    void readUnderAQuotaOfOnePagesReconstructedValuesReadsEveryRow(int threads) throws IOException {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Long.toString(3 * PAGE_VALUES_BYTES + MIB));

        assertThat(readAll(file(), threads)).isEqualTo(ROWS);
    }

    /// A quota below one page's reconstructed values in its task's buffer and its batch's chunks:
    /// the read fails at the page once its decode has learned them, before it allocates them, on a
    /// pool and without one.
    @ParameterizedTest
    @ValueSource(ints = { 4, 0 })
    void pageWhoseReconstructedValuesExceedTheQuotaFailsBeforeAllocatingThem(int threads) {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Long.toString(2 * PAGE_VALUES_BYTES - MIB));
        byte[] file = file();

        assertThatThrownBy(() -> readAll(file, threads))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', page 0] Data page needs 67108864 more bytes for "
                        + "the values it reconstructs, which takes the context's heap quota to 67255472 bytes, more "
                        + "than its limit of 66060288 (hardwood.internal.readAheadBytes)");
    }

    private static long readAll(byte[] file, int threads) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                for (int c = 0; c < COLUMNS; c++) {
                    assertThat(rows.getBinary("c" + c)).hasSize(VALUE_BYTES);
                }
                count++;
            }
            return count;
        }
    }

    private static byte[] file() {
        FileSchema.Builder schema = FileSchema.builder("reconstructed");
        for (int c = 0; c < COLUMNS; c++) {
            schema.addColumn("c" + c, PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        }
        byte[] value = new byte[VALUE_BYTES];
        Arrays.fill(value, (byte) 7);
        byte[][] values = new byte[ROWS][];
        Arrays.fill(values, value);
        WriterConfig config = WriterConfig.builder().encoding(ColumnEncoding.DELTA_BYTE_ARRAY)
                .pageTargetBytes(128 * 1024 * 1024).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema.build(), config)) {
            writer.columnWriter().writeBatch(batch -> {
                for (int c = 0; c < COLUMNS; c++) {
                    batch.bytes(c, values);
                }
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return InMemoryFiles.toByteArray(out);
    }
}
