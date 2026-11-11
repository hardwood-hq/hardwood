/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read under a read-ahead buffer of no bytes still reads every row: the work its consumer's
/// next batch needs starts uncharged, one job at a time.
class NoReadAheadBufferTest {

    @AfterEach
    void clearBuffer() {
        System.clearProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY);
    }

    /// Dictionaries and pages above the retention size in every column, read with no read-ahead
    /// buffer, from the pages' headers and from an offset index: the read returns every row, on a
    /// pool and without one.
    @ParameterizedTest
    @CsvSource({ "0, false", "1, false", "4, false", "0, true", "4, true" })
    void readWithNoReadAheadBufferReadsEveryRow(int threads, boolean indexed) throws IOException {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, "0");
        int pageValues = 5 * 1024 * 1024;
        byte[] file = RleExpansionFile.buildWide(4, 3, 2 * DecodeBuffers.RETAINED_BYTES, pageValues, indexed);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertThat(count).isEqualTo(3L * pageValues);
        }
    }

    /// The file of [#readWithNoReadAheadBufferReadsEveryRow] read through column readers: the read
    /// returns every row.
    @ParameterizedTest
    @CsvSource({ "0, false", "4, false", "0, true", "4, true" })
    void columnReadWithNoReadAheadBufferReadsEveryRow(int threads, boolean indexed) throws IOException {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, "0");
        int pageValues = 5 * 1024 * 1024;
        byte[] file = RleExpansionFile.buildWide(4, 3, 2 * DecodeBuffers.RETAINED_BYTES, pageValues, indexed);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            long count = 0;
            while (columns.nextBatch()) {
                count += columns.getRecordCount();
            }
            assertThat(count).isEqualTo(3L * pageValues);
        }
    }

    @Test
    void bufferThatIsNotAByteCountIsRejected() {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, "-1");

        assertThatThrownBy(ReadAheadBuffer::new)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.readAheadBytes must be a non-negative byte count, but was: -1");
    }
}
