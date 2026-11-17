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

/// A read under a heap quota with room for about one page and one dictionary of each column, and
/// so hardly any for decoding ahead, still reads every row: the pages and dictionaries its
/// consumer's next batch needs take the room the read reserves for them.
class TightHeapQuotaTest {

    private static final int COLUMNS = 4;
    private static final int PAGE_VALUES = 5 * 1024 * 1024;
    /// at least the rows a batch of the read holds
    private static final int BATCH_ROWS = 65_536;
    /// what each row group's dictionary decompresses to, and its entries take
    private static final int DICTIONARY_BYTES = 2 * DecodeBuffers.RETAINED_BYTES;
    /// per column, one page of `long` values, the batch it shares with the next page, which either
    /// page's decode may allocate, a batch's worth for the page's fetched bytes, and its chunk's
    /// dictionary; and one dictionary parse's buffers
    private static final long QUOTA = (long) COLUMNS * ((PAGE_VALUES + 2L * BATCH_ROWS) * Long.BYTES + DICTIONARY_BYTES)
            + DICTIONARY_BYTES;

    @AfterEach
    void clearBuffer() {
        System.clearProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY);
    }

    /// Dictionaries and pages above the retention size in every column, read under a quota of one
    /// page of `long` values per column, from the pages' headers and from an offset index: the
    /// read returns every row, on a pool and without one.
    @ParameterizedTest
    @CsvSource({ "0, false", "1, false", "4, false", "0, true", "4, true" })
    void readUnderAQuotaOfOnePagePerColumnReadsEveryRow(int threads, boolean indexed) throws IOException {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Long.toString(QUOTA));
        byte[] file = RleExpansionFile.buildWide(COLUMNS, 3, DICTIONARY_BYTES, PAGE_VALUES, indexed);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertThat(count).isEqualTo(3L * PAGE_VALUES);
        }
    }

    /// The file of [#readUnderAQuotaOfOnePagePerColumnReadsEveryRow] read through column readers:
    /// the read returns every row.
    @ParameterizedTest
    @CsvSource({ "0, false", "4, false", "0, true", "4, true" })
    void columnReadUnderAQuotaOfOnePagePerColumnReadsEveryRow(int threads, boolean indexed) throws IOException {
        System.setProperty(ReadAheadBuffer.READ_AHEAD_BYTES_PROPERTY, Long.toString(QUOTA));
        byte[] file = RleExpansionFile.buildWide(COLUMNS, 3, DICTIONARY_BYTES, PAGE_VALUES, indexed);

        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            long count = 0;
            while (columns.nextBatch()) {
                count += columns.getRecordCount();
            }
            assertThat(count).isEqualTo(3L * PAGE_VALUES);
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
