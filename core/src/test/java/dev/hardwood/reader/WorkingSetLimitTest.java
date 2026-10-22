/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.RleExpansionFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read's working set, the dictionary and data page each decoded column holds, is held to the
/// working-set limit before anything is allocated for it.
///
/// The files are `INT64` columns whose dictionary page holds 1 MiB of zeros and whose data page
/// holds 1,024 values in 4 bytes. Each column holds its dictionary's entries (1,048,576 bytes) and
/// its page's batches (8,192 bytes); the read holds the largest task buffers once, the
/// dictionary page's decompressed bytes (1,048,576 bytes).
class WorkingSetLimitTest {

    private static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";

    private static final String LIMIT_OF_ONE = "more than its limit of 1 (hardwood.internal.maxWorkingSetBytes)";

    private static final int DICTIONARY_BYTES = 1024 * 1024;
    private static final int PAGE_VALUES = 1024;
    private static final long COLUMN_BYTES = DICTIONARY_BYTES + 8_192;

    /// The working set of a read of `columns` columns.
    private static long workingSet(int columns) {
        return columns * COLUMN_BYTES + DICTIONARY_BYTES;
    }

    @AfterEach
    void clearLimit() {
        System.clearProperty(MAX_WORKING_SET_BYTES_PROPERTY);
    }

    @Test
    void readAtTheLimitReads() throws IOException {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet(4)));

        assertThat(countRows(RleExpansionFile.buildWide(4, 1, DICTIONARY_BYTES, PAGE_VALUES), 4))
                .isEqualTo(PAGE_VALUES);
    }

    @Test
    void rowGroupsInBatchesOfTheirOwnCountOneDictionary() throws IOException {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet(1)));
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        assertThat(countValues(file, PAGE_VALUES)).isEqualTo(3 * PAGE_VALUES);
    }

    @Test
    void batchSpanningRowGroupsCountsTheDictionaryOfEach() throws IOException {
        // One batch holds the rows of all three row groups, so their three dictionaries at once
        long workingSet = 3L * DICTIONARY_BYTES + 8_192 + DICTIONARY_BYTES;
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet));
        assertThat(countValues(file, 3 * PAGE_VALUES)).isEqualTo(3 * PAGE_VALUES);

        // The fetch step parses the three dictionaries before the first page's task runs
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet - 1));
        assertThatThrownBy(() -> countValues(file, 3 * PAGE_VALUES))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', page 0] Data page needs 12292 bytes to decode, "
                        + "which takes the read's working set to 4202496 bytes, more than its limit of 4202495 "
                        + "(hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void dictionaryPruningReadCountsOnceTheColumnTakesIt() {
        // Pruning reads each row group's dictionary to look for the literal, which it holds, so
        // every row group stays and its column takes the dictionary instead of parsing it again.
        // Each parse fits the limit alone; the batch holding all three row groups' rows does not.
        long workingSet = 3L * DICTIONARY_BYTES + 8_192 + 4_100;
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet - 1));
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(0);
                    ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                    RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("c0", 0L)).build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', page 0] Data page needs 12292 bytes to decode, "
                        + "which takes the read's working set to 3158020 bytes, more than its limit of 3158019 "
                        + "(hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void readOneBytePastTheLimitFails() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(workingSet(4) - 1));
        byte[] file = RleExpansionFile.buildWide(4, 1, DICTIONARY_BYTES, PAGE_VALUES);

        // Without a pool the read's steps and tasks run in one order, so the same charge crosses
        assertThatThrownBy(() -> countRows(file, 0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c1', page 0] Data page needs 12292 bytes to decode, "
                        + "which takes the read's working set to 5275648 bytes, more than its limit of 5275647 "
                        + "(hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void dictionaryLargerThanTheLimitFailsBeforeItIsDecompressed() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(2L * DICTIONARY_BYTES - 1));
        byte[] file = RleExpansionFile.buildWide(1, 1, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> countRows(file, 4))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', dictionary page] Dictionary page needs 2097152 "
                        + "bytes to decode, which takes the read's working set to 2097152 bytes, more than its "
                        + "limit of 2097151 (hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void limitThatIsNotAByteCountIsRejected() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "0");
        byte[] file = RleExpansionFile.buildWide(1, 1, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> countRows(file, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.maxWorkingSetBytes must be a positive byte count, but was: 0");
    }

    /// Every way a read reaches a page charges it before decompressing it, so a limit of one byte
    /// fails each read: nested and binary columns, `DATA_PAGE_V2`, optional columns, the page
    /// index's plan and the column reader. Without a pool the read's order is the same each run,
    /// so the same charges are made and the same page fails.
    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
            compat_maxrep_list.parquet         | [compat_maxrep_list.parquet: row group 0, column 'g.list.element', page 0] Data page needs 96 bytes to decode, which takes the read's working set to 96 bytes, %1$s
            misaligned_pages_nested_v2.parquet | [misaligned_pages_nested_v2.parquet: row group 0, column 'narrow', page 0] Data page needs 2128 bytes to decode, which takes the read's working set to 5122 bytes, %1$s
            dict_string_map.parquet            | [dict_string_map.parquet: row group 0, column 'id', dictionary page] Dictionary page needs 1600 bytes to decode, which takes the read's working set to 1682 bytes, %1$s
            page_index_test.parquet            | [page_index_test.parquet: row group 0, column 'id', page 0] Data page needs 8192 bytes to decode, which takes the read's working set to 17568 bytes, %1$s
            inline_null_pages_v2.parquet       | [inline_null_pages_v2.parquet: row group 0, column 'id', page 0] Data page needs 2000 bytes to decode, which takes the read's working set to 9389 bytes, %1$s
            """)
    void everyPathChargesBeforeDecompressing(String file, String message) {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "1");

        assertThatThrownBy(() -> readAll(Path.of("src/test/resources", file)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message.formatted(LIMIT_OF_ONE));
    }

    @Test
    void columnReaderChargesBeforeDecompressing() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "1");

        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(0);
                    ParquetFileReader reader = ParquetFileReader.open(
                            InputFile.of(Path.of("src/test/resources/page_index_test.parquet")), context);
                    ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    // drains the column
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[page_index_test.parquet: row group 0, column 'id', page 0] Data page needs 8192 bytes "
                        + "to decode, which takes the read's working set to 8192 bytes, " + LIMIT_OF_ONE);
    }

    private static void readAll(Path file) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(0);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
                RowReader rows = reader.buildRowReader().build()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    /// The values of `file`'s one column, read in batches of `batchSize` rows on a context
    /// without threads, each checked to be the dictionary's one value.
    private static long countValues(byte[] file, int batchSize) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(0);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                ColumnReader column = reader.buildColumnReader(0).batchSize(batchSize).build()) {
            long count = 0;
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    assertThat(values[i]).isZero();
                }
                count += column.getRecordCount();
            }
            return count;
        }
    }

    /// The rows of `file`, read on a context of `threads` threads, each checked to hold the
    /// dictionary's one value in every column.
    private static long countRows(byte[] file, int threads) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("c0")).isZero();
                count++;
            }
            return count;
        }
    }
}
