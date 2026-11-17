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
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What a read's decodes keep, the dictionary and data pages each decoded column holds, is held to
/// its context's heap quota before anything is allocated for it.
///
/// The files are `INT64` columns whose dictionary page holds 1 MiB of zeros and whose data page
/// holds 1,024 values in 4 bytes. Each column holds its dictionary's entries (1,048,576 bytes)
/// until its chunk's data pages have decoded; its parse's buffers fit the size every decode
/// thread keeps, which the quota does not count.
class HeapQuotaTest {

    private static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";
    private static final String HEAP_QUOTA_PROPERTY = "hardwood.internal.readAheadBytes";

    private static final String QUOTA_OF_ONE = "more than its limit of 1 (hardwood.internal.readAheadBytes)";

    private static final int DICTIONARY_BYTES = 1024 * 1024;
    private static final int PAGE_VALUES = 1024;
    /// room for one data page's batches and what its read fetches, well below a dictionary's
    private static final long PAGE_ROOM = 256 * 1024;

    @AfterEach
    void clearLimit() {
        System.clearProperty(MAX_WORKING_SET_BYTES_PROPERTY);
        System.clearProperty(HEAP_QUOTA_PROPERTY);
    }

    @Test
    void readUnderAQuotaOfEveryColumnsDictionaryAndPageReads() throws IOException {
        System.setProperty(HEAP_QUOTA_PROPERTY, Long.toString(4 * (DICTIONARY_BYTES + PAGE_ROOM)));

        assertThat(countRows(RleExpansionFile.buildWide(4, 1, DICTIONARY_BYTES, PAGE_VALUES), 4))
                .isEqualTo(PAGE_VALUES);
    }

    @Test
    void dictionaryLargerThanTheQuotaFailsBeforeItIsDecompressed() {
        System.setProperty(HEAP_QUOTA_PROPERTY, Long.toString(DICTIONARY_BYTES - 1));
        byte[] file = RleExpansionFile.buildWide(1, 1, DICTIONARY_BYTES, PAGE_VALUES);

        assertThatThrownBy(() -> countRows(file, 0))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'c0', dictionary page] Dictionary page needs 1048576 "
                        + "bytes to decode, which takes the context's heap quota to 1048576 bytes, more than its "
                        + "limit of 1048575 (hardwood.internal.readAheadBytes)");
    }

    /// Three row groups under a quota of one dictionary and one page: each row group's dictionary
    /// goes once its data page has decoded, whether the row groups' rows are batches of their own
    /// or one batch holds them all, as a numeric column's batches hold the values they took from
    /// the dictionary, not the dictionary.
    @ParameterizedTest
    @ValueSource(ints = { 1, 3 })
    void rowGroupsHoldOneNumericDictionaryAtATime(int rowGroupsPerBatch) throws IOException {
        System.setProperty(HEAP_QUOTA_PROPERTY, Long.toString(DICTIONARY_BYTES + PAGE_ROOM));
        byte[] file = RleExpansionFile.buildWide(1, 3, DICTIONARY_BYTES, PAGE_VALUES);

        assertThat(countValues(file, rowGroupsPerBatch * PAGE_VALUES)).isEqualTo(3 * PAGE_VALUES);
    }

    /// Pruning reads each row group's dictionary to look for the literal, which it holds, so every
    /// row group stays and its column takes the dictionary instead of parsing it again: the quota
    /// holds each dictionary the column takes, which a quota of two dictionaries does not hold
    /// three of.
    @Test
    void dictionaryPruningReadCountsEachDictionaryTheColumnTakes() {
        System.setProperty(HEAP_QUOTA_PROPERTY, Long.toString(2 * DICTIONARY_BYTES + PAGE_ROOM));
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
                .hasMessage("[<memory>: row group 2, column 'c0', page 0] Data page needs 24960 bytes to decode, "
                        + "which takes the context's heap quota to 3195648 bytes, more than its limit of 2359296 "
                        + "(hardwood.internal.readAheadBytes)");
    }

    @Test
    void limitThatIsNotAByteCountIsRejected() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "0");

        ColumnSchema column = FileSchema.builder("t").addColumn("c0", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build().getColumn(0);

        assertThatThrownBy(() -> WorkingSet.standalone(column))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.maxWorkingSetBytes must be a positive byte count, but was: 0");
    }

    /// Every way a read reaches a page charges it before decompressing it, so a heap quota of one
    /// byte fails each read: nested and flat columns, dictionaries, `DATA_PAGE_V2`, optional
    /// columns, the page index's plan and the column reader. Without a pool the read's order is
    /// the same each run, so the same charges are made and the same page fails.
    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
            compat_maxrep_list.parquet         | [compat_maxrep_list.parquet: row group 0, column 'g.list.element', page 0] Data page needs 120 bytes to decode, which takes the context's heap quota to 120 bytes, %s
            misaligned_pages_nested_v2.parquet | [misaligned_pages_nested_v2.parquet: row group 0, column 'narrow', page 0] Data page needs 2660 bytes to decode, which takes the context's heap quota to 2660 bytes, %s
            dict_string_map.parquet            | [dict_string_map.parquet: row group 0, column 'id', dictionary page] Dictionary page needs 800 bytes to decode, which takes the context's heap quota to 800 bytes, %s
            page_index_test.parquet            | [page_index_test.parquet: row group 0, column 'id', page 0] Data page needs 80000 bytes to decode, which takes the context's heap quota to 80000 bytes, %s
            inline_null_pages_v2.parquet       | [inline_null_pages_v2.parquet: row group 0, column 'id', page 0] Data page needs 8000 bytes to decode, which takes the context's heap quota to 8000 bytes, %s
            """)
    void everyPathChargesBeforeDecompressing(String file, String message) {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "1");
        System.setProperty(HEAP_QUOTA_PROPERTY, "1");

        assertThatThrownBy(() -> readAll(Path.of("src/test/resources", file)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(message.formatted(QUOTA_OF_ONE));
    }

    @Test
    void columnReaderChargesBeforeDecompressing() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, "1");
        System.setProperty(HEAP_QUOTA_PROPERTY, "1");

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
                .hasMessage("[page_index_test.parquet: row group 0, column 'id', page 0] Data page needs 80000 bytes "
                        + "to decode, which takes the context's heap quota to 80000 bytes, " + QUOTA_OF_ONE);
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
