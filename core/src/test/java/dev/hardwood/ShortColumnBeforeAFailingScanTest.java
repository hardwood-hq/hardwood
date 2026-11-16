/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A file found by fuzzing (#1630), of two row groups and no page index, whose list column
/// `bss_i32` holds one record fewer than the first row group's rows, and whose chunk of it in the
/// second row group has a page header that ends past the chunk. The scan of that chunk fails,
/// which fails the read at the second row group's first row; its pages are all keyed there, so
/// no job measures them. `bss_i32`, short of the first row group with nothing left to place before
/// the failure, abandons its open cell rather than waiting for them. The string column
/// `bss_flba` ends every batch at its row group.
class ShortColumnBeforeAFailingScanTest {

    private static final Path FILE = Path.of("src/test/resources/fuzz_nested_column_short_before_a_failing_scan.parquet");

    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = { 0, 4 })
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void theReadFails(int threads) {
        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(threads);
                 ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE), context);
                 ColumnReaders readers = reader.columnReaders(
                         ColumnProjection.columns("bss_i32.list.element", "bss_flba.list.element"))) {
                while (readers.nextBatch()) {
                    // read to the end
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[fuzz_nested_column_short_before_a_failing_scan.parquet: row group 1, column "
                        + "'bss_i32.list.element', page 15] Page header at offset 36685 does not fit in the 20 bytes "
                        + "left in its column chunk — the file is likely corrupt");
    }
}
