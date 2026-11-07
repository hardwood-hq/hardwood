/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.LongStream;

import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `empty_data_pages_v1.parquet` and `empty_data_pages_v2.parquet`: a required `INT64` column
/// whose column chunk holds the data pages `[]`, `[1, 2]`, `[]`, `[3]`, as DataPageV1 and
/// DataPageV2 respectively, with no page index. A reader skips the pages without values, so
/// each file reads as `1, 2, 3`, as its `empty_data_pages.md` documents.
///
/// parquet-java cannot read the files, so this holds Hardwood to the documented values rather
/// than to a reference reader.
class EmptyDataPagesTest {

    private static final String COLUMN = "int64";

    private static final long[] VALUES = { 1, 2, 3 };

    @ParameterizedTest
    @ValueSource(strings = { "empty_data_pages_v1.parquet", "empty_data_pages_v2.parquet" })
    void rowsSkipThePagesWithoutValues(String fileName) throws IOException {
        LongStream.Builder read = LongStream.builder();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture(fileName)));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                read.add(rows.getLong(COLUMN));
            }
        }

        assertThat(read.build().toArray()).containsExactly(VALUES);
    }

    @ParameterizedTest
    @ValueSource(strings = { "empty_data_pages_v1.parquet", "empty_data_pages_v2.parquet" })
    void columnBatchesSkipThePagesWithoutValues(String fileName) throws IOException {
        LongStream.Builder read = LongStream.builder();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture(fileName)));
             ColumnReader column = reader.columnReader(COLUMN)) {
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    read.add(values[i]);
                }
            }
        }

        assertThat(read.build().toArray()).containsExactly(VALUES);
    }

    /// Why this module reads the fixtures without parquet-java: it reads a value from the first
    /// page, which has none. A parquet-java that skips such pages fails here, and the skips in
    /// [Utils] come off.
    @ParameterizedTest
    @ValueSource(strings = { "empty_data_pages_v1.parquet", "empty_data_pages_v2.parquet" })
    void parquetJavaReadsAValueFromTheFirstPage(String fileName) throws IOException {
        Path fixture = fixture(fileName);

        assertThatThrownBy(() -> Utils.readWithParquetJava(fixture))
                .isInstanceOf(ParquetDecodingException.class)
                .hasMessage("Can not read value at 1 in block 0 in file file:" + fixture.toAbsolutePath());
    }

    private static Path fixture(String fileName) throws IOException {
        return ParquetTestingRepoCloner.getTestFile("data/" + fileName);
    }
}
