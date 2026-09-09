/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reading a file whose schema declares no columns.
///
/// Nothing in the format requires a leaf and Arrow writes such a file, so one reaches the
/// reader even though [dev.hardwood.writer.ParquetFileWriter] declines to produce one. It
/// carries no column chunk and so no batch, which leaves the row reader with no column to take
/// a record count from: it read the count from the first column, and there is no first column.
/// The relation such a file describes is empty, not endless.
class NoColumnsFileReadTest {

    private static final Path NO_COLUMNS = Paths.get("src/test/resources/no_columns.parquet");

    @Test
    void schemaAndMetadataAreReadable() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NO_COLUMNS))) {
            assertThat(reader.getFileSchema().getColumnCount()).isZero();
            assertThat(reader.getFileSchema().isFlatSchema()).isTrue();
            assertThat(reader.getFileMetaData().numRows()).isZero();
        }
    }

    /// `hasNext()` used to answer `true` for as long as it was asked, so any `while
    /// (hasNext())` loop over such a file ran forever.
    @Test
    void rowReaderIsExhaustedFromTheStart() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NO_COLUMNS));
             RowReader rows = reader.rowReader()) {

            assertThat(rows.hasNext()).isFalse();
            assertThat(rows.hasNext()).isFalse();
            assertThatThrownBy(rows::next).isInstanceOf(NoSuchElementException.class);
        }
    }
}
