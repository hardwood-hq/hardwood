/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A file whose planning fails fails the read at that file, and every later step of it.
///
/// Every column of a read reaches the failed file on its own. A column that reaches it after
/// another column has failed there must see that failure too: were it handed the end of its stream
/// it would end cleanly, and the read could end short without an exception (#1278). Nor may a later
/// step go past the failed file, which would silently drop it.
class PlanningFailureTest {

    private static final Path ORDER_AB = Paths.get("src/test/resources/compat_order_ab.parquet");
    private static final Path CHUNK_PATH_SWAPPED =
            Paths.get("src/test/resources/compat_chunk_path_swapped.parquet");
    private static final String SWAPPED_MESSAGE =
            "[compat_chunk_path_swapped.parquet] Row group 0 lists column 'b'"
                    + " where the schema declares 'a'";
    private static final Path THREE_ROW_GROUPS = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final String ERROR_MESSAGE = "open failed";

    @Test
    void aLaterReadOfTheFailedFileFailsTheSameWay() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(CHUNK_PATH_SWAPPED))) {
            assertThatThrownBy(() -> reader.columnReaders(ColumnProjection.all()))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
            assertThatThrownBy(() -> reader.columnReaders(ColumnProjection.all()))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
        }
    }

    @Test
    void aLaterStepDoesNotReadPastTheFailedFile() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(
                List.of(InputFile.of(ORDER_AB), InputFile.of(CHUNK_PATH_SWAPPED), InputFile.of(ORDER_AB)));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            long rows = rowsUntilFailure(columns, SchemaIncompatibleException.class, SWAPPED_MESSAGE);

            assertThat(rows).isEqualTo(rows(ORDER_AB));
            // Not the third file's batch, which would silently drop the second file
            assertThatThrownBy(columns::nextBatch)
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
        }
    }

    @Test
    void anErrorInPlanningFailsEveryLaterStep() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(
                List.of(InputFile.of(ORDER_AB), new ErrorOnOpenInputFile()));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            long rows = rowsUntilFailure(columns, AssertionError.class, ERROR_MESSAGE);

            assertThat(rows).isEqualTo(rows(ORDER_AB));
            // Not the end of the stream, which would end the read cleanly
            assertThatThrownBy(columns::nextBatch)
                    .isInstanceOf(AssertionError.class)
                    .hasMessage(ERROR_MESSAGE);
        }
    }

    /// Every row group of the file before the failed one is read, by every column, before the
    /// failure is raised.
    @Test
    void everyRowGroupBeforeTheFailedFileIsRead() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(
                List.of(InputFile.of(THREE_ROW_GROUPS), new ErrorOnOpenInputFile()));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            long rows = rowsUntilFailure(columns, AssertionError.class, ERROR_MESSAGE);

            assertThat(rows).isEqualTo(rows(THREE_ROW_GROUPS));
        }
    }

    /// The rows `columns` returns before it fails with `type` and `message`.
    private static long rowsUntilFailure(ColumnReaders columns, Class<? extends Throwable> type, String message) {
        long[] rows = new long[1];
        assertThatThrownBy(() -> {
            while (columns.nextBatch()) {
                rows[0] += columns.getRecordCount();
            }
        })
                .isInstanceOf(type)
                .hasMessage(message);
        return rows[0];
    }

    private static long rows(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return reader.getFileMetaData().numRows();
        }
    }

    /// Raises an `Error`, not an exception, when the read opens it.
    private static final class ErrorOnOpenInputFile implements InputFile {

        @Override
        public void open() {
            throw new AssertionError(ERROR_MESSAGE);
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            throw new IllegalStateException("Not opened");
        }

        @Override
        public long length() {
            throw new IllegalStateException("Not opened");
        }

        @Override
        public String name() {
            return "error_on_open.parquet";
        }

        @Override
        public void close() {
        }
    }
}
