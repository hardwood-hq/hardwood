/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A file whose planning fails fails every later request for a work item.
///
/// Each projected column asks [RowGroupIterator#workItemAt] on its own retriever thread. A
/// column that asks after another column's request has failed must see that failure too:
/// were it handed `null` it would end its stream cleanly, and the read could end empty without
/// an exception (#1278).
class RowGroupIteratorPlanningFailureTest {

    private static final Path ORDER_AB = Paths.get("src/test/resources/compat_order_ab.parquet");
    private static final Path CHUNK_PATH_SWAPPED =
            Paths.get("src/test/resources/compat_chunk_path_swapped.parquet");
    private static final String SWAPPED_MESSAGE =
            "[compat_chunk_path_swapped.parquet] Row group 0 lists column 'b'"
                    + " where the schema declares 'a'";
    private static final Path THREE_ROW_GROUPS = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final String ERROR_MESSAGE = "open failed";

    @Test
    void aLaterRequestForTheFailedFileFailsTheSameWay() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = new RowGroupIterator(
                     List.of(InputFile.of(CHUNK_PATH_SWAPPED)), context, 0)) {
            iterator.openFirst();
            iterator.initialize(ColumnProjection.all(), null);

            assertThatThrownBy(() -> iterator.workItemAt(0))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
            assertThatThrownBy(() -> iterator.workItemAt(0))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
        }
    }

    @Test
    void aLaterRequestDoesNotPlanPastTheFailedFile() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = new RowGroupIterator(
                     List.of(InputFile.of(ORDER_AB), InputFile.of(CHUNK_PATH_SWAPPED),
                             InputFile.of(ORDER_AB)),
                     context, 0)) {
            iterator.openFirst();
            iterator.initialize(ColumnProjection.all(), null);

            assertThat(iterator.workItemAt(0).fileIndex()).isZero();
            assertThatThrownBy(() -> iterator.workItemAt(1))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
            // Not the third file's row group, which would silently drop the second file.
            assertThatThrownBy(() -> iterator.workItemAt(1))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage(SWAPPED_MESSAGE);
        }
    }

    @Test
    void anErrorInPlanningFailsEveryLaterRequest() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = new RowGroupIterator(
                     List.of(InputFile.of(ORDER_AB), new ErrorOnOpenInputFile()), context, 0)) {
            iterator.openFirst();
            iterator.initialize(ColumnProjection.all(), null);

            assertThat(iterator.workItemAt(0).fileIndex()).isZero();
            assertThatThrownBy(() -> iterator.workItemAt(1))
                    .isInstanceOf(AssertionError.class)
                    .hasMessage(ERROR_MESSAGE);
            // Not `null`, which would end the asking column's stream cleanly.
            assertThatThrownBy(() -> iterator.workItemAt(1))
                    .isInstanceOf(AssertionError.class)
                    .hasMessage(ERROR_MESSAGE);
        }
    }

    /// A column behind the one whose prefetch planned the failed file still reads every row
    /// group before it. Handed the failure instead, it would end its stream short of the
    /// other columns.
    @Test
    void aRequestBeforeTheFailedFileIsAnswered() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = new RowGroupIterator(
                     List.of(InputFile.of(THREE_ROW_GROUPS), new ErrorOnOpenInputFile()), context, 0)) {
            iterator.openFirst();
            iterator.initialize(ColumnProjection.all(), null);

            assertThat(iterator.workItemAt(0).rowGroupIndex()).isZero();
            assertThatThrownBy(() -> iterator.workItemAt(3))
                    .isInstanceOf(AssertionError.class)
                    .hasMessage(ERROR_MESSAGE);
            assertThat(iterator.workItemAt(1).rowGroupIndex()).isEqualTo(1);
            assertThat(iterator.workItemAt(2).rowGroupIndex()).isEqualTo(2);
            assertThatThrownBy(() -> iterator.workItemAt(3))
                    .isInstanceOf(AssertionError.class)
                    .hasMessage(ERROR_MESSAGE);
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
