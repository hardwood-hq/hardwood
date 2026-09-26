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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What an iterator that owns its input files does when closing one of them fails.
///
/// It used to log a warning and carry on, which is a silent failure: a caller that asked for
/// the files to be released was told they had been. Now the first failure is raised and the
/// rest are suppressed beneath it, which is what `ParquetFileReader.close` has always done.
class RowGroupIteratorCloseTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_uncompressed.parquet");

    /// An [InputFile] that refuses to close, so a close failure can be observed without one.
    private static final class UncloseableInputFile implements InputFile {

        private final InputFile delegate;
        private final String name;
        private final boolean failUnchecked;

        private UncloseableInputFile(InputFile delegate, String name, boolean failUnchecked) {
            this.delegate = delegate;
            this.name = name;
            this.failUnchecked = failUnchecked;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void close() throws IOException {
            if (failUnchecked) {
                throw new IllegalStateException("cannot release " + name);
            }
            throw new IOException("cannot release " + name);
        }
    }

    private static InputFile uncloseable(String name) {
        return new UncloseableInputFile(InputFile.of(TEST_FILE), name, false);
    }

    @Test
    void aFailedFileCloseIsReportedRatherThanLogged() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(
                    List.of(uncloseable("first.parquet")), context, 0);

            assertThatThrownBy(iterator::close)
                    .isInstanceOf(IOException.class)
                    .hasMessage("cannot release first.parquet");
        }
    }

    @Test
    void theFirstFailureIsRaisedAndTheRestSuppressedBeneathIt() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(
                    List.of(uncloseable("first.parquet"),
                            uncloseable("second.parquet"),
                            uncloseable("third.parquet")),
                    context, 0);

            assertThatThrownBy(iterator::close)
                    .isInstanceOf(IOException.class)
                    .hasMessage("cannot release first.parquet")
                    .satisfies(e -> assertThat(e.getSuppressed())
                            .extracting(Throwable::getMessage)
                            .containsExactly("cannot release second.parquet",
                                    "cannot release third.parquet"));
        }
    }

    @Test
    void everyFileIsStillAttemptedAfterOneFails() throws Exception {
        // The loop does not stop at the first failure: a file left open because an earlier one
        // refused to close is the leak this reporting exists to make visible.
        CountingInputFile closeable = new CountingInputFile(InputFile.of(TEST_FILE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(
                    List.of(uncloseable("first.parquet"), closeable), context, 0);

            assertThatThrownBy(iterator::close).isInstanceOf(IOException.class);
        }

        assertThat(closeable.closeCount()).isEqualTo(1);
    }

    @Test
    void anUncheckedCloseFailureStillClosesTheRemainingFiles() throws Exception {
        CountingInputFile closeable = new CountingInputFile(InputFile.of(TEST_FILE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            RowGroupIterator iterator = new RowGroupIterator(
                    List.of(new UncloseableInputFile(InputFile.of(TEST_FILE), "first.parquet", true), closeable),
                    context, 0);

            assertThatThrownBy(iterator::close)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("cannot release first.parquet");
        }

        assertThat(closeable.closeCount()).isEqualTo(1);
    }
}
