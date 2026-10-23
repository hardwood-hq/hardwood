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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ParquetFileReader#close()] attempts to close every input file, throws the first
/// close failure and attaches the later ones as suppressed, and closes each file once.
class ParquetFileReaderCloseTest {

    private static final Path FIXTURE = Path.of("src/test/resources/address_book_test.parquet");

    private static RecordingInputFile file(String name, Exception closeFailure) throws IOException {
        return new RecordingInputFile(name, Files.readAllBytes(FIXTURE), null, closeFailure);
    }

    @Test
    void runtimeCloseFailureStillClosesTheRemainingFiles() throws IOException {
        RecordingInputFile first = file("first", new IllegalStateException("cannot close first"));
        RecordingInputFile second = file("second", null);
        RecordingInputFile third = file("third", null);
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(first, second, third));

        assertThatThrownBy(reader::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cannot close first");

        assertThat(second.closed).isTrue();
        assertThat(third.closed).isTrue();
    }

    @Test
    void laterCloseFailuresAreSuppressedUnderTheFirst() throws IOException {
        RecordingInputFile first = file("first", new IOException("cannot close first"));
        RecordingInputFile second = file("second", new IllegalStateException("cannot close second"));
        RecordingInputFile third = file("third", null);
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(first, second, third));

        assertThatThrownBy(reader::close)
                .isInstanceOf(IOException.class)
                .hasMessage("cannot close first")
                .satisfies(e -> assertThat(e.getSuppressed())
                        .extracting(Throwable::getMessage)
                        .containsExactly("cannot close second"));

        assertThat(third.closed).isTrue();
    }

    @Test
    void aSecondCloseDoesNotCloseTheFilesAgain() throws IOException {
        RecordingInputFile first = file("first", null);
        RecordingInputFile second = file("second", null);
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(first, second));

        reader.close();
        reader.close();

        assertThat(first.closeCount).isEqualTo(1);
        assertThat(second.closeCount).isEqualTo(1);
    }

    @Test
    void aSecondCloseAfterAFailedCloseDoesNotThrowAgain() throws IOException {
        RecordingInputFile first = file("first", new IOException("cannot close first"));
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(first));

        assertThatThrownBy(reader::close)
                .isInstanceOf(IOException.class)
                .hasMessage("cannot close first");
        reader.close();

        assertThat(first.closeCount).isEqualTo(1);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aConcurrentCloseWaitsForTheFirstAndDoesNotCloseTheFilesAgain() throws Exception {
        RecordingInputFile recording = file("file", null);
        HeldCloseInputFile file = new HeldCloseInputFile(recording);
        ParquetFileReader reader = ParquetFileReader.openAll(List.of(file));
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread first = startClose(reader, failure);
        Thread second;
        try {
            file.closeEntered.await();
            second = startClose(reader, failure);
            // The second close waits for the first, which holds the lifecycle lock.
            while (second.getState() != Thread.State.WAITING) {
                Thread.onSpinWait();
            }
        }
        finally {
            file.closeReleased.countDown();
        }
        first.join();
        second.join();

        assertThat(failure.get()).isNull();
        assertThat(recording.closeCount).isEqualTo(1);
    }

    private static Thread startClose(ParquetFileReader reader, AtomicReference<Throwable> failure) {
        return Thread.ofPlatform().start(() -> {
            try {
                reader.close();
            }
            catch (Throwable t) {
                failure.set(t);
            }
        });
    }

    /// Delegates to a [RecordingInputFile] and holds its first close until released.
    private static final class HeldCloseInputFile implements InputFile {

        private final RecordingInputFile delegate;
        private final CountDownLatch closeEntered = new CountDownLatch(1);
        private final CountDownLatch closeReleased = new CountDownLatch(1);

        HeldCloseInputFile(RecordingInputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            closeEntered.countDown();
            try {
                closeReleased.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            delegate.close();
        }
    }
}
