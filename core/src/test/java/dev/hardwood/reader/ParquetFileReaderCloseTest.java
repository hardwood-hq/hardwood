/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ParquetFileReader#close()] attempts to close every input file, throws the first
/// close failure and attaches the later ones as suppressed.
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
}
