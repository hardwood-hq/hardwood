/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.HardwoodContextImpl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A failed `open`/`openAll` closes every input file it took ownership of and a
/// context it created, and attaches close failures to the original exception.
class ParquetFileReaderOpenFailureTest {

    private static final String BAD_MAGIC_MESSAGE =
            "[bad] Not a Parquet file (invalid magic number at end)";

    @Test
    void footerFailureClosesEveryFile() {
        RecordingInputFile first = RecordingInputFile.notParquet("bad");
        RecordingInputFile second = RecordingInputFile.notParquet("second");
        RecordingInputFile third = RecordingInputFile.notParquet("third");

        assertThatThrownBy(() -> ParquetFileReader.openAll(List.of(first, second, third)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(BAD_MAGIC_MESSAGE);

        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
        assertThat(third.closed).isTrue();
    }

    @Test
    void openFailureClosesEveryFile() {
        RecordingInputFile first = new RecordingInputFile("first", new IOException("cannot open first"), null);
        RecordingInputFile second = RecordingInputFile.notParquet("second");

        assertThatThrownBy(() -> ParquetFileReader.openAll(List.of(first, second)))
                .isInstanceOf(IOException.class)
                .hasMessage("cannot open first");

        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
    }

    @Test
    void closeFailuresAreSuppressedUnderTheOpenFailure() {
        RecordingInputFile first = RecordingInputFile.notParquet("bad");
        RecordingInputFile second = new RecordingInputFile("second", null, new IOException("cannot close second"));
        RecordingInputFile third = new RecordingInputFile("third", null, new IOException("cannot close third"));

        assertThatThrownBy(() -> ParquetFileReader.openAll(List.of(first, second, third)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(BAD_MAGIC_MESSAGE)
                .satisfies(e -> assertThat(e.getSuppressed())
                        .extracting(Throwable::getMessage)
                        .containsExactly("cannot close second", "cannot close third"));

        assertThat(third.closed).isTrue();
    }

    @Test
    void footerFailureClosesAnOwnedContext() {
        HardwoodContextImpl context = HardwoodContextImpl.create(1);
        RecordingInputFile first = RecordingInputFile.notParquet("bad");

        assertThatThrownBy(() -> ParquetFileReader.openInternal(List.of(first), context, ReaderConfig.defaults(), true))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(BAD_MAGIC_MESSAGE);

        assertThat(context.executor().isShutdown()).isTrue();
    }

    @Test
    void emptyFileListClosesAnOwnedContext() {
        HardwoodContextImpl context = HardwoodContextImpl.create(1);

        assertThatThrownBy(() -> ParquetFileReader.openInternal(List.of(), context, ReaderConfig.defaults(), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("At least one file must be provided");

        assertThat(context.executor().isShutdown()).isTrue();
    }

    @Test
    void openFailureLeavesASharedContextOpen() {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            RecordingInputFile first = RecordingInputFile.notParquet("bad");

            assertThatThrownBy(() -> ParquetFileReader.openAll(List.of(first), context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage(BAD_MAGIC_MESSAGE);

            assertThat(context.executor().isShutdown()).isFalse();
            assertThat(first.closed).isTrue();
        }
    }
}
