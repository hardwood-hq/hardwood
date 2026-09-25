/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.jfr;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that Hardwood JFR events are emitted during normal read operations.
public class JfrEventTest extends AbstractJfrRecorderTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_snappy.parquet");

    private static final Duration OPEN_DELAY = Duration.ofMillis(50);

    @Test
    void shouldEmitFileOpenedAndFileMappingEvents() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE))) {
            assertThat(reader.getFileMetaData()).isNotNull();
        }

        awaitEvents();

        assertThat(events("dev.hardwood.FileOpened").count())
                .as("Should emit one FileOpened event")
                .isEqualTo(1);

        assertThat(events("dev.hardwood.FileMapping").count())
                .as("Should emit at least one FileMapping event")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    void shouldEmitAllEventsWhenReadingRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader rowReader = reader.rowReader()) {

            int count = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                count++;
            }
            assertThat(count).isGreaterThan(0);
        }

        awaitEvents();

        assertThat(events("dev.hardwood.FileOpened").count())
                .as("Should emit one FileOpened event")
                .isEqualTo(1);

        assertThat(events("dev.hardwood.FileMapping").count())
                .as("Should emit at least one FileMapping event")
                .isGreaterThanOrEqualTo(1);

        assertThat(events("dev.hardwood.PageDecoded").count())
                .as("Should emit at least one PageDecoded event")
                .isGreaterThanOrEqualTo(1);

        assertThat(events("dev.hardwood.RowGroupScanned").count())
                .as("Should emit at least one RowGroupScanned event")
                .isGreaterThanOrEqualTo(1);
    }

    /// `FileOpened` spans opening the file and reading its footer, for the first file
    /// (`ParquetFileReader.open`) and for each further file of a multi-file read alike.
    @Test
    void fileOpenedSpansOpeningTheFile() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(
                List.of(new SlowOpeningFile(TEST_FILE, "slow-first.parquet"),
                        new SlowOpeningFile(TEST_FILE, "slow-second.parquet")));
                RowReader rowReader = reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
            }
        }

        awaitEvents();

        assertOpenedSlowly("slow-first.parquet");
        assertOpenedSlowly("slow-second.parquet");
    }

    private void assertOpenedSlowly(String fileName) {
        List<RecordedEvent> opened = events("dev.hardwood.FileOpened")
                .filter(event -> fileName.equals(event.getString("file")))
                .toList();
        assertThat(opened).as("FileOpened events for %s", fileName).isNotEmpty();
        assertThat(opened).allSatisfy(event -> assertThat(event.getDuration())
                .as("FileOpened duration for %s", fileName)
                .isGreaterThanOrEqualTo(OPEN_DELAY));
    }

    /// An [InputFile] whose `open()` takes at least [#OPEN_DELAY].
    private static final class SlowOpeningFile implements InputFile {

        private final InputFile delegate;
        private final String name;

        SlowOpeningFile(Path path, String name) {
            this.delegate = InputFile.of(path);
            this.name = name;
        }

        @Override
        public void open() throws IOException {
            try {
                Thread.sleep(OPEN_DELAY);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while opening", e);
            }
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
            delegate.close();
        }
    }
}
