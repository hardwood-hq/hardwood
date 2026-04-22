/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.jfr;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that Hardwood JFR events are emitted during normal read operations.
public class JfrEventTest extends AbstractJfrRecorderTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_snappy.parquet");

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
             RowReader rowReader = reader.createRowReader()) {

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
}
