/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.event;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;
import org.moditect.jfrunit.EnableEvent;
import org.moditect.jfrunit.JfrEvents;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that Hardwood JFR events are emitted during normal read operations.
 */
@org.moditect.jfrunit.JfrEventTest
public class JfrEventTest {

    private static final Path TEST_FILE = Paths.get("src/test/resources/plain_snappy.parquet");

    public JfrEvents jfrEvents = new JfrEvents();

    @Test
    @EnableEvent("dev.hardwood.FileOpened")
    @EnableEvent("dev.hardwood.FileMapping")
    void shouldEmitFileOpenedAndFileMappingEvents() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(TEST_FILE)) {
            assertThat(reader.getFileMetaData()).isNotNull();
        }

        jfrEvents.awaitEvents();

        long fileOpenedCount = jfrEvents.filter(e ->
                "dev.hardwood.FileOpened".equals(e.getEventType().getName()))
                .count();
        assertThat(fileOpenedCount)
                .as("Should emit one FileOpened event")
                .isEqualTo(1);

        long fileMappingCount = jfrEvents.filter(e ->
                "dev.hardwood.FileMapping".equals(e.getEventType().getName()))
                .count();
        assertThat(fileMappingCount)
                .as("Should emit at least one FileMapping event")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @EnableEvent("dev.hardwood.FileOpened")
    @EnableEvent("dev.hardwood.FileMapping")
    @EnableEvent("dev.hardwood.PageDecoded")
    @EnableEvent("dev.hardwood.RowGroupScanned")
    @EnableEvent("dev.hardwood.PrefetchMiss")
    @EnableEvent("dev.hardwood.BatchWait")
    void shouldEmitAllEventsWhenReadingRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(TEST_FILE);
             RowReader rowReader = reader.createRowReader()) {

            int count = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                count++;
            }
            assertThat(count).isGreaterThan(0);
        }

        jfrEvents.awaitEvents();

        assertThat(jfrEvents.filter(e ->
                "dev.hardwood.FileOpened".equals(e.getEventType().getName())).count())
                .as("Should emit one FileOpened event")
                .isEqualTo(1);

        assertThat(jfrEvents.filter(e ->
                "dev.hardwood.FileMapping".equals(e.getEventType().getName())).count())
                .as("Should emit at least one FileMapping event")
                .isGreaterThanOrEqualTo(1);

        assertThat(jfrEvents.filter(e ->
                "dev.hardwood.PageDecoded".equals(e.getEventType().getName())).count())
                .as("Should emit at least one PageDecoded event")
                .isGreaterThanOrEqualTo(1);

        assertThat(jfrEvents.filter(e ->
                "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName())).count())
                .as("Should emit at least one RowGroupScanned event")
                .isGreaterThanOrEqualTo(1);

        // PrefetchMiss and BatchWait are timing-dependent — they may or may not fire
        // depending on thread scheduling. We enable them to ensure they don't cause
        // errors, but don't assert a specific count.
    }
}
