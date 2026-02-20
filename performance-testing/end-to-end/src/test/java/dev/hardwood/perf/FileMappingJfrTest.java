/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.moditect.jfrunit.EnableEvent;
import org.moditect.jfrunit.JfrEventTest;
import org.moditect.jfrunit.JfrEvents;

import dev.hardwood.Hardwood;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * Tests that verify file mapping (mmap) operations using JFR events.
 */
@JfrEventTest
public class FileMappingJfrTest {

    private static final Path DATA_DIR = Path.of("../test-data-setup/target/tlc-trip-record-data");

    public JfrEvents jfrEvents = new JfrEvents();

    @Test
    @EnableEvent("dev.hardwood.FileMapping")
    void shouldTrackFileMappingOperations() throws Exception {
        // Get all 2025 files
        List<Path> files2025 = new ArrayList<>();
        for (int month = 1; month <= 12; month++) {
            String filename = String.format("yellow_tripdata_2025-%02d.parquet", month);
            Path file = DATA_DIR.resolve(filename);
            if (Files.exists(file) && Files.size(file) > 0) {
                files2025.add(file);
            }
        }

        if (files2025.isEmpty()) {
            System.out.println("Skipping JFR test - no 2025 data files available");
            return;
        }

        long totalFileSize = 0;
        for (Path file : files2025) {
            totalFileSize += Files.size(file);
        }

        long rowCount = 0;

        // Use MultiFileRowReader with all columns (no projection)
        try (Hardwood hardwood = Hardwood.create();
                MultiFileParquetReader parquet = hardwood.openAll(files2025);
                MultiFileRowReader rowReader = parquet.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                rowCount++;
            }
        }

        jfrEvents.awaitEvents();

        // Get total bytes mapped from FileMapping events
        long totalBytesMapped = jfrEvents.filter(event ->
                "dev.hardwood.FileMapping".equals(event.getEventType().getName()))
                .mapToLong(event -> event.getLong("size"))
                .sum();

        long mappingCount = jfrEvents.filter(event ->
                "dev.hardwood.FileMapping".equals(event.getEventType().getName()))
                .count();

        System.out.println("\n=== JFR File Mapping Analysis ===");
        System.out.println("Files: " + files2025.size() + " (2025 data)");
        System.out.println("Total file size: " + String.format("%,d bytes", totalFileSize));
        System.out.println("Total bytes mapped (JFR): " + String.format("%,d bytes", totalBytesMapped));
        System.out.println("Number of mmap operations: " + mappingCount);
        System.out.println("Rows processed: " + String.format("%,d", rowCount));

        // Assert that we mapped data
        assertThat(totalBytesMapped)
                .as("Should have mapped some bytes from the parquet files")
                .isGreaterThan(0);

        // Assert that total mapped approximates total file size (reading all columns)
        assertThat(totalBytesMapped)
                .as("With all columns, mapped bytes should approximate total file size")
                .isCloseTo(totalFileSize, withinPercentage(1));
    }
}
