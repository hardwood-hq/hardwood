/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.moditect.jfrunit.EnableEvent;
import org.moditect.jfrunit.JfrEvents;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that column projection and row group filtering reduce S3 I/O,
/// using JFR events as the assertion mechanism:
///
/// - `dev.hardwood.RowGroupScanned` — only projected columns are scanned
/// - `dev.hardwood.RowGroupFilter` — row groups are skipped by predicate push-down
/// - `jdk.SocketRead` — fewer bytes are transferred over the network
///
/// Note: `S3InputFile` pre-fetches a 64 KB tail on `open()`, so files
/// smaller than 64 KB are served entirely from that cache — no additional socket
/// reads occur. The byte-comparison tests therefore use `page_index_test.parquet`
/// (170 KB, larger than the tail cache) to ensure socket-level differences are observable.
@Testcontainers
@org.moditect.jfrunit.JfrEventTest
public class S3SelectiveReadJfrTest {

    /// 170 KB, 3 columns (id, value, category), many pages, offset indexes — larger than the 64 KB tail cache.
    private static final String PAGE_INDEX_FILE = "page_index_test.parquet";

    /// 9.6 KB, 3 columns (id, value, label), 3 row groups — smaller than tail cache.
    private static final String FILTER_PUSHDOWN_FILE = "filter_pushdown_int.parquet";

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    static S3Source source;

    public JfrEvents jfrEvents = new JfrEvents();

    @BeforeAll
    static void setup() throws Exception {
        source = S3Source.builder()
                .endpoint(s3Mock.getHttpEndpoint())
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .build();

        source.api().createBucket("test-bucket");
        source.api().putObject("test-bucket", PAGE_INDEX_FILE, Files.readAllBytes(
                TEST_RESOURCES.resolve(PAGE_INDEX_FILE)));
        source.api().putObject("test-bucket", FILTER_PUSHDOWN_FILE, Files.readAllBytes(
                TEST_RESOURCES.resolve(FILTER_PUSHDOWN_FILE)));
    }

    @AfterAll
    static void tearDown() {
        source.close();
    }

    // ==================== Column Projection ====================

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void projectionScansOnlyRequestedColumns() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", PAGE_INDEX_FILE))) {

            try (RowReader rows = reader.createRowReader(
                    ColumnProjection.columns("id", "value"))) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }

        jfrEvents.awaitEvents();

        Set<String> scannedColumns = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .map(e -> e.getString("column"))
                .collect(Collectors.toSet());

        assertThat(scannedColumns)
                .as("Only projected columns should be scanned")
                .containsExactlyInAnyOrder("id", "value");
    }

    @Test
    @EnableEvent("jdk.SocketRead")
    void projectionTransfersFewerBytes() throws Exception {
        // page_index_test.parquet is 170 KB (> 64 KB tail cache), so column chunk
        // reads go to the network and are observable via jdk.SocketRead.
        long allColumnsBytes = readAndMeasureSocketBytes(PAGE_INDEX_FILE,
                ColumnProjection.all(), null);

        long oneColumnBytes = readAndMeasureSocketBytes(PAGE_INDEX_FILE,
                ColumnProjection.columns("id"), null);

        assertThat(oneColumnBytes)
                .as("Reading 1 of 3 columns should transfer fewer bytes than reading all columns")
                .isLessThan(allColumnsBytes);
    }

    // ==================== Row Group Filtering ====================

    @Test
    @EnableEvent("dev.hardwood.RowGroupFilter")
    void filterSkipsRowGroups() throws Exception {
        // filter_pushdown_int.parquet has 3 row groups:
        // RG0: id 1-100, RG1: id 101-200, RG2: id 201-300
        // Filtering id > 200 should keep only RG2
        FilterPredicate filter = FilterPredicate.gt("id", 200L);

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", FILTER_PUSHDOWN_FILE))) {
            try (RowReader rows = reader.createRowReader(filter)) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }

        jfrEvents.awaitEvents();

        jfrEvents
                .filter(e -> "dev.hardwood.RowGroupFilter".equals(e.getEventType().getName()))
                .findFirst()
                .ifPresentOrElse(event -> {
                    assertThat(event.getInt("totalRowGroups"))
                            .as("File should have 3 row groups")
                            .isEqualTo(3);
                    assertThat(event.getInt("rowGroupsSkipped"))
                            .as("Filter id > 200 should skip 2 row groups")
                            .isEqualTo(2);
                    assertThat(event.getInt("rowGroupsKept"))
                            .as("Filter id > 200 should keep 1 row group")
                            .isEqualTo(1);
                }, () -> {
                    throw new AssertionError("Expected a RowGroupFilter JFR event");
                });
    }

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void filterReducesScannedRowGroups() throws Exception {
        // With filter id > 200, only 1 of 3 row groups should be scanned
        FilterPredicate filter = FilterPredicate.gt("id", 200L);

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", FILTER_PUSHDOWN_FILE))) {
            try (RowReader rows = reader.createRowReader(filter)) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }

        jfrEvents.awaitEvents();

        long scannedRowGroups = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        // File has 3 columns (id, value, label), filter keeps 1 of 3 row groups
        // -> 3 RowGroupScanned events (one per column in the kept row group)
        assertThat(scannedRowGroups)
                .as("Only columns from the 1 kept row group should be scanned")
                .isEqualTo(3);
    }

    // ==================== Helpers ====================

    private long readAndMeasureSocketBytes(String file, ColumnProjection projection,
            FilterPredicate filter) throws Exception {
        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", file))) {
            try (RowReader rows = filter != null
                    ? reader.createRowReader(projection, filter)
                    : reader.createRowReader(projection)) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }

        jfrEvents.awaitEvents();

        return jfrEvents
                .filter(e -> "jdk.SocketRead".equals(e.getEventType().getName()))
                .mapToLong(e -> e.getLong("bytesRead"))
                .sum();
    }
}
