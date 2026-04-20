/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.moditect.jfrunit.EnableEvent;
import org.moditect.jfrunit.JfrEvents;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.reader.ColumnReader;
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
///
/// The lazy fetch tests override `hardwood.internal.sequentialChunkSize` to a small value
/// so that the generated test files stay small while still exercising multi-chunk
/// behaviour.
@Testcontainers
@org.moditect.jfrunit.JfrEventTest
public class S3SelectiveReadJfrTest {

    private static final System.Logger LOG = System.getLogger(S3SelectiveReadJfrTest.class.getName());

    /// 170 KB, 3 columns (id, value, category), many pages, offset indexes — larger than the 64 KB tail cache.
    private static final String PAGE_INDEX_FILE = "page_index_test.parquet";

    /// 9.6 KB, 3 columns (id, value, label), 3 row groups — smaller than tail cache.
    private static final String FILTER_PUSHDOWN_FILE = "filter_pushdown_int.parquet";

    /// Generated on-the-fly: 8 INT64 columns, 20 row groups of 50000 rows each (1M rows total).
    private static final String LAZY_ROWGROUP_FILE = "lazy_rowgroup_test.parquet";
    private static final int LAZY_RG_COUNT = 20;
    private static final int LAZY_RG_ROWS = 50_000;
    private static final int LAZY_RG_COLUMNS = 8;

    /// Generated on-the-fly for cross-RG lazy fetch tests.
    /// 40 row groups, each column chunk ~400 KB (50K rows × 8 bytes).
    /// With the 128 KB test chunk size, each column spans ~3 chunks, and
    /// partial reads skip most RGs entirely.
    private static final String LAZY_PAGE_FILE = "lazy_page_test.parquet";
    private static final int LAZY_PAGE_RG_COUNT = 40;
    private static final int LAZY_PAGE_RG_ROWS = 50_000;
    private static final int LAZY_PAGE_COLUMNS = 4;

    /// Generated on-the-fly for early-close back-pressure tests.
    /// 10 row groups with many pages per column. Each column chunk is
    /// ~1.6 MB (200K rows × 8 bytes); with the 128 KB test chunk size,
    /// that's ~12 chunks per column per RG. One batch (~196K rows) consumes
    /// nearly one entire RG, so the pipeline processes ~1-2 RGs before
    /// back-pressure from the BatchExchange stops it. With 10 RGs, early
    /// close should save at least 75% vs a full read.
    private static final String LARGE_RG_FILE = "large_rg_test.parquet";
    private static final int LARGE_RG_COUNT = 10;
    private static final int LARGE_RG_ROWS = 200_000;
    private static final int LARGE_RG_COLUMNS = 4;
    private static final int LARGE_RG_ROWS_PER_PAGE = 1_000;

    /// Override the sequential chunk size so the generated test files stay small
    /// while still producing multiple chunks per column.
    private static final int TEST_CHUNK_SIZE = 128 * 1024;

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static GenericContainer<?> s3 = S3ProxyContainers.filesystemBacked()
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve(PAGE_INDEX_FILE)),
                    S3ProxyContainers.objectPath(PAGE_INDEX_FILE))
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve(FILTER_PUSHDOWN_FILE)),
                    S3ProxyContainers.objectPath(FILTER_PUSHDOWN_FILE))
            .withCopyToContainer(
                    Transferable.of(TestParquetGenerator.generate(LAZY_RG_COUNT, LAZY_RG_ROWS, LAZY_RG_COLUMNS)),
                    S3ProxyContainers.objectPath(LAZY_ROWGROUP_FILE))
            .withCopyToContainer(
                    Transferable.of(TestParquetGenerator.generate(LAZY_PAGE_RG_COUNT, LAZY_PAGE_RG_ROWS, LAZY_PAGE_COLUMNS)),
                    S3ProxyContainers.objectPath(LAZY_PAGE_FILE))
            .withCopyToContainer(
                    Transferable.of(TestParquetGenerator.generate(LARGE_RG_COUNT, LARGE_RG_ROWS, LARGE_RG_COLUMNS, LARGE_RG_ROWS_PER_PAGE)),
                    S3ProxyContainers.objectPath(LARGE_RG_FILE));

    static S3Source source;

    public JfrEvents jfrEvents = new JfrEvents();

    @BeforeAll
    static void setup() {
        // Override sequential chunk size for this test class
        System.setProperty("hardwood.internal.sequentialChunkSize", String.valueOf(TEST_CHUNK_SIZE));

        source = S3Source.builder()
                .endpoint(S3ProxyContainers.endpoint(s3))
                .pathStyle(true)
                .credentials(S3Credentials.of(S3ProxyContainers.ACCESS_KEY, S3ProxyContainers.SECRET_KEY))
                .build();
    }

    @AfterAll
    static void tearDown() {
        System.clearProperty("hardwood.internal.sequentialChunkSize");
        source.close();
    }

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void fullReadScansAllRowGroupsAndReturnsAllRows() throws Exception {
        int expectedRows = LAZY_RG_COUNT * LAZY_RG_ROWS;
        int rowCount = 0;
        long lastC0 = -1;

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE))) {
            try (RowReader rows = reader.createRowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                    lastC0 = rows.getLong("c0");
                    rowCount++;
                }
            }
        }

        assertThat(rowCount)
                .as("Full read should return all rows across all row groups")
                .isEqualTo(expectedRows);
        assertThat(lastC0)
                .as("Last c0 value should be expectedRows - 1")
                .isEqualTo(expectedRows - 1);

        jfrEvents.awaitEvents();

        long scannedEvents = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        long expectedEvents = (long) LAZY_RG_COUNT * LAZY_RG_COLUMNS;
        assertThat(scannedEvents)
                .as("Full read should scan all row groups (%d RGs × %d columns)".formatted(
                        LAZY_RG_COUNT, LAZY_RG_COLUMNS))
                .isEqualTo(expectedEvents);
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

    // ==================== Row Limit ====================

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    @EnableEvent("jdk.SocketRead")
    void maxRowsSkipsUnneededRowGroups() throws Exception {
        // lazy_rowgroup_test.parquet: 20 row groups × 50K rows × 8 columns.
        // With maxRows=10, the reader knows from metadata that only 1 row group
        // is needed (first RG has 50K rows > 10).

        long fullReadBytes = readAndMeasureSocketBytes(LAZY_ROWGROUP_FILE,
                ColumnProjection.all(), null);

        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE))) {
            try (RowReader rows = reader.createRowReader(ColumnProjection.all(), null, 10L)) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(10);
            }
        }

        jfrEvents.awaitEvents();

        long scannedEvents = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        long partialReadBytes = jfrEvents
                .filter(e -> "jdk.SocketRead".equals(e.getEventType().getName()))
                .mapToLong(e -> e.getLong("bytesRead"))
                .sum();

        LOG.log(System.Logger.Level.INFO,
                "maxRowsSkipsUnneededRowGroups: full={0} bytes, maxRows=10 read={1} bytes ({2}%), scanned={3} events",
                fullReadBytes, partialReadBytes, partialReadBytes * 100 / fullReadBytes, scannedEvents);

        // With maxRows=10, totalRowGroups is set to 1 (first RG has 50K rows).
        // Only 1 row group × 8 columns = 8 RowGroupScanned events.
        assertThat(scannedEvents)
                .as("maxRows=10 should limit to 1 row group (8 events for 8 columns); "
                        + "scanned=%d".formatted(scannedEvents))
                .isLessThanOrEqualTo(8);

        // maxRows=10 limits to 1 of 40 RGs — should transfer well under 25%.
        assertThat(partialReadBytes)
                .as("maxRows=10 should transfer well under 10%% of full read; "
                        + "full=%,d bytes, partial=%,d bytes (%d%%)"
                                .formatted(fullReadBytes, partialReadBytes,
                                        partialReadBytes * 100 / fullReadBytes))
                .isLessThan(fullReadBytes / 10);
    }

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    @EnableEvent("jdk.SocketRead")
    void negativeMaxRowsOnlyFetchesLastRowGroup() throws Exception {
        // lazy_rowgroup_test.parquet: 20 row groups × 50K rows × 8 columns.
        // A tail of 10 rows fits entirely within the last row group, so only
        // that row group should be fetched; bytes transferred should be a small
        // fraction of a full read.

        long fullReadBytes = readAndMeasureSocketBytes(LAZY_ROWGROUP_FILE,
                ColumnProjection.all(), null);

        jfrEvents.reset();

        long expectedFirstC0 = (long) LAZY_RG_COUNT * LAZY_RG_ROWS - 10;
        long expectedLastC0 = (long) LAZY_RG_COUNT * LAZY_RG_ROWS - 1;

        long firstC0 = -1;
        long lastC0 = -1;
        int count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE))) {
            try (RowReader rows = reader.createRowReader(ColumnProjection.all(), null, -10L)) {
                while (rows.hasNext()) {
                    rows.next();
                    long c0 = rows.getLong("c0");
                    if (count == 0) {
                        firstC0 = c0;
                    }
                    lastC0 = c0;
                    count++;
                }
            }
        }

        assertThat(count).isEqualTo(10);
        assertThat(firstC0).isEqualTo(expectedFirstC0);
        assertThat(lastC0).isEqualTo(expectedLastC0);

        jfrEvents.awaitEvents();

        long scannedEvents = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        long partialReadBytes = jfrEvents
                .filter(e -> "jdk.SocketRead".equals(e.getEventType().getName()))
                .mapToLong(e -> e.getLong("bytesRead"))
                .sum();

        LOG.log(System.Logger.Level.INFO,
                "negativeMaxRowsOnlyFetchesLastRowGroup: full={0} bytes, tail=-10 read={1} bytes ({2}%), scanned={3} events",
                fullReadBytes, partialReadBytes, partialReadBytes * 100 / fullReadBytes, scannedEvents);

        // Tail of 10 rows fits inside the final row group, so only 1 RG ×
        // 8 columns = 8 RowGroupScanned events should fire.
        assertThat(scannedEvents)
                .as("Tail of 10 rows should scan only the final row group (8 events for 8 columns); "
                        + "scanned=%d".formatted(scannedEvents))
                .isLessThanOrEqualTo(LAZY_RG_COLUMNS);

        // Tail fetches only 1 of 20 row groups — bytes transferred should be
        // well under 10% of a full read.
        assertThat(partialReadBytes)
                .as("Tail of 10 rows should transfer well under 10%% of full read; "
                        + "full=%,d bytes, partial=%,d bytes (%d%%)"
                                .formatted(fullReadBytes, partialReadBytes,
                                        partialReadBytes * 100 / fullReadBytes))
                .isLessThan(fullReadBytes / 10);
    }

    // ==================== Lazy Fetch: Early Close ====================

    @Test
    @EnableEvent("jdk.SocketRead")
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void earlyCloseTransfersFewerBytesThanFullRead() throws Exception {
        // lazy_page_test.parquet: 40 row groups × 50K rows × 4 INT64 columns.
        // Reading 10 rows and closing relies on pipeline back-pressure and
        // cancellation on close() to avoid fetching the entire file.

        long fullReadBytes = readAndMeasureSocketBytes(LAZY_PAGE_FILE,
                ColumnProjection.all(), null);

        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_PAGE_FILE))) {
            try (RowReader rows = reader.createRowReader()) {
                int count = 0;
                while (rows.hasNext() && count < 10) {
                    rows.next();
                    count++;
                }
            }
        }

        jfrEvents.awaitEvents();

        long partialReadBytes = jfrEvents
                .filter(e -> "jdk.SocketRead".equals(e.getEventType().getName()))
                .mapToLong(e -> e.getLong("bytesRead"))
                .sum();

        long scannedEvents = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        LOG.log(System.Logger.Level.INFO,
                "earlyCloseTransfersFewerBytesThanFullRead: full={0} bytes, partial={1} bytes ({2}%), scanned={3} events",
                fullReadBytes, partialReadBytes, partialReadBytes * 100 / fullReadBytes, scannedEvents);

        // The pipeline processes ~1-2 RGs before back-pressure and close()
        // stop it. With 40 RGs, the partial read should be well under 25%.
        assertThat(partialReadBytes)
                .as("Early close (10 rows from 40 RGs) should transfer under 25%% of full read; "
                        + "full=%,d bytes, partial=%,d bytes (%d%%)"
                                .formatted(fullReadBytes, partialReadBytes,
                                        partialReadBytes * 100 / fullReadBytes))
                .isLessThan(fullReadBytes / 4);

        // Full read scans 160 events (40 RGs × 4 columns). Back-pressure
        // should limit scanning to a fraction of row groups.
        assertThat(scannedEvents)
                .as("Early close should scan far fewer than all 40 RGs; scanned=%d"
                        .formatted(scannedEvents))
                .isLessThan(50);
    }

    @Test
    @EnableEvent("jdk.SocketRead")
    void earlyCloseFromLargeRowGroupDoesNotFetchEntireFile() throws Exception {
        // large_rg_test.parquet: 10 row groups × 200K rows × 4 INT64 columns.
        // One batch (~196K rows) consumes nearly one RG. Reading 10 rows and
        // closing should process at most ~1-2 RGs before BatchExchange
        // back-pressure and close() stop the pipeline. With 10 RGs total,
        // the partial read should be well under 25%.

        long fullReadBytes = readAndMeasureSocketBytes(LARGE_RG_FILE,
                ColumnProjection.all(), null);

        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LARGE_RG_FILE))) {
            try (RowReader rows = reader.createRowReader()) {
                int count = 0;
                while (rows.hasNext() && count < 10) {
                    rows.next();
                    count++;
                }
            }
        }

        jfrEvents.awaitEvents();

        long partialReadBytes = jfrEvents
                .filter(e -> "jdk.SocketRead".equals(e.getEventType().getName()))
                .mapToLong(e -> e.getLong("bytesRead"))
                .sum();

        LOG.log(System.Logger.Level.INFO,
                "earlyCloseFromLargeRowGroupDoesNotFetchEntireFile: full={0} bytes, partial={1} bytes ({2}%)",
                fullReadBytes, partialReadBytes, partialReadBytes * 100 / fullReadBytes);

        // The pipeline processes ~1-2 RGs before back-pressure and close()
        // stop it. With 10 RGs, the partial read should be well under 33%.
        assertThat(partialReadBytes)
                .as("Early close (10 rows from 10 RGs) should transfer under 25%% of full read; "
                        + "full=%,d bytes, partial=%,d bytes (%d%%)"
                                .formatted(fullReadBytes, partialReadBytes,
                                        partialReadBytes * 100 / fullReadBytes))
                .isLessThan(fullReadBytes / 3);
    }

    // ==================== ColumnReader ====================

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void columnReaderPartialReadDoesNotScanAllRowGroups() throws Exception {
        // ColumnReader has the same lazy row-group fetching as RowReader.
        // Reading a single batch from one column should not scan all 20 row groups.
        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE));
             ColumnReader col = reader.createColumnReader("c0")) {
            assertThat(col.nextBatch()).isTrue();
            // Consume one batch and close — don't read further
        }

        jfrEvents.awaitEvents();

        long scannedEvents = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        // ColumnReader reads a single column, so each row group produces 1 event.
        // Full read = 20 events. Consumer reads 1 batch and closes; back-pressure
        // limits scanning to a handful of RGs.
        assertThat(scannedEvents)
                .as("ColumnReader partial read should scan far fewer than all 20 row groups; "
                        + "scanned=%d".formatted(scannedEvents))
                .isLessThan(10);
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
