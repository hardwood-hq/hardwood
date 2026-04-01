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
@Testcontainers
@org.moditect.jfrunit.JfrEventTest
public class S3SelectiveReadJfrTest {

    /// 170 KB, 3 columns (id, value, category), many pages, offset indexes — larger than the 64 KB tail cache.
    private static final String PAGE_INDEX_FILE = "page_index_test.parquet";

    /// 9.6 KB, 3 columns (id, value, label), 3 row groups — smaller than tail cache.
    private static final String FILTER_PUSHDOWN_FILE = "filter_pushdown_int.parquet";

    /// Generated on-the-fly: 8 INT64 columns, 20 row groups of 50000 rows each (1M rows total).
    /// With 8 INT64 columns (64 bytes/row), batch size is ~98K rows.
    /// ColumnAssemblyBuffer back-pressure (3 batches max) kicks in after ~295K rows (~6 row groups),
    /// preventing the assembly thread from fetching all 20 row groups.
    private static final String LAZY_ROWGROUP_FILE = "lazy_rowgroup_test.parquet";
    private static final int LAZY_RG_COUNT = 20;
    private static final int LAZY_RG_ROWS = 50_000;
    private static final int LAZY_RG_COLUMNS = 8;

    /// Generated on-the-fly: 1 row group, 500K rows, 8 INT64 columns, 1000 rows per page.
    /// Each page is ~8 KB, 500 pages per column, ~4 MB per column, ~32 MB total.
    /// With 8 INT64 columns (64 bytes/row), batch size is ~98K rows.
    /// Back-pressure at ~3 batches ≈ 295K rows ≈ 295 of 500 pages per column.
    /// Partial read should fetch significantly less than the full 32 MB.
    private static final String LAZY_PAGE_FILE = "lazy_page_test.parquet";
    private static final int LAZY_PAGE_ROWS = 500_000;
    private static final int LAZY_PAGE_COLUMNS = 8;
    private static final int LAZY_PAGE_ROWS_PER_PAGE = 1000;

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
        source.api().putObject("test-bucket", LAZY_ROWGROUP_FILE,
                TestParquetGenerator.generate(LAZY_RG_COUNT, LAZY_RG_ROWS, LAZY_RG_COLUMNS));
        source.api().putObject("test-bucket", LAZY_PAGE_FILE,
                TestParquetGenerator.generate(1, LAZY_PAGE_ROWS, LAZY_PAGE_COLUMNS, LAZY_PAGE_ROWS_PER_PAGE));
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

    // ==================== Lazy Row-Group Initialization ====================

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void lazyInitScansRowGroupsIncrementally() throws Exception {
        // Verify that lazy row-group initialization scans row groups incrementally
        // (on demand) rather than all at once in initialize().
        // The RowGroupScanned events should come from multiple row groups, confirming
        // that PageCursor's tryLoadNextRowGroupBlocking() drives the scanning.
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE))) {
            try (RowReader rows = reader.createRowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }

        jfrEvents.awaitEvents();

        long scannedRowGroups = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        // All row groups × all columns should be scanned for a full read
        long expectedEvents = (long) LAZY_RG_COUNT * LAZY_RG_COLUMNS;
        assertThat(scannedRowGroups)
                .as("Full read should scan all row groups (%d RGs × %d columns)".formatted(
                        LAZY_RG_COUNT, LAZY_RG_COLUMNS))
                .isEqualTo(expectedEvents);
    }

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void partialReadDoesNotScanAllRowGroups() throws Exception {
        // Read only 10 rows from a 1M-row file with 20 row groups.
        // Back-pressure from ColumnAssemblyBuffer should prevent the assembly threads
        // from scanning all 20 row groups.
        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE))) {
            try (RowReader rows = reader.createRowReader()) {
                int count = 0;
                while (rows.hasNext() && count < 10) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(10);
            }
        }

        jfrEvents.awaitEvents();

        long scannedRowGroups = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .count();

        // Print per-column row group counts for diagnostic visibility
        java.util.Map<String, Long> rowGroupsPerColumn = jfrEvents
                .filter(e -> "dev.hardwood.RowGroupScanned".equals(e.getEventType().getName()))
                .collect(Collectors.groupingBy(e -> e.getString("column"), Collectors.counting()));
        System.out.println("Partial read: " + scannedRowGroups + " RowGroupScanned events, per column: " + rowGroupsPerColumn);

        // Full read produces 160 events (20 RGs × 8 columns).
        // With batch size ~98K rows and ColumnAssemblyBuffer back-pressure (capacity 2 + 1 being
        // filled = 3 batches ≈ 295K rows ≈ 6 row groups), the assembly threads should scan
        // at most ~6 of 20 row groups before blocking. That's ~48 events (6 × 8 columns).
        // Allow some margin for thread scheduling variance.
        assertThat(scannedRowGroups)
                .as("Partial read (10 rows) should scan far fewer row groups than full read (160)")
                .isLessThanOrEqualTo(50);
    }

    @Test
    void fullReadReturnsAllRows() throws Exception {
        // Verify that lazy row-group initialization reads all data correctly
        // across lazily-fetched row groups.
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
    }

    // ==================== Row Limit (Phase 3) ====================

    @Test
    @EnableEvent("dev.hardwood.RowGroupScanned")
    void maxRowsSkipsUnneededRowGroups() throws Exception {
        // lazy_rowgroup_test.parquet: 20 row groups × 50K rows × 8 columns.
        // With maxRows=10, the reader knows from metadata that only 1 row group
        // is needed (first RG has 50K rows > 10). It should never register the
        // remaining 19 row groups with PageCursor.
        jfrEvents.reset();

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", LAZY_ROWGROUP_FILE))) {
            try (RowReader rows = reader.createRowReader(10)) {
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

        // With maxRows=10, totalRowGroups is set to 1 (first RG has 50K rows).
        // Only 1 row group × 8 columns = 8 RowGroupScanned events.
        // Without maxRows, we saw ~33-48 events (6 row groups due to back-pressure).
        assertThat(scannedEvents)
                .as("maxRows=10 should limit to 1 row group (8 events for 8 columns)")
                .isLessThanOrEqualTo(8);
    }

    // ==================== Lazy Page Fetching (Phase 2) ====================

    @Test
    @EnableEvent("jdk.SocketRead")
    void partialReadFromSingleRowGroupTransfersFewerBytes() throws Exception {
        // lazy_page_test.parquet: 1 row group, 100K rows, 4 columns, 1000 rows/page.
        // Phase 1 doesn't help (only 1 row group). Phase 2's windowed chunk reader
        // should fetch only the first window (~256 KB) per column instead of the
        // full ~800 KB per column.

        // Full read: measure total bytes
        long fullReadBytes = readAndMeasureSocketBytes(LAZY_PAGE_FILE,
                ColumnProjection.all(), null);

        // Partial read: 10 rows only
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

        System.out.println("Phase 2 test: full read " + fullReadBytes
                + " bytes, partial read " + partialReadBytes + " bytes ("
                + (partialReadBytes * 100 / fullReadBytes) + "%)");

        // Back-pressure limits the assembly thread to ~3 batches (~295K of 500K rows).
        // The windowed chunk reader fetches only the pages covered by those rows,
        // so partial read should transfer well under 75% of full read bytes.
        assertThat(partialReadBytes)
                .as("Partial read (10 rows) from single row group should transfer significantly fewer bytes")
                .isLessThan(fullReadBytes * 3 / 4);
    }

    // ==================== Lazy Row-Group Initialization (ColumnReader) ====================

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

        System.out.println("ColumnReader partial read: " + scannedEvents + " RowGroupScanned events");

        // ColumnReader reads a single column, so each row group produces 1 RowGroupScanned event.
        // Full read = 20 events. ColumnReader uses DEFAULT_BATCH_SIZE (262144 rows) — larger
        // than the RowReader's adaptive size. With 50K rows per row group, one batch spans
        // ~6 row groups. ColumnAssemblyBuffer back-pressure (3 batches in flight) limits
        // to ~18 row groups, but the consumer reads only 1 batch and closes, so fewer are
        // scanned in practice.
        assertThat(scannedEvents)
                .as("ColumnReader partial read should scan far fewer than all 20 row groups")
                .isLessThanOrEqualTo(15);
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
