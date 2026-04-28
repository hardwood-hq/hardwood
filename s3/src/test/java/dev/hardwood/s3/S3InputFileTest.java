/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class S3InputFileTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static GenericContainer<?> s3 = S3ProxyContainers.filesystemBacked()
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve("plain_uncompressed.parquet")),
                    S3ProxyContainers.objectPath("plain_uncompressed.parquet"))
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve("plain_uncompressed_with_nulls.parquet")),
                    S3ProxyContainers.objectPath("plain_uncompressed_with_nulls.parquet"))
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve("column_index_pushdown.parquet")),
                    S3ProxyContainers.objectPath("column_index_pushdown.parquet"));

    static S3Source source;

    @BeforeAll
    static void setup() {
        source = S3Source.builder()
                .endpoint(S3ProxyContainers.endpoint(s3))
                .pathStyle(true)
                .credentials(S3Credentials.of(S3ProxyContainers.ACCESS_KEY, S3ProxyContainers.SECRET_KEY))
                .build();
    }

    @AfterAll
    static void tearDown() {
        source.close();
    }

    @Test
    void readMetadata() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "plain_uncompressed.parquet"))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
        }
    }

    @Test
    void readRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "plain_uncompressed.parquet"))) {
            try (RowReader rows = reader.rowReader()) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(3);
            }
        }
    }

    @Test
    void readRowValues() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "plain_uncompressed.parquet"))) {
            try (RowReader rows = reader.rowReader()) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(1L);
                assertThat(rows.getLong("value")).isEqualTo(100L);

                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(2L);
                assertThat(rows.getLong("value")).isEqualTo(200L);

                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(3L);
                assertThat(rows.getLong("value")).isEqualTo(300L);

                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void readWithNulls() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "plain_uncompressed_with_nulls.parquet"))) {
            try (RowReader rows = reader.rowReader()) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isGreaterThan(0);
            }
        }
    }

    @Test
    void fileNotFound() {
        assertThatThrownBy(() ->
                ParquetFileReader.open(
                        source.inputFile("test-bucket", "nonexistent.parquet")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void columnIndexPageFilteringReducesNetworkIo() throws Exception {
        // column_index_pushdown.parquet: 10000 rows, sorted id [0,9999], ~10 pages of 1024 values
        // Filter to id < 1000 should skip ~90% of pages via Column Index,
        // and page-range I/O should fetch only matching pages from S3
        FilterPredicate filter = FilterPredicate.lt("id", 1000L);

        ByteCountingInputFile unfilteredFile = new ByteCountingInputFile(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
        long unfilteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(unfilteredFile);
             ColumnReader col = reader.columnReader("id")) {
            while (col.nextBatch()) {
                unfilteredCount += col.getRecordCount();
            }
        }

        ByteCountingInputFile filteredFile = new ByteCountingInputFile(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
        long filteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(filteredFile);
             ColumnReader col = reader.buildColumnReader("id").filter(filter).build()) {
            while (col.nextBatch()) {
                filteredCount += col.getRecordCount();
            }
        }

        assertThat(unfilteredCount).isEqualTo(10000);
        assertThat(filteredCount).isLessThan(unfilteredCount);
        assertThat(filteredFile.bytesRead())
                .as("Filtered S3 read should transfer fewer bytes than unfiltered")
                .isLessThan(unfilteredFile.bytesRead());
    }

    /// InputFile wrapper that tracks total bytes fetched via readRange.
    private static class ByteCountingInputFile implements InputFile {

        private final InputFile delegate;
        private final AtomicLong totalBytesRead = new AtomicLong();

        ByteCountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        long bytesRead() {
            return totalBytesRead.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            totalBytesRead.addAndGet(length);
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void recordLevelFilteringOverS3() throws Exception {
        // column_index_pushdown.parquet: 10000 rows, sorted id [0,9999]
        // EQ filter for id=500 should return exactly 1 row via record-level filtering
        FilterPredicate filter = FilterPredicate.eq("id", 500L);

        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {

            int totalRows = 0;
            while (rows.hasNext()) {
                rows.next();
                totalRows++;
                assertThat(rows.getLong("id")).isEqualTo(500L);
            }
            assertThat(totalRows).isEqualTo(1);
        }
    }

    @Test
    void networkStatsCountOpenAndReadRangeButNotTailCacheHits() throws Exception {
        // column_index_pushdown.parquet is large enough that the column data
        // sits outside the 64 KB tail cache, so readRange must hit the
        // network — proving the counters increase past the open() baseline.
        // (A tiny fixture would fit entirely inside the tail cache and the
        // counters would correctly stay at 1.)
        S3InputFile file = source.inputFile("test-bucket", "column_index_pushdown.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            long openRequests = file.networkRequestCount();
            long openBytes = file.networkBytesFetched();
            assertThat(openRequests).isPositive();
            assertThat(openBytes).isPositive();

            try (ColumnReader col = reader.columnReader("id")) {
                while (col.nextBatch()) {
                    // drain
                }
            }

            assertThat(file.networkRequestCount()).isGreaterThan(openRequests);
            assertThat(file.networkBytesFetched()).isGreaterThan(openBytes);
        }
    }

    @Test
    void name() {
        S3InputFile file = source.inputFile("test-bucket", "data/file.parquet");
        assertThat(file.name()).isEqualTo("s3://test-bucket/data/file.parquet");
    }

    @Test
    void uriFactory() {
        S3InputFile file = source.inputFile("s3://test-bucket/data/file.parquet");
        assertThat(file.name()).isEqualTo("s3://test-bucket/data/file.parquet");
    }
}
