/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
<<<<<<< HEAD
import java.nio.file.Files;
=======
import java.net.URI;
import java.nio.ByteBuffer;
>>>>>>> df90b7e (#118 Updated S3 reader to use selective chunking when filters are present)
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

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
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    static S3Source source;

    @BeforeAll
    static void setup() throws Exception {
        source = S3Source.builder()
                .endpoint(s3Mock.getHttpEndpoint())
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .build();

        source.api().createBucket("test-bucket");
        source.api().putObject("test-bucket", "plain_uncompressed.parquet", Files.readAllBytes(
                TEST_RESOURCES.resolve("plain_uncompressed.parquet")));
        source.api().putObject("test-bucket", "plain_uncompressed_with_nulls.parquet", Files.readAllBytes(
                TEST_RESOURCES.resolve("plain_uncompressed_with_nulls.parquet")));
        source.api().putObject("test-bucket", "column_index_pushdown.parquet", Files.readAllBytes(
                TEST_RESOURCES.resolve("column_index_pushdown.parquet")));
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
            try (RowReader rows = reader.createRowReader()) {
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
            try (RowReader rows = reader.createRowReader()) {
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
            try (RowReader rows = reader.createRowReader()) {
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

<<<<<<< HEAD
        long unfilteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
=======
        ByteCountingInputFile unfilteredFile = new ByteCountingInputFile(
                S3InputFile.of(s3, "test-bucket", "column_index_pushdown.parquet"));
        long unfilteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(unfilteredFile);
>>>>>>> df90b7e (#118 Updated S3 reader to use selective chunking when filters are present)
             ColumnReader col = reader.createColumnReader("id")) {
            while (col.nextBatch()) {
                unfilteredCount += col.getRecordCount();
            }
        }

<<<<<<< HEAD
        long filteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
=======
        ByteCountingInputFile filteredFile = new ByteCountingInputFile(
                S3InputFile.of(s3, "test-bucket", "column_index_pushdown.parquet"));
        long filteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(filteredFile);
>>>>>>> df90b7e (#118 Updated S3 reader to use selective chunking when filters are present)
             ColumnReader col = reader.createColumnReader("id", filter)) {
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
