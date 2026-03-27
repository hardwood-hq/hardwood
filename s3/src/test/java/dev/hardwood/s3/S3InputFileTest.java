/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

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
    void columnIndexPageFiltering() throws Exception {
        // column_index_pushdown.parquet: 10000 rows, sorted id [0,9999], ~10 pages of 1024 values
        // Filter to id < 1000 should skip ~90% of pages via Column Index
        FilterPredicate filter = FilterPredicate.lt("id", 1000L);

        long unfilteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
             ColumnReader col = reader.createColumnReader("id")) {
            while (col.nextBatch()) {
                unfilteredCount += col.getRecordCount();
            }
        }

        long filteredCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(
                source.inputFile("test-bucket", "column_index_pushdown.parquet"));
             ColumnReader col = reader.createColumnReader("id", filter)) {
            while (col.nextBatch()) {
                filteredCount += col.getRecordCount();
            }
        }

        assertThat(unfilteredCount).isEqualTo(10000);
        assertThat(filteredCount).isLessThan(unfilteredCount);
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
