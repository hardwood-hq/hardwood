/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.RangeBackedInputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end test for [RangeBacking#SPARSE_TEMPFILE] against an S3
/// proxy backend. Asserts that repeat reads against the same logical
/// file hit the local mmap-backed cache instead of issuing fresh HTTP
/// GETs to S3.
@Testcontainers
class S3RangeBackingTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static GenericContainer<?> s3 = S3ProxyContainers.filesystemBacked()
            .withCopyFileToContainer(
                    MountableFile.forHostPath(TEST_RESOURCES.resolve("column_index_pushdown.parquet")),
                    S3ProxyContainers.objectPath("column_index_pushdown.parquet"));

    @TempDir
    static Path cacheDir;

    static S3Source backedSource;
    static S3Source bareSource;

    @BeforeAll
    static void setup() {
        backedSource = S3Source.builder()
                .endpoint(S3ProxyContainers.endpoint(s3))
                .pathStyle(true)
                .credentials(S3Credentials.of(S3ProxyContainers.ACCESS_KEY, S3ProxyContainers.SECRET_KEY))
                .rangeBacking(RangeBacking.SPARSE_TEMPFILE)
                .tempDir(cacheDir)
                .build();
        bareSource = S3Source.builder()
                .endpoint(S3ProxyContainers.endpoint(s3))
                .pathStyle(true)
                .credentials(S3Credentials.of(S3ProxyContainers.ACCESS_KEY, S3ProxyContainers.SECRET_KEY))
                .build();
    }

    @AfterAll
    static void tearDown() {
        backedSource.close();
        bareSource.close();
    }

    @Test
    void backedSourceWrapsInRangeBacked() {
        InputFile file = backedSource.inputFile("test-bucket", "column_index_pushdown.parquet");
        assertThat(file).isInstanceOf(RangeBackedInputFile.class);
        assertThat(((RangeBackedInputFile) file).delegate()).isInstanceOf(S3InputFile.class);
    }

    @Test
    void bareSourceReturnsBareS3InputFile() {
        InputFile file = bareSource.inputFile("test-bucket", "column_index_pushdown.parquet");
        assertThat(file).isInstanceOf(S3InputFile.class);
    }

    @Test
    void repeatReadAgainstSameRangeReducesHttpGets() throws Exception {
        InputFile cached = backedSource.inputFile("test-bucket", "column_index_pushdown.parquet");
        // Underlying S3InputFile to inspect network counters.
        S3InputFile inner = (S3InputFile) ((RangeBackedInputFile) cached).delegate();

        try (ParquetFileReader reader = ParquetFileReader.open(cached);
                ColumnReader col = reader.columnReader("id")) {
            while (col.nextBatch()) {
                // drain
            }
        }
        long requestsAfterFirst = inner.networkRequestCount();
        long bytesAfterFirst = inner.networkBytesFetched();
        assertThat(requestsAfterFirst).isPositive();

        long requestsBeforeSecond = inner.networkRequestCount();
        // Re-open + re-read against the *same* InputFile instance. The
        // range cache is alive, so byte ranges fetched on the first
        // pass should now be served from the local mmap.
        try (ParquetFileReader reader = ParquetFileReader.open(cached);
                ColumnReader col = reader.columnReader("id")) {
            while (col.nextBatch()) {
                // drain
            }
        }
        long requestsForSecond = inner.networkRequestCount() - requestsBeforeSecond;

        assertThat(requestsForSecond)
                .as("second pass should issue strictly fewer GETs than the first")
                .isLessThan(requestsAfterFirst);
        assertThat(inner.networkBytesFetched() - bytesAfterFirst)
                .as("second pass should fetch strictly fewer bytes than the first")
                .isLessThan(bytesAfterFirst);
    }

    @Test
    void exactSameRangeReadTwiceHitsCache() throws Exception {
        // Direct readRange() against the decorator: same offset/length
        // twice, the second goes to the mmap, no network call.
        InputFile cached = backedSource.inputFile("test-bucket", "column_index_pushdown.parquet");
        S3InputFile inner = (S3InputFile) ((RangeBackedInputFile) cached).delegate();
        cached.open();

        // Pick an offset that is outside the 64 KB tail cache so the
        // counter increments are unambiguous.
        long offset = 1024;
        int length = 4096;
        cached.readRange(offset, length);
        long requestsAfterFirst = inner.networkRequestCount();
        long bytesAfterFirst = inner.networkBytesFetched();

        cached.readRange(offset, length);

        assertThat(inner.networkRequestCount())
                .as("exact-match repeat read must not issue a new HTTP GET")
                .isEqualTo(requestsAfterFirst);
        assertThat(inner.networkBytesFetched()).isEqualTo(bytesAfterFirst);
    }

    @Test
    void rowReaderRereadReducesHttpGets() throws Exception {
        InputFile cached = backedSource.inputFile("test-bucket", "column_index_pushdown.parquet");
        S3InputFile inner = (S3InputFile) ((RangeBackedInputFile) cached).delegate();

        try (ParquetFileReader reader = ParquetFileReader.open(cached);
                RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
        long after = inner.networkRequestCount();
        long requestsBeforeSecond = inner.networkRequestCount();

        try (ParquetFileReader reader = ParquetFileReader.open(cached);
                RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
        long requestsForSecond = inner.networkRequestCount() - requestsBeforeSecond;

        assertThat(requestsForSecond)
                .as("RowReader re-read should issue fewer GETs than the first read")
                .isLessThan(after);
    }
}
