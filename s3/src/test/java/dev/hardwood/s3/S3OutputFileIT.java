/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Writes Parquet files through [S3Source#outputFile(String, String)] to the shared s3proxy, in a
/// bucket of its own, and reads them back through [S3InputFile].
class S3OutputFileIT {

    private static final int PART_SIZE = 5 * 1024 * 1024;
    private static final FileSchema SCHEMA = FileSchema.builder("events")
            .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
            .build();
    /// Uncompressed `PLAIN` in row groups of 500,000 rows (4 MB), so that 2,000,000 rows span
    /// several parts and the first part is uploaded before the file is closed.
    private static final WriterConfig CONFIG = WriterConfig.builder()
            .codec(CompressionCodec.UNCOMPRESSED)
            .encoding(ColumnEncoding.PLAIN)
            .rowGroupTargetRows(500_000)
            .build();
    /// Bytes standing in for a checked-in fixture, which `withObject(Path)` hard-links into the
    /// bucket where it can.
    private static final byte[] FIXTURE = "a checked-in fixture".getBytes(StandardCharsets.UTF_8);

    private static Path fixture;
    private static TestBucket bucket;
    private static String bucketName;
    private static S3UploadTestClient observer;
    private static S3Source source;

    @BeforeAll
    static void setUp() throws IOException {
        // Under the module's target directory, on the same file system as the server's data dir.
        fixture = Files.createTempFile(Path.of("target"), "fixture", ".parquet");
        Files.write(fixture, FIXTURE);
        bucket = S3Proxy.get().bucketFor(S3OutputFileIT.class).withObject("linked.parquet", fixture);
        bucket.create();
        bucketName = bucket.name();
        observer = new S3UploadTestClient(bucket.endpoint());
        source = S3Source.builder()
                .endpoint(bucket.endpoint())
                .pathStyle(true)
                .credentials(S3Credentials.of(S3Proxy.ACCESS_KEY, S3Proxy.SECRET_KEY))
                .uploadPartSize(PART_SIZE)
                .build();
    }

    @AfterAll
    static void tearDown() throws IOException {
        source.close();
        try {
            observer.deleteObjects(bucketName);
        }
        finally {
            observer.close();
            bucket.delete();
        }
        Files.deleteIfExists(fixture);
    }

    /// 1,000 rows fit one part and are sent in one `PUT`; 2,000,000 uncompressed `INT64` rows
    /// span several parts.
    @ParameterizedTest
    @ValueSource(ints = { 1_000, 2_000_000 })
    void writtenFileReadsBackThroughS3(int rows) throws IOException {
        String key = "dir/rows-" + rows + ".parquet";

        writeRows(source.outputFile(bucketName, key), rows);

        assertThat(sum(key)).isEqualTo((long) rows * (rows - 1) / 2);
        assertThat(observer.uploads(bucketName, key)).isEmpty();
    }

    @Test
    void abortedWriterLeavesNoObjectAndNoUpload() throws IOException {
        String key = "aborted.parquet";
        ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile(bucketName, key), SCHEMA, CONFIG);
        writeIds(writer, 2_000_000);
        assertThat(observer.uploads(bucketName, key)).hasSize(1);

        writer.abort();

        assertThat(observer.uploads(bucketName, key)).isEmpty();
        assertThat(observer.exists(bucketName, key)).isFalse();
    }

    @Test
    void replacesAnExistingObjectOnlyOnClose() throws IOException {
        String key = "replaced.parquet";
        writeRows(source.outputFile(bucketName, key), 10);

        ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile(bucketName, key), SCHEMA, CONFIG);
        writeIds(writer, 2_000_000);
        assertThat(sum(key)).isEqualTo(45L);
        writer.close();

        assertThat(sum(key)).isEqualTo(2_000_000L * 1_999_999 / 2);
    }

    /// The shared server's data dir is writable, and `withObject(Path)` hard-links fixtures into
    /// it. s3proxy writes an object to a temporary file and renames it over the old one, so writing
    /// over a linked object leaves the fixture as it was; an s3proxy that wrote in place would
    /// change the checked-in file.
    @Test
    void writingOverALinkedObjectLeavesTheFixtureIntact() throws IOException {
        assertThat(Files.getAttribute(fixture, "unix:nlink")).as("the fixture is hard-linked into the bucket").isEqualTo(2);

        writeRows(source.outputFile(bucketName, "linked.parquet"), 10);

        assertThat(Files.readAllBytes(fixture)).isEqualTo(FIXTURE);
        assertThat(sum("linked.parquet")).isEqualTo(45L);
    }

    @Test
    void failsToPublishIntoAMissingBucket() throws IOException {
        ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile("no-such-bucket", "file.parquet"), SCHEMA, CONFIG);
        writeIds(writer, 10);

        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasMessage("PUT s3://no-such-bucket/file.parquet failed: HTTP 404 NoSuchBucket: The specified bucket does not exist");
    }

    private static void writeRows(OutputFile out, int rows) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA, CONFIG)) {
            writeIds(writer, rows);
        }
    }

    private static void writeIds(ParquetFileWriter writer, int rows) throws IOException {
        int batch = 100_000;
        for (int start = 0; start < rows; start += batch) {
            long[] ids = new long[Math.min(batch, rows - start)];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = start + i;
            }
            writer.columnWriter().writeBatch(b -> b.longs("id", ids));
        }
    }

    private static long sum(String key) throws IOException {
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(bucketName, key));
                RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                sum += rows.getLong("id");
            }
        }
        return sum;
    }
}
