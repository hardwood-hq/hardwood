/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InMemoryOutputFile;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Timeout(30)
class S3OutputFileIT {

    private static final int PART_SIZE = 5 * 1024 * 1024;
    private static final FileSchema SCHEMA = FileSchema.builder("events")
            .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED).build();
    private static final WriterConfig CONFIG = WriterConfig.builder()
            .codec(CompressionCodec.UNCOMPRESSED).encoding(ColumnEncoding.PLAIN)
            .rowGroupTargetRows(200_000).build();

    private static final TestBucket BUCKET = S3Proxy.get().bucketFor(S3OutputFileIT.class);
    private static S3Source source;
    private static S3UploadTestClient observer;
    private static S3UploadServer uploadServer;

    @BeforeAll
    static void setup() throws IOException {
        BUCKET.create();
        uploadServer = new S3UploadServer();
        source = source(uploadServer.endpoint());
        source.api().createBucket(BUCKET.name());
        observer = new S3UploadTestClient(uploadServer.endpoint());
    }

    @AfterAll
    static void tearDown() throws IOException {
        source.close();
        try (S3UploadTestClient closingObserver = observer) {
            for (S3UploadTestClient.Upload upload : closingObserver.uploads(BUCKET.name(), null)) {
                closingObserver.abort(BUCKET.name(), upload);
            }
            closingObserver.deleteBucket(BUCKET.name());
        }
        finally {
            uploadServer.close();
            BUCKET.delete();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 5242879, 5242880, 5242881, 10485760, 10485761 })
    void exactBytesAcrossRealUploadBoundaries(int length) throws Exception {
        String key = "boundary-" + length;
        byte[] bytes = bytes(length);
        OutputFile out = source.outputFile(BUCKET.name(), key);
        out.create();
        out.write(ByteBuffer.wrap(bytes));
        assertThat(out.position()).isEqualTo(length);
        assertThat(observer.head(BUCKET.name(), key).statusCode()).isEqualTo(404);
        assertThat(observer.uploads(BUCKET.name(), key)).hasSize(length < PART_SIZE ? 0 : 1);
        out.close();
        assertThat(observer.get(BUCKET.name(), key)).isEqualTo(bytes);
        HttpResponse<byte[]> head = observer.head(BUCKET.name(), key);
        assertThat(head.statusCode()).isEqualTo(200);
        assertThat(head.headers().firstValueAsLong("Content-Length")).hasValue(length);
        assertThatCode(() -> UUID.fromString(head.headers().firstValue("x-amz-meta-hardwood-write-id").orElseThrow()))
                .doesNotThrowAnyException();
        assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
    }

    @Test
    void smallObjectPublicationRequiresWritableStorage() throws Exception {
        try (S3Source filesystem = source(BUCKET.endpoint());
             S3UploadTestClient files = new S3UploadTestClient(BUCKET.endpoint())) {
            OutputFile out = filesystem.outputFile(BUCKET.name(), "writable");
            out.create();
            out.write(ByteBuffer.wrap(new byte[] { 7 }));
            assertThatCode(out::close).doesNotThrowAnyException();
            assertThat(files.get(BUCKET.name(), "writable")).containsExactly(7);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void writesParquetThroughBothWriterApisAndReadsThroughS3(boolean rows) throws Exception {
        String key = "nested/events +%雪-" + rows + ".parquet";
        try (ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile(BUCKET.uri(key)), SCHEMA, CONFIG)) {
            if (rows) {
                for (long value : new long[] { 7, 11, 19 }) {
                    writer.rowWriter().writeRow(row -> row.setLong("id", value));
                }
            }
            else {
                writer.columnWriter().writeBatch(batch -> batch.longs("id", new long[] { 7, 11, 19 }));
            }
        }
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(BUCKET.uri(key)));
             RowReader readerRows = reader.rowReader()) {
            for (long value : new long[] { 7, 11, 19 }) {
                assertThat(readerRows.hasNext()).isTrue();
                readerRows.next();
                assertThat(readerRows.getLong("id")).isEqualTo(value);
            }
            assertThat(readerRows.hasNext()).isFalse();
        }
        assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
    }

    @Test
    void zeroRowParquetIsReadable() throws Exception {
        String key = "zero.parquet";
        ParquetFileWriter.create(source.outputFile(BUCKET.name(), key), SCHEMA, CONFIG).close();
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(BUCKET.name(), key));
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().numRows()).isZero();
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void multipartParquetPreservesEveryRowAndExistingObjectUntilClose() throws Exception {
        String key = "many-rows.parquet";
        byte[] old = { 3, 1, 4 };
        observer.put(BUCKET.name(), key, old);
        try (ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile(BUCKET.name(), key), SCHEMA, CONFIG)) {
            writeRows(writer, 900_000);
            writer.endRowGroup();
            assertThat(observer.uploads(BUCKET.name(), key)).hasSize(1);
            assertThat(observer.get(BUCKET.name(), key)).isEqualTo(old);
        }
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(BUCKET.name(), key));
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(5);
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(count++);
            }
            assertThat(count).isEqualTo(900_000);
        }
        assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
    }

    @Test
    void footerCanCrossMultipartBoundary() throws Exception {
        FileSchema schema = FileSchema.builder("blob")
                .addColumn("data", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED).build();
        int probeSize = PART_SIZE - 1024;
        InMemoryOutputFile probe = OutputFile.inMemory();
        long overhead;
        try (ParquetFileWriter writer = ParquetFileWriter.create(probe, schema, CONFIG)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes("data", new byte[][] { bytes(probeSize) }));
            writer.endRowGroup();
            overhead = probe.position() - probeSize;
        }
        byte[] data = bytes(Math.toIntExact(PART_SIZE - overhead - 4));
        String key = "footer-boundary.parquet";
        try (S3UploadFaultProxy proxy = new S3UploadFaultProxy(uploadServer.endpoint());
             S3Source destination = source(proxy.endpoint())) {
            OutputFile out = destination.outputFile(BUCKET.name(), key);
            try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, CONFIG)) {
                writer.columnWriter().writeBatch(batch -> batch.bytes("data", new byte[][] { data }));
                writer.endRowGroup();
                // Compact metadata can vary by a byte as the payload size changes.
                assertThat(out.position()).isBetween(PART_SIZE - 16L, PART_SIZE - 1L);
                assertThat(observer.head(BUCKET.name(), key).statusCode()).isEqualTo(404);
            }
            assertThat(proxy.requests.stream().filter(request -> "PUT".equals(request.method())).map(S3UploadFaultProxy.Request::bytes))
                    .hasSize(2).first().isEqualTo(PART_SIZE);
        }
        try (ParquetFileReader reader = ParquetFileReader.open(source.inputFile(BUCKET.name(), key));
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getBinary("data")).isEqualTo(data);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedWriterOrCallerAbortRemovesUploadedParts(boolean abort) throws Exception {
        String key = "cancel-" + abort;
        OutputFile out = source.outputFile(BUCKET.name(), key);
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA, CONFIG)) {
            writeRows(writer, 700_000);
            writer.endRowGroup();
            assertThat(observer.uploads(BUCKET.name(), key)).hasSize(1);
            if (abort) {
                writer.abort();
            }
            else {
                assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> {
                    throw new IllegalArgumentException("producer failed");
                })).isInstanceOf(IllegalArgumentException.class);
            }
        }
        assertThat(observer.head(BUCKET.name(), key).statusCode()).isEqualTo(404);
        assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
    }

    @Test
    void rejectedPartPreservesExistingObjectAndRemovesKnownUpload() throws Exception {
        String key = "part-failure";
        byte[] old = { 1, 2, 3 };
        observer.put(BUCKET.name(), key, old);
        try (S3UploadFaultProxy proxy = new S3UploadFaultProxy(uploadServer.endpoint());
             S3Source destination = source(proxy.endpoint())) {
            proxy.rejectedPart = 2;
            OutputFile out = destination.outputFile(BUCKET.name(), key);
            out.create();
            out.write(ByteBuffer.wrap(new byte[PART_SIZE]));
            assertThat(observer.uploads(BUCKET.name(), key)).hasSize(1);
            Throwable failure = catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])));
            assertThat(failure).isInstanceOf(IOException.class);
            // The pinned proxy omits required ListParts fields even after successful
            // abort. The sink preserves the original failure and reports cleanup as
            // inconclusive; independent ListMultipartUploads below proves removal.
            assertThat(failure.getSuppressed()).isNotEmpty();
            assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining("cleanup");
            assertThat(proxy.publicationCount()).isZero();
            assertThat(observer.get(BUCKET.name(), key)).isEqualTo(old);
            assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void lostPublicationResponseRecoversFromRealMetadata(boolean multipart) throws Exception {
        String key = "lost-" + multipart;
        byte[] data = bytes(multipart ? PART_SIZE + 7 : 3);
        try (S3UploadFaultProxy proxy = new S3UploadFaultProxy(uploadServer.endpoint());
             S3Source destination = source(proxy.endpoint())) {
            OutputFile out = destination.outputFile(BUCKET.name(), key);
            out.create();
            out.write(ByteBuffer.wrap(data));
            proxy.losePublication = true;
            assertThatCode(out::close).doesNotThrowAnyException();
            assertThat(proxy.publicationCount()).isEqualTo(1);
            assertThat(proxy.requests.stream().map(S3UploadFaultProxy.Request::method)).contains("HEAD").doesNotContain("DELETE");
            out.close();
            assertThat(proxy.publicationCount()).isEqualTo(1);
            assertThat(observer.get(BUCKET.name(), key)).isEqualTo(data);
            assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void inconclusiveVerificationNeverDeletesCompletedOrReplacementObject(boolean replacement) throws Exception {
        String key = "inconclusive-" + replacement;
        try (S3UploadFaultProxy proxy = new S3UploadFaultProxy(uploadServer.endpoint());
             S3Source destination = source(proxy.endpoint())) {
            OutputFile out = destination.outputFile(BUCKET.name(), key);
            out.create();
            byte[] data = { 1, 2, 3 };
            out.write(ByteBuffer.wrap(data));
            proxy.losePublication = true;
            proxy.denyHead = !replacement;
            proxy.replaceOnHead = replacement;
            Throwable failure = catchThrowable(out::close);
            assertThat(failure).isInstanceOf(IOException.class);
            assertThat(Arrays.stream(failure.getSuppressed()).map(Throwable::getMessage))
                    .anyMatch(message -> message.contains("could not be confirmed") && message.contains("expected length 3"));
            out.discard();
            out.close();
            assertThat(proxy.publicationCount()).isEqualTo(1);
            assertThat(proxy.requests.stream().map(S3UploadFaultProxy.Request::method)).doesNotContain("DELETE");
            assertThat(observer.get(BUCKET.name(), key)).isEqualTo(replacement ? new byte[] { 9, 8, 7 } : data);
        }
    }

    @Test
    void abortFailureLeavesObservablePendingUploadAndExplicitDiscardCanRetry() throws Exception {
        String key = "cleanup-failure";
        try (S3UploadFaultProxy proxy = new S3UploadFaultProxy(uploadServer.endpoint());
             S3Source destination = source(proxy.endpoint())) {
            OutputFile out = destination.outputFile(BUCKET.name(), key);
            out.create();
            out.write(ByteBuffer.wrap(new byte[PART_SIZE]));
            proxy.rejectAbort = true;
            assertThatThrownBy(out::discard).isInstanceOf(IOException.class);
            assertThat(observer.head(BUCKET.name(), key).statusCode()).isEqualTo(404);
            assertThat(observer.uploads(BUCKET.name(), key)).hasSize(1);
            proxy.rejectAbort = false;
            out.discard();
            out.close();
            assertThat(observer.uploads(BUCKET.name(), key)).isEmpty();
            assertThat(proxy.publicationCount()).isZero();
        }
    }

    @Test
    void listingAndDiscardAffectOnlyTheOwnedUpload() throws Exception {
        OutputFile first = source.outputFile(BUCKET.name(), "ownership-a");
        OutputFile second = source.outputFile(BUCKET.name(), "ownership-z");
        first.create();
        second.create();
        first.write(ByteBuffer.wrap(new byte[PART_SIZE]));
        second.write(ByteBuffer.wrap(new byte[PART_SIZE]));
        assertThat(observer.uploads(BUCKET.name(), "ownership-a")).hasSize(1);
        assertThat(observer.uploads(BUCKET.name(), "ownership-z")).hasSize(1);
        first.discard();
        assertThat(observer.uploads(BUCKET.name(), "ownership-a")).isEmpty();
        assertThat(observer.uploads(BUCKET.name(), "ownership-z")).hasSize(1);
        second.close();
        assertThat(observer.uploads(BUCKET.name(), "ownership-z")).isEmpty();
        assertThat(observer.head(BUCKET.name(), "ownership-z").statusCode()).isEqualTo(200);
    }

    private static S3Source source(String endpoint) {
        return S3Source.builder().endpoint(endpoint).pathStyle(true)
                .credentials(S3Credentials.of(S3Proxy.ACCESS_KEY, S3Proxy.SECRET_KEY))
                .uploadPartSize(PART_SIZE).maxRetries(0).requestTimeout(Duration.ofSeconds(3)).build();
    }

    private static void writeRows(ParquetFileWriter writer, int count) throws IOException {
        for (int start = 0; start < count; start += 100_000) {
            long[] values = new long[Math.min(100_000, count - start)];
            for (int i = 0; i < values.length; i++) {
                values[i] = start + i;
            }
            writer.columnWriter().writeBatch(batch -> batch.longs("id", values));
        }
    }

    private static byte[] bytes(int length) {
        byte[] bytes = new byte[length];
        // Reproducible nonzero values distinguish dropped, reordered, or padded bytes.
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 251);
        }
        return bytes;
    }
}
