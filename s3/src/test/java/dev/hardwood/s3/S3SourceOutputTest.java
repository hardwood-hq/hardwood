/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class S3SourceOutputTest {

    private static final int MIN_PART_SIZE = 5 * 1024 * 1024;
    private static final FileSchema SCHEMA = FileSchema.builder("events")
            .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
            .build();

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger credentialCalls = new AtomicInteger();
    private HttpServer server;
    private HttpClient client;
    private volatile boolean rejectPublication;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        client.shutdownNow();
        server.stop(0);
    }

    @Test
    void factoriesAndCreatePerformNoNetworkOrCredentialResolution() throws Exception {
        try (S3Source source = builder().build()) {
            OutputFile out = source.outputFile("bucket", "key");
            assertThatThrownBy(out::position).isInstanceOf(IllegalStateException.class);
            out.create();
            assertThat(out.position()).isZero();
            assertThat(requests).isEmpty();
            assertThat(credentialCalls).hasValue(0);
            out.discard();
            out.close();
            assertThat(requests).isEmpty();
        }
    }

    @Test
    void factoriesRejectNullArguments() {
        try (S3Source source = builder().build()) {
            assertThatThrownBy(() -> source.outputFile(null, "key"))
                    .isInstanceOf(NullPointerException.class).hasMessage("bucket must not be null");
            assertThatThrownBy(() -> source.outputFile("bucket", null))
                    .isInstanceOf(NullPointerException.class).hasMessage("key must not be null");
            assertThatThrownBy(() -> source.outputFile((String) null))
                    .isInstanceOf(NullPointerException.class).hasMessage("uri must not be null");
            assertThat(requests).isEmpty();
        }
    }

    @Test
    void factoriesRejectEmptyBucketOrKey() {
        try (S3Source source = builder().build()) {
            assertThatThrownBy(() -> source.outputFile("", "key")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> source.outputFile("bucket", "")).isInstanceOf(IllegalArgumentException.class);
            assertThat(requests).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "https://bucket/key", "s3://", "s3://bucket", "s3://bucket/", "s3:///key" })
    void uriFactoryRejectsMissingBucketOrKey(String uri) {
        try (S3Source source = builder().build()) {
            assertThatThrownBy(() -> source.outputFile(uri)).isInstanceOf(IllegalArgumentException.class);
            assertThat(requests).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void factoryOverloadsPreserveOpaqueObjectKeys(boolean uri) throws Exception {
        try (S3Source source = builder().build()) {
            String key = "folder//../a b+%?#雪";
            OutputFile out = uri ? source.outputFile("s3://bucket/" + key) : source.outputFile("bucket", key);
            out.create();
            out.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
            out.close();
            assertThat(requests).hasSize(1);
            Request request = requests.getFirst();
            assertThat(request.path()).isEqualTo("/bucket/folder//../a%20b%2B%25%3F%23%E9%9B%AA");
            assertThat(request.query()).isNull();
            assertThat(request.body()).containsExactly(1, 2, 3);
            assertThat(request.writeId()).isNotBlank();
            assertThat(request.authorization()).contains("Credential=access/", "x-amz-meta-hardwood-write-id");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 0, 5242879, 2147483640, Integer.MAX_VALUE })
    void builderRejectsInvalidPartSizeBeforeCreatingClient(int bytes) {
        assertThatThrownBy(() -> S3Source.builder().uploadPartSize(bytes))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(requests).isEmpty();
        assertThat(credentialCalls).hasValue(0);
    }

    @Test
    void maximumPartSizeDoesNotAllocateAtFactoryTime() {
        try (S3Source source = builder().uploadPartSize(Integer.MAX_VALUE - 8).build()) {
            assertThatCode(() -> source.outputFile("bucket", "key").discard()).doesNotThrowAnyException();
            assertThat(requests).isEmpty();
        }
    }

    @Test
    void defaultPartSizeBuffersBelowEightMiBAndUploadsAtBoundary() throws Exception {
        try (S3Source source = builder().build()) {
            OutputFile out = source.outputFile("bucket", "key");
            out.create();
            out.write(ByteBuffer.wrap(new byte[MIN_PART_SIZE]));
            assertThat(requests).isEmpty();
            out.write(ByteBuffer.wrap(new byte[3 * 1024 * 1024]));
            assertThat(requests.stream().map(Request::method)).containsExactly("POST", "PUT");
            assertThat(requests.get(1).body()).hasSize(8 * 1024 * 1024);
            out.close();
            assertThat(requests.stream().map(Request::method)).containsExactly("POST", "PUT", "POST");
        }
    }

    @Test
    void configuredPartSizeIsCapturedPerSourceAndLeavesReadSettingsIndependent() throws Exception {
        S3Source.Builder builder = builder().uploadPartSize(MIN_PART_SIZE)
                .rangeBacking(RangeBacking.SPARSE_TEMPFILE).tempDir(tempDir);
        try (S3Source first = builder.build();
             S3Source second = builder.uploadPartSize(8 * 1024 * 1024).build()) {
            OutputFile out = first.outputFile("bucket", "first");
            out.create();
            out.write(ByteBuffer.wrap(new byte[MIN_PART_SIZE]));
            assertThat(requests.stream().map(Request::method)).containsExactly("POST", "PUT");
            assertThat(requests.get(1).body()).hasSize(MIN_PART_SIZE);
            out.discard();
            assertThat(requests.getLast().method()).isEqualTo("DELETE");
            assertThat(requests.getLast().query()).isEqualTo("uploadId=owned");
            out.close();
            requests.clear();
            OutputFile other = second.outputFile("bucket", "second");
            other.create();
            other.write(ByteBuffer.wrap(new byte[MIN_PART_SIZE]));
            assertThat(requests).isEmpty();
            other.discard();
            try (Stream<Path> files = Files.list(tempDir)) {
                assertThat(files).isEmpty();
            }
        }
    }

    @Test
    void writerPublishesReadableParquetAndLeavesSharedClientUsable() throws Exception {
        S3Source source = builder().build();
        try (source) {
            try (ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile("s3://bucket/events"), SCHEMA)) {
                writer.columnWriter().writeBatch(batch -> batch.ints("id", new int[] { 7, 11, 19 }));
                assertThat(requests).isEmpty();
            }
            assertThat(requests).hasSize(1);
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(requests.getFirst().body())));
                 RowReader rows = reader.rowReader()) {
                for (int expected : new int[] { 7, 11, 19 }) {
                    assertThat(rows.hasNext()).isTrue();
                    rows.next();
                    assertThat(rows.getInt("id")).isEqualTo(expected);
                }
                assertThat(rows.hasNext()).isFalse();
            }
            assertThat(credentialCalls).hasValue(1);
        }
        try (S3Source another = builder().build()) {
            OutputFile out = another.outputFile("bucket", "next");
            out.create();
            out.close();
            assertThat(requests).hasSize(2);
            assertThat(requests.getLast().body()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void writerFailureOrCallerAbortDoesNotPublish(boolean abort) throws Exception {
        try (S3Source source = builder().build();
             ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile("bucket", "events"), SCHEMA)) {
            writer.columnWriter().writeBatch(batch -> batch.ints("id", new int[] { 1 }));
            if (abort) {
                writer.abort();
            }
            else {
                assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> {
                    throw new IllegalArgumentException("source failed");
                })).isInstanceOf(IllegalArgumentException.class).hasMessage("source failed");
            }
        }
        assertThat(requests).isEmpty();
    }

    @Test
    void writerPropagatesRejectedPublicationWithoutRetryAndSourceCanBeReused() throws Exception {
        try (S3Source source = builder().build()) {
            ParquetFileWriter writer = ParquetFileWriter.create(source.outputFile("bucket", "rejected"), SCHEMA);
            rejectPublication = true;
            assertThatThrownBy(writer::close).isInstanceOf(IOException.class);
            writer.close();
            assertThat(requests.stream().map(Request::method)).containsExactly("PUT");
            rejectPublication = false;
            try (ParquetFileWriter next = ParquetFileWriter.create(source.outputFile("bucket", "next"), SCHEMA)) {
                next.columnWriter().writeBatch(batch -> batch.ints("id", new int[] { 2 }));
            }
            assertThat(requests).hasSize(2);
        }
    }

    private S3Source.Builder builder() {
        return S3Source.builder()
                .endpoint("http://localhost:" + server.getAddress().getPort())
                .pathStyle(true)
                .credentials(() -> {
                    credentialCalls.incrementAndGet();
                    return S3Credentials.of("access", "secret");
                })
                .requestTimeout(Duration.ofSeconds(5))
                .maxRetries(0)
                .httpClient(client);
    }

    private void respond(HttpExchange exchange) throws IOException {
        requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(),
                exchange.getRequestURI().getRawQuery(), exchange.getRequestBody().readAllBytes(),
                exchange.getRequestHeaders().getFirst("x-amz-meta-hardwood-write-id"),
                exchange.getRequestHeaders().getFirst("Authorization")));
        String query = exchange.getRequestURI().getRawQuery();
        switch (exchange.getRequestMethod()) {
            case "PUT" -> {
                if (rejectPublication) {
                    respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
                }
                else {
                    exchange.getResponseHeaders().set("ETag", "\"etag\"");
                    respond(exchange, 200, "");
                }
            }
            case "POST" -> respond(exchange, 200, query.startsWith("uploads")
                    ? "<InitiateMultipartUploadResult><UploadId>owned</UploadId></InitiateMultipartUploadResult>"
                    : "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>");
            case "DELETE" -> respond(exchange, 204, "");
            default -> respond(exchange, 500, "Unexpected request");
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(bytes);
        }
    }

    private record Request(String method, String path, String query, byte[] body, String writeId, String authorization) {
    }
}
