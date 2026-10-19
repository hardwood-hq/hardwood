/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.internal.S3Api;
import dev.hardwood.s3.internal.S3Xml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [S3OutputFile] against an in-process server that records each request and answers it the way
/// S3 does, unless a test has scripted a failure for it.
class S3OutputFileTest {

    private static final int PART_SIZE = S3OutputFile.MIN_PART_SIZE;
    private static final String UPLOAD_ID = "upload+/=1";

    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final Map<Integer, byte[]> parts = new TreeMap<>();
    /// Requests, as recorded in [#requests], to fail once each with the mapped status.
    private final Map<String, Integer> failOnce = new TreeMap<>();
    private final AtomicInteger completionFailures = new AtomicInteger();
    private volatile byte[] published;
    private volatile boolean aborted;
    /// Counted down when part 2 arrives; part 1's upload waits for it, when set.
    private volatile CountDownLatch partTwoArrived;
    private volatile boolean partOneSawPartTwo;
    /// When set, part 2's upload waits for it before answering.
    private volatile CountDownLatch partTwoRelease;
    private volatile boolean partTwoDone;
    private volatile boolean abortSawPartTwoDone;
    private ExecutorService handlers;
    private HttpServer server;
    private HttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        handlers = Executors.newCachedThreadPool();
        server.setExecutor(handlers);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.stop(0);
        handlers.shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, PART_SIZE - 1 })
    void sendsAnObjectSmallerThanOnePartInOnePut(int length) throws IOException {
        byte[] bytes = payload(length);

        write(output(), bytes);

        assertThat(published).isEqualTo(bytes);
        assertThat(requests).containsExactly("PUT /bucket/dir/file.parquet");
    }

    @ParameterizedTest
    @ValueSource(ints = { PART_SIZE, PART_SIZE + 1, 2 * PART_SIZE, 2 * PART_SIZE + 1 })
    void sendsALargerObjectAsAMultipartUpload(int length) throws IOException {
        byte[] bytes = payload(length);

        write(output(), bytes);

        int partCount = (length + PART_SIZE - 1) / PART_SIZE;
        assertThat(published).isEqualTo(bytes);
        assertThat(parts).hasSize(partCount);
        assertThat(requests.getFirst()).isEqualTo("POST /bucket/dir/file.parquet?uploads=");
        assertThat(requests.getLast()).isEqualTo("POST /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1");
        assertThat(requests).hasSize(partCount + 2);
    }

    @Test
    void sendsNothingBeforeTheFirstPartFills() throws IOException {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(PART_SIZE - 1)));

        assertThat(requests).isEmpty();
        assertThat(out.position()).isEqualTo(PART_SIZE - 1);
    }

    @Test
    void collectsManySmallWritesIntoParts() throws IOException {
        byte[] bytes = payload(2 * PART_SIZE + 17);
        S3OutputFile out = output();
        out.create();
        for (int offset = 0; offset < bytes.length; offset += 1000) {
            out.write(ByteBuffer.wrap(bytes, offset, Math.min(1000, bytes.length - offset)));
        }
        out.close();

        assertThat(published).isEqualTo(bytes);
        assertThat(parts).hasSize(3);
    }

    @Test
    void retriesAPartThatFailsTransiently() throws IOException {
        failOnce.put("PUT /bucket/dir/file.parquet?partNumber=1&uploadId=upload%2B%2F%3D1", 503);
        byte[] bytes = payload(PART_SIZE + 1);

        write(output(), bytes);

        assertThat(published).isEqualTo(bytes);
        assertThat(requests).filteredOn(r -> r.contains("partNumber=1")).hasSize(2);
    }

    @Test
    void uploadsTheNextPartWhileOneIsInFlight() throws IOException {
        partTwoArrived = new CountDownLatch(1);
        byte[] bytes = payload(2 * PART_SIZE + 1);

        write(output(), bytes);

        assertThat(partOneSawPartTwo).isTrue();
        assertThat(published).isEqualTo(bytes);
    }

    @Test
    void aFailedPartStopsTheWriterOnceTheUploadSlotsAreTaken() throws IOException {
        failOnce.put("PUT /bucket/dir/file.parquet?partNumber=1&uploadId=upload%2B%2F%3D1", 403);
        S3OutputFile out = output();
        out.create();

        assertThatThrownBy(() -> out.write(ByteBuffer.wrap(payload((S3OutputFile.MAX_UPLOADS_IN_FLIGHT + 1) * PART_SIZE))))
                .isInstanceOf(IOException.class)
                .hasMessage("Upload part 1 of s3://bucket/dir/file.parquet failed: HTTP 403 AccessDenied: Access Denied");
        out.discard();

        assertThat(aborted).isTrue();
        assertThat(published).isNull();
        assertThat(requests.getLast()).isEqualTo("DELETE /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1");
    }

    @Test
    void aFailedLastPartFailsCloseAndAbortsAfterTheOtherParts() throws IOException {
        failOnce.put("PUT /bucket/dir/file.parquet?partNumber=3&uploadId=upload%2B%2F%3D1", 403);
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(2 * PART_SIZE + 1)));

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Upload part 3 of s3://bucket/dir/file.parquet failed: HTTP 403 AccessDenied: Access Denied");
        assertThat(aborted).isTrue();
        assertThat(requests.getLast()).isEqualTo("DELETE /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1");
    }

    @Test
    void discardAbortsTheUpload() throws IOException {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(PART_SIZE)));

        out.discard();
        out.close();

        assertThat(aborted).isTrue();
        assertThat(published).isNull();
        assertThat(requests.getLast()).isEqualTo("DELETE /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1");
    }

    @Test
    void discardBeforeAnyPartSendsNothing() throws IOException {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(10)));

        out.discard();
        out.close();

        assertThat(requests).isEmpty();
    }

    @Test
    void failedCompletionAbortsTheUpload() throws IOException {
        failOnce.put("POST /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1", 403);
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(PART_SIZE + 1)));

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Complete multipart upload s3://bucket/dir/file.parquet failed: HTTP 403 AccessDenied: Access Denied");
        assertThat(aborted).isTrue();
        assertThat(published).isNull();
    }

    @Test
    void retriesACompletionReportingAnInternalErrorInA200Response() throws IOException {
        completionFailures.set(1);
        byte[] bytes = payload(PART_SIZE + 1);

        write(output(), bytes);

        assertThat(published).isEqualTo(bytes);
        assertThat(requests).filteredOn(r -> r.startsWith("POST") && r.contains("uploadId=")).hasSize(2);
    }

    @Test
    void reusesSpareBuffersAcrossManyParts() throws IOException {
        byte[] bytes = payload((S3OutputFile.MAX_UPLOADS_IN_FLIGHT + 3) * PART_SIZE + 1);

        write(output(), bytes);

        assertThat(parts).hasSize(S3OutputFile.MAX_UPLOADS_IN_FLIGHT + 4);
        assertThat(published).isEqualTo(bytes);
    }

    @Test
    void discardWaitsForThePartsInFlightBeforeAborting() throws Exception {
        partTwoRelease = new CountDownLatch(1);
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(2 * PART_SIZE)));

        Thread discarding = Thread.ofVirtual().start(() -> {
            try {
                out.discard();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(discarding.join(Duration.ofMillis(300))).isFalse();
        partTwoRelease.countDown();
        assertThat(discarding.join(Duration.ofSeconds(10))).isTrue();

        assertThat(aborted).isTrue();
        assertThat(abortSawPartTwoDone).isTrue();
    }

    @Test
    void anInterruptedWriterStillAbortsTheUpload() throws IOException {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(PART_SIZE)));

        Thread.currentThread().interrupt();
        try {
            out.discard();
        }
        finally {
            assertThat(Thread.interrupted()).isTrue();
        }

        assertThat(aborted).isTrue();
    }

    @Test
    void completionReportingAnErrorInA200ResponseFailsOnceItsRetriesAreSpent() throws IOException {
        completionFailures.set(4);
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(PART_SIZE)));

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Complete multipart upload s3://bucket/dir/file.parquet failed: InternalError: We encountered an internal error.");
        assertThat(aborted).isTrue();
    }

    @Test
    void failedPutLeavesNothingToAbort() throws IOException {
        failOnce.put("PUT /bucket/dir/file.parquet", 403);
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(10)));

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("PUT s3://bucket/dir/file.parquet failed: HTTP 403 AccessDenied: Access Denied");
        assertThat(requests).containsExactly("PUT /bucket/dir/file.parquet");
    }

    @Test
    void abortFailureIsSuppressedOnTheCloseFailure() throws IOException {
        failOnce.put("POST /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1", 403);
        failOnce.put("DELETE /bucket/dir/file.parquet?uploadId=upload%2B%2F%3D1", 403);
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(PART_SIZE)));

        assertThatThrownBy(out::close)
                .hasMessage("Complete multipart upload s3://bucket/dir/file.parquet failed: HTTP 403 AccessDenied: Access Denied")
                .satisfies(e -> assertThat(e.getSuppressed()).singleElement()
                        .extracting(Throwable::getMessage)
                        .isEqualTo("Abort multipart upload s3://bucket/dir/file.parquet failed: HTTP 403 AccessDenied: Access Denied"));
    }

    @Test
    void rejectsAWriteBeyondTheLastPart() throws IOException {
        long maximum = (long) PART_SIZE * S3Xml.MAX_PARTS;
        S3OutputFile.requireCapacity("s3://bucket/key", maximum - 10, 10, PART_SIZE);

        assertThatThrownBy(() -> S3OutputFile.requireCapacity("s3://bucket/key", maximum - 10, 11, PART_SIZE))
                .isInstanceOf(IOException.class)
                .hasMessage("Output for s3://bucket/key cannot exceed 52428800000 bytes (10000 parts of 5242880 bytes)");
    }

    @Test
    void rejectsUseOutsideItsLifecycle() throws IOException {
        S3OutputFile out = output();
        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for s3://bucket/dir/file.parquet is not open");
        out.create();
        assertThatThrownBy(out::create)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Output for s3://bucket/dir/file.parquet was already created");
        out.close();
        out.close();
        assertThat(requests).containsExactly("PUT /bucket/dir/file.parquet");
    }

    @Test
    void rejectsAPartSizeBelowTheMinimum() {
        assertThatThrownBy(() -> S3OutputFile.validatePartSize(PART_SIZE - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Upload part size must be between 5242880 and 2147483639 bytes: 5242879");
    }

    private S3OutputFile output() {
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        S3Api api = new S3Api(client, () -> S3Credentials.of("access", "secret"), "us-east-1", endpoint, true,
                Duration.ofSeconds(10), 3);
        return new S3OutputFile(api, "bucket", "dir/file.parquet", PART_SIZE);
    }

    private static void write(S3OutputFile out, byte[] bytes) throws IOException {
        out.create();
        out.write(ByteBuffer.wrap(bytes));
        assertThat(out.position()).isEqualTo(bytes.length);
        out.close();
    }

    private void handle(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getRawQuery();
        String request = method + " " + exchange.getRequestURI().getRawPath() + (query != null ? "?" + query : "");
        requests.add(request);
        Integer failure;
        synchronized (failOnce) {
            failure = failOnce.remove(request);
        }
        if (failure != null) {
            respond(exchange, failure, failure == 403
                    ? "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
                    : "<Error><Code>ServiceUnavailable</Code><Message>Reduce your request rate.</Message></Error>");
            return;
        }
        if (method.equals("POST") && "uploads=".equals(query)) {
            respond(exchange, 200, "<InitiateMultipartUploadResult><UploadId>" + UPLOAD_ID + "</UploadId></InitiateMultipartUploadResult>");
        }
        else if (method.equals("PUT") && query != null) {
            int partNumber = Integer.parseInt(query.substring("partNumber=".length(), query.indexOf('&')));
            CountDownLatch latch = partTwoArrived;
            if (latch != null && partNumber == 2) {
                latch.countDown();
            }
            else if (latch != null && partNumber == 1) {
                try {
                    partOneSawPartTwo = latch.await(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            CountDownLatch release = partTwoRelease;
            if (release != null && partNumber == 2) {
                try {
                    release.await(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            synchronized (parts) {
                parts.put(partNumber, body);
            }
            exchange.getResponseHeaders().add("ETag", "\"etag-" + partNumber + "\"");
            respond(exchange, 200, "");
            if (partNumber == 2) {
                partTwoDone = true;
            }
        }
        else if (method.equals("PUT")) {
            published = body;
            respond(exchange, 200, "");
        }
        else if (method.equals("POST")) {
            if (completionFailures.getAndDecrement() > 0) {
                respond(exchange, 200, "<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>");
                return;
            }
            String xml = new String(body, StandardCharsets.UTF_8);
            ByteArrayOutputStream object = new ByteArrayOutputStream();
            synchronized (parts) {
                for (Map.Entry<Integer, byte[]> part : parts.entrySet()) {
                    assertThat(xml).contains("<PartNumber>" + part.getKey() + "</PartNumber><ETag>&quot;etag-" + part.getKey() + "&quot;</ETag>");
                    object.writeBytes(part.getValue());
                }
            }
            published = object.toByteArray();
            respond(exchange, 200, "<CompleteMultipartUploadResult><ETag>\"done\"</ETag></CompleteMultipartUploadResult>");
        }
        else if (method.equals("DELETE")) {
            abortSawPartTwoDone = partTwoDone;
            aborted = true;
            respond(exchange, 204, "");
        }
        else {
            respond(exchange, 400, "");
        }
    }

    private static void respond(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static byte[] payload(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i * 31 + (i >>> 8));
        }
        return bytes;
    }
}
