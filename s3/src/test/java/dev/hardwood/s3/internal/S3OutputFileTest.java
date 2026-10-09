/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Timeout(30)
class S3OutputFileTest {

    private static final int PART_SIZE = 5 * 1024 * 1024;
    private static final String UPLOAD_ID = "owned+/=upload";
    private static final S3Credentials CREDENTIALS = S3Credentials.of("access", "secret");

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Map<Integer, byte[]> parts = new ConcurrentHashMap<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private HttpServer server;
    private ExecutorService executor;
    private HttpClient client;
    private volatile HttpHandler responder;
    private volatile byte[] published;

    @BeforeEach
    void setUp() throws IOException {
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestURI().getRawQuery(), body,
                    exchange.getRequestHeaders().getFirst(S3Api.WRITE_ID_HEADER)));
            responder.handle(exchange);
        });
        server.setExecutor(executor);
        server.start();
        client = HttpClient.newHttpClient();
        responder = this::respondNormally;
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        client.shutdownNow();
        server.stop(0);
        executor.shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, PART_SIZE - 1, PART_SIZE, PART_SIZE + 1, 2 * PART_SIZE, 2 * PART_SIZE + 1 })
    void publishesExactBytesAtPartBoundaries(int length) throws Exception {
        byte[] bytes = payload(length);
        S3OutputFile out = output();
        out.create();
        assertThat(requests).isEmpty();
        ByteBuffer input = ByteBuffer.wrap(bytes);
        out.write(input);
        assertThat(input.position()).isEqualTo(input.limit());
        assertThat(out.position()).isEqualTo(length);
        assertThat(published).isNull();
        out.close();

        assertThat(published).isEqualTo(bytes);
        int expectedParts = length < PART_SIZE ? 0 : (length + PART_SIZE - 1) / PART_SIZE;
        assertThat(parts).hasSize(expectedParts);
        assertThat(count("PUT", false)).isEqualTo(expectedParts == 0 ? 1 : 0);
        assertThat(count("POST", true)).isEqualTo(expectedParts == 0 ? 0 : 2);
        for (int number = 1; number <= expectedParts; number++) {
            assertThat(parts.get(number)).hasSize(Math.min(PART_SIZE, length - (number - 1) * PART_SIZE));
        }
        if (expectedParts != 0) {
            Request completion = requests.getLast();
            String xml = new String(completion.body(), StandardCharsets.UTF_8);
            for (int number = 1; number <= expectedParts; number++) {
                assertThat(xml).contains("<PartNumber>" + number + "</PartNumber><ETag>\"part-" + number + "\"</ETag>");
            }
        }
        int operations = requests.size();
        out.close();
        out.discard();
        assertThat(requests).hasSize(operations);
        assertTerminal(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "heap", "slice", "direct", "readonly" })
    void consumesRemainingBytesWithoutChangingLimit(String kind) throws Exception {
        byte[] bytes = payload(PART_SIZE + 17);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if ("slice".equals(kind)) {
            byte[] padded = new byte[bytes.length + 9];
            System.arraycopy(bytes, 0, padded, 5, bytes.length);
            buffer = ByteBuffer.wrap(padded, 3, bytes.length + 4).slice();
            buffer.position(2).limit(bytes.length + 2);
        }
        else if ("direct".equals(kind)) {
            buffer = ByteBuffer.allocateDirect(bytes.length + 2);
            buffer.position(1).put(bytes).flip().position(1);
        }
        else if ("readonly".equals(kind)) {
            buffer = buffer.asReadOnlyBuffer();
        }
        int limit = buffer.limit();
        S3OutputFile out = output();
        out.create();
        out.write(buffer);
        assertThat(buffer.position()).isEqualTo(limit);
        assertThat(buffer.limit()).isEqualTo(limit);
        assertThat(out.position()).isEqualTo(bytes.length);
        out.close();
        assertThat(published).isEqualTo(bytes);
    }

    @Test
    void coalescesTinyWritesAndDoesNotRetainCallerBuffer() throws Exception {
        S3OutputFile out = output();
        out.create();
        byte[] caller = { 3 };
        for (int i = 0; i < 2048; i++) {
            out.write(ByteBuffer.wrap(caller));
        }
        caller[0] = 99;
        out.write(ByteBuffer.allocate(0));
        assertThat(requests).isEmpty();
        assertThat(out.position()).isEqualTo(2048);
        out.close();
        byte[] expected = new byte[2048];
        Arrays.fill(expected, (byte) 3);
        assertThat(published).isEqualTo(expected);
    }

    @Test
    void randomizedWritePartitionsPreserveOrderAcrossParts() throws Exception {
        byte[] bytes = payload(3 * PART_SIZE + 13);
        Random random = new Random(1454);
        S3OutputFile out = output();
        out.create();
        int offset = 0;
        while (offset < bytes.length) {
            int length = Math.min(1 + random.nextInt(64 * 1024), bytes.length - offset);
            out.write(ByteBuffer.wrap(bytes, offset, length));
            offset += length;
            assertThat(out.position()).isEqualTo(offset);
        }
        out.close();
        assertThat(published).isEqualTo(bytes);
    }

    @Test
    void lifecycleRejectsMisuseAndAllowsCloseBeforeCreate() throws Exception {
        S3OutputFile out = output();
        assertThatThrownBy(out::position).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(0))).isInstanceOf(IllegalStateException.class);
        out.close();
        out.create();
        assertThatThrownBy(out::create).isInstanceOf(IllegalStateException.class);
        assertThat(out.position()).isZero();
        out.discard();
        out.close();
        out.discard();
        assertTerminal(out);
        assertThat(requests).isEmpty();
    }

    @Test
    void discardBeforeCreatePermanentlyPreventsPublication() throws Exception {
        S3OutputFile out = output();
        out.discard();
        out.close();
        assertTerminal(out);
        assertThat(requests).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, PART_SIZE, PART_SIZE + 1 })
    void discardAbortsOnlyKnownUploadAndCloseDoesNotPublish(int length) throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(payload(length)));
        out.discard();
        int operations = requests.size();
        out.close();
        out.discard();
        assertThat(requests).hasSize(operations);
        assertThat(count("DELETE", true)).isEqualTo(length < PART_SIZE ? 0 : 1);
        assertThat(published).isNull();
        assertThat(requests.stream().filter(r -> "DELETE".equals(r.method())).map(Request::query))
                .allMatch(query -> query.equals("uploadId=owned%2B%2F%3Dupload"));
        assertTerminal(out);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, PART_SIZE - 1, Integer.MAX_VALUE })
    void rejectsInvalidPartSizeBeforeCreatingResources(int size) {
        assertThatThrownBy(() -> new S3OutputFile(api(0, () -> CREDENTIALS), "bucket", "key", size))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(requests).isEmpty();
    }

    @Test
    void validatesPartSizeAndCapacityUsingLongArithmetic() {
        long maximum = (long) PART_SIZE * 10_000;
        assertThatCode(() -> S3OutputFile.requireCapacity(maximum - 1, 1, PART_SIZE)).doesNotThrowAnyException();
        assertThatCode(() -> S3OutputFile.requireCapacity(maximum, 0, PART_SIZE)).doesNotThrowAnyException();
        assertThatCode(() -> S3OutputFile.requireCapacity(3_000_000_000L, Integer.MAX_VALUE, PART_SIZE)).doesNotThrowAnyException();
        assertThatCode(() -> S3OutputFile.validatePartSize(Integer.MAX_VALUE - 8)).doesNotThrowAnyException();
        assertThatThrownBy(() -> S3OutputFile.requireCapacity(maximum, 1, PART_SIZE)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> S3OutputFile.requireCapacity(maximum - 1, 2, PART_SIZE)).isInstanceOf(IOException.class);
        long largestMaximum = (long) (Integer.MAX_VALUE - 8) * 10_000;
        assertThatCode(() -> S3OutputFile.requireCapacity(largestMaximum - 1, 1, Integer.MAX_VALUE - 8)).doesNotThrowAnyException();
        assertThatThrownBy(() -> S3OutputFile.requireCapacity(largestMaximum, 1, Integer.MAX_VALUE - 8)).isInstanceOf(IOException.class);
    }

    @Test
    void capacityFailureDoesNotConsumeInputAndAbortsUpload() throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[PART_SIZE]));
        // Reach the logical limit without allocating or uploading a 50 GiB fixture.
        Field position = S3OutputFile.class.getDeclaredField("position");
        position.setAccessible(true);
        position.setLong(out, (long) PART_SIZE * 10_000 - 2);
        ByteBuffer extra = ByteBuffer.wrap(new byte[] { 99, 1, 2, 3 });
        extra.position(1);
        assertThatThrownBy(() -> out.write(extra)).isInstanceOf(IOException.class).hasMessageContaining("s3://bucket/key");
        assertThat(extra.position()).isEqualTo(1);
        assertThat(count("DELETE", true)).isEqualTo(1);
        out.close();
        assertThat(published).isNull();
        assertTerminal(out);
    }

    @Test
    void partFailureIsLatchedAndCleanupFailuresAreSuppressed() throws Exception {
        responder = exchange -> {
            String method = exchange.getRequestMethod();
            if ("PUT".equals(method)) {
                respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
            }
            else if ("DELETE".equals(method) || "GET".equals(method)) {
                respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
            }
            else {
                respondNormally(exchange);
            }
        };
        S3OutputFile out = output();
        out.create();
        Throwable failure = catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])));
        assertThat(failure).isInstanceOf(IOException.class).hasMessageContaining("AccessDenied");
        assertThat(failure.getSuppressed()).isNotEmpty();
        assertTerminal(out);
        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
        assertThat(count("POST", true)).isEqualTo(1);
        assertThat(published).isNull();
    }

    @Test
    void retriesDiscardAfterAbortFailureWithoutRetainingPayload() throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[PART_SIZE]));
        responder = exchange -> respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
        assertThatThrownBy(out::discard).isInstanceOf(IOException.class);
        assertTerminal(out);
        responder = this::respondNormally;
        out.discard();
        int operations = requests.size();
        out.close();
        out.discard();
        assertThat(requests).hasSize(operations);
        assertThat(count("DELETE", true)).isEqualTo(2);
        assertThat(published).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "lost", "invalid" })
    void uncertainInitiationIsNotReplayedOrCleanedByListingOtherUploads(String failure) throws Exception {
        responder = exchange -> {
            if ("lost".equals(failure)) {
                exchange.sendResponseHeaders(200, 100);
                exchange.getResponseBody().write(1);
                exchange.close();
            }
            else {
                respond(exchange, 200, "<InitiateMultipartUploadResult/>");
            }
        };
        S3OutputFile out = output();
        out.create();
        Throwable problem = catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])));
        assertThat(problem).isInstanceOf(IOException.class);
        assertThat(Arrays.stream(problem.getSuppressed()).map(Throwable::getMessage))
                .anyMatch(message -> message.contains("upload") && message.contains("may remain"));
        out.close();
        out.discard();
        assertThat(requests).hasSize(1);
        assertTerminal(out);
    }

    @Test
    void failedFinalPartNeverCompletesUpload() throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[PART_SIZE + 1]));
        responder = exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
            }
            else {
                respondNormally(exchange);
            }
        };
        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
        int operations = requests.size();
        out.close();
        out.discard();
        assertThat(requests).hasSize(operations);
        assertThat(count("POST", true)).isEqualTo(1);
        assertThat(published).isNull();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, PART_SIZE })
    void publicationErrorDoesNotReplayOrDeleteDestination(int length) throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[length]));
        responder = exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "<Error><Code>InvalidPart</Code></Error>");
            }
            else {
                respondNormally(exchange);
            }
        };
        if (length < PART_SIZE) {
            responder = exchange -> respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
        }
        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
        int operations = requests.size();
        out.close();
        assertThat(requests).hasSize(operations);
        assertThat(requests.stream().filter(r -> "DELETE".equals(r.method())).map(Request::query)).doesNotContainNull();
        assertThat(published).isNull();
        assertTerminal(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "empty", "parts" })
    void uncertainPartCleanupRequiresMissingUploadAndSharesRetryBudget(String listing) throws Exception {
        AtomicInteger aborts = new AtomicInteger();
        responder = exchange -> {
            switch (exchange.getRequestMethod()) {
                case "PUT" -> {
                    exchange.sendResponseHeaders(200, 100);
                    exchange.getResponseBody().write(1);
                    exchange.close();
                }
                case "DELETE" -> {
                    aborts.incrementAndGet();
                    respond(exchange, 204, "");
                }
                case "GET" -> {
                    if (aborts.get() < 2) {
                        String part = "parts".equals(listing) ? "<Part><PartNumber>1</PartNumber></Part>" : "";
                        respond(exchange, 200, "<ListPartsResult><IsTruncated>false</IsTruncated>" + part + "</ListPartsResult>");
                    }
                    else {
                        respond(exchange, 404, "<Error><Code>NoSuchUpload</Code></Error>");
                    }
                }
                default -> respondNormally(exchange);
            }
        };
        S3OutputFile out = new S3OutputFile(api(1, () -> CREDENTIALS), "bucket", "key", PART_SIZE);
        out.create();
        Throwable failure = catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])));
        assertThat(failure).isInstanceOf(IOException.class);
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(count("PUT", true)).isEqualTo(2);
        assertThat(count("DELETE", true)).isEqualTo(2);
        assertThat(count("GET", true)).isEqualTo(2);
        int operations = requests.size();
        out.close();
        assertThat(requests).hasSize(operations);
    }

    @Test
    void exhaustedUncertainCleanupReportsFailureAndAllowsExplicitRetry() throws Exception {
        responder = exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                exchange.close();
            }
            else if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>");
            }
            else {
                respondNormally(exchange);
            }
        };
        S3OutputFile out = new S3OutputFile(api(1, () -> CREDENTIALS), "bucket", "key", PART_SIZE);
        out.create();
        Throwable failure = catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])));
        assertThat(failure).isInstanceOf(IOException.class);
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(count("DELETE", true)).isEqualTo(2);
        assertThat(count("GET", true)).isEqualTo(2);
        responder = this::respondNormally;
        out.discard();
        out.close();
        assertThat(count("DELETE", true)).isEqualTo(3);
        assertThat(count("GET", true)).isEqualTo(3);
        assertThat(published).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "runtime", "error" })
    void preservesCredentialFailureAndStillCleansKnownUpload(String kind) throws Exception {
        Throwable failure = "runtime".equals(kind) ? new IllegalStateException("credentials failed") : new AssertionError("credentials failed");
        AtomicInteger resolutions = new AtomicInteger();
        S3CredentialsProvider credentials = () -> {
            if (resolutions.incrementAndGet() == 2) {
                if (failure instanceof Error error) {
                    throw error;
                }
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new AssertionError("Unexpected failure type", failure);
            }
            return CREDENTIALS;
        };
        S3OutputFile out = new S3OutputFile(api(0, credentials), "bucket", "key", PART_SIZE);
        out.create();
        assertThat(catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])))).isSameAs(failure);
        assertThat(count("DELETE", true)).isEqualTo(1);
        assertThat(published).isNull();
        out.close();
        assertTerminal(out);
    }

    @Test
    void interruptionAttemptsCleanupAndRestoresFlagWithoutPublication() throws Exception {
        CountDownLatch uploading = new CountDownLatch(1);
        responder = exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                uploading.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exchange.close();
            }
            else {
                respondNormally(exchange);
            }
        };
        S3OutputFile out = output();
        out.create();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread writer = new Thread(() -> {
            failure.set(catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE]))));
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        writer.start();
        try {
            assertThat(uploading.await(5, TimeUnit.SECONDS)).isTrue();
            writer.interrupt();
            writer.join(5000);
            assertThat(writer.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(IOException.class);
            assertThat(interrupted.get()).isTrue();
            assertThat(count("DELETE", true)).isEqualTo(1);
            assertThat(count("GET", true)).isEqualTo(1);
            assertThat(published).isNull();
            out.close();
        }
        finally {
            release.countDown();
            writer.interrupt();
            writer.join(5000);
        }
    }

    @Test
    void outputsHaveIndependentIdentityAndDoNotCloseSharedClient() throws Exception {
        S3Api api = api(0, () -> CREDENTIALS);
        S3OutputFile first = new S3OutputFile(api, "bucket", "key", PART_SIZE);
        S3OutputFile second = new S3OutputFile(api, "bucket", "other", PART_SIZE);
        first.create();
        first.write(ByteBuffer.wrap(new byte[] { 1 }));
        first.close();
        second.create();
        second.write(ByteBuffer.wrap(new byte[] { 2 }));
        second.close();
        assertThat(requests).hasSize(2);
        UUID firstId = UUID.fromString(requests.getFirst().writeId());
        UUID secondId = UUID.fromString(requests.getLast().writeId());
        assertThat(firstId).isNotEqualTo(secondId);
        assertThat(client.isTerminated()).isFalse();
    }

    @Test
    void failedPublicationCleanupCanBeRetriedOnlyByExplicitDiscard() throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[PART_SIZE]));
        responder = exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "<Error><Code>InvalidPart</Code></Error>");
            }
            else {
                respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
            }
        };
        Throwable failure = catchThrowable(out::close);
        assertThat(failure).isInstanceOf(S3Xml.ServiceException.class).hasMessageContaining("InvalidPart");
        assertThat(failure.getSuppressed()).hasSize(1);
        int operations = requests.size();
        out.close();
        assertThat(requests).hasSize(operations);
        responder = this::respondNormally;
        out.discard();
        out.close();
        assertThat(count("DELETE", true)).isEqualTo(2);
        assertThat(published).isNull();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, PART_SIZE })
    void publicationCredentialFailureIsPreservedAndPreventsAnotherCloseAttempt(int length) throws Exception {
        AssertionError failure = new AssertionError("publication credentials failed");
        AtomicInteger resolutions = new AtomicInteger();
        int failingResolution = length < PART_SIZE ? 1 : 3;
        S3Api api = api(0, () -> {
            if (resolutions.incrementAndGet() == failingResolution) {
                throw failure;
            }
            return CREDENTIALS;
        });
        S3OutputFile out = new S3OutputFile(api, "bucket", "key", PART_SIZE);
        out.create();
        out.write(ByteBuffer.wrap(new byte[length]));
        assertThat(catchThrowable(out::close)).isSameAs(failure);
        int operations = requests.size();
        out.close();
        assertThat(requests).hasSize(operations);
        assertThat(count("DELETE", true)).isEqualTo(length < PART_SIZE ? 0 : 1);
        assertThat(published).isNull();
        assertTerminal(out);
    }

    @Test
    void discardTemporarilyClearsAnExistingInterruptAndRestoresIt() throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[PART_SIZE]));
        try {
            Thread.currentThread().interrupt();
            out.discard();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(count("DELETE", true)).isEqualTo(1);
            assertThat(published).isNull();
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void timedOutPartIsCancelledAndCleanedWithoutPublishing() throws Exception {
        responder = exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                try {
                    release.await(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exchange.close();
            }
            else {
                respondNormally(exchange);
            }
        };
        S3Api api = new S3Api(client, () -> CREDENTIALS, "us-east-1",
                URI.create("http://localhost:" + server.getAddress().getPort()), true, Duration.ofMillis(500), 0);
        S3OutputFile out = new S3OutputFile(api, "bucket", "key", PART_SIZE);
        out.create();
        assertThatThrownBy(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE]))).isInstanceOf(IOException.class);
        assertThat(count("DELETE", true)).isEqualTo(1);
        assertThat(count("GET", true)).isEqualTo(1);
        out.close();
        assertThat(published).isNull();
        assertTerminal(out);
    }

    @Test
    void nullWriteFailsOutputAndReleasesPayloadInsteadOfPublishingPrefix() throws Exception {
        S3OutputFile out = output();
        out.create();
        out.write(ByteBuffer.wrap(new byte[] { 1 }));
        assertThatThrownBy(() -> out.write(null)).isInstanceOf(NullPointerException.class);
        assertTerminal(out);
        out.close();
        assertThat(requests).isEmpty();
        Field buffer = S3OutputFile.class.getDeclaredField("buffer");
        buffer.setAccessible(true);
        assertThat(buffer.get(out)).isNull();
    }

    @Test
    void invalidCleanupResponseIsReportedAndRetainsUploadForExplicitRetry() throws Exception {
        responder = exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "");
            }
            else if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 404, "<Code>NoSuchUpload</Code>");
            }
            else {
                respondNormally(exchange);
            }
        };
        S3OutputFile out = output();
        out.create();
        Throwable failure = catchThrowable(() -> out.write(ByteBuffer.wrap(new byte[PART_SIZE])));
        assertThat(failure).isInstanceOf(IOException.class).hasMessageContaining("ETag");
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(count("DELETE", true)).isEqualTo(1);
        assertThat(count("GET", true)).isEqualTo(1);
        responder = this::respondNormally;
        out.discard();
        out.close();
        assertThat(count("DELETE", true)).isEqualTo(2);
        assertThat(published).isNull();
    }

    private void assertTerminal(S3OutputFile out) {
        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(0))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(out::position).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(out::create).isInstanceOf(IllegalStateException.class);
    }

    private S3OutputFile output() {
        return new S3OutputFile(api(0, () -> CREDENTIALS), "bucket", "key", PART_SIZE);
    }

    private S3Api api(int retries, S3CredentialsProvider credentials) {
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        return new S3Api(client, credentials, "us-east-1", endpoint, true, Duration.ofSeconds(5), retries);
    }

    private long count(String method, boolean query) {
        return requests.stream().filter(r -> method.equals(r.method()) && (r.query() != null) == query).count();
    }

    private void respondNormally(HttpExchange exchange) throws IOException {
        Request request = requests.getLast();
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getRawQuery();
        if ("POST".equals(method) && "uploads=".equals(query)) {
            respond(exchange, 200, "<InitiateMultipartUploadResult><UploadId>" + UPLOAD_ID + "</UploadId></InitiateMultipartUploadResult>");
        }
        else if ("PUT".equals(method) && query != null) {
            int number = Integer.parseInt(query.substring("partNumber=".length(), query.indexOf('&')));
            parts.put(number, request.body());
            exchange.getResponseHeaders().add("ETag", "\"part-" + number + "\"");
            respond(exchange, 200, "");
        }
        else if ("POST".equals(method)) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            for (int number = 1; number <= parts.size(); number++) {
                bytes.writeBytes(parts.get(number));
            }
            published = bytes.toByteArray();
            respond(exchange, 200, "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>");
        }
        else if ("PUT".equals(method)) {
            published = request.body();
            respond(exchange, 200, "");
        }
        else if ("DELETE".equals(method)) {
            parts.clear();
            respond(exchange, 204, "");
        }
        else if ("GET".equals(method)) {
            respond(exchange, 404, "<Error><Code>NoSuchUpload</Code></Error>");
        }
        else {
            throw new IOException("Unexpected test request: " + method);
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
            bytes[i] = (byte) (i * 31);
        }
        return bytes;
    }

    private record Request(String method, String query, byte[] body, String writeId) {
    }
}
