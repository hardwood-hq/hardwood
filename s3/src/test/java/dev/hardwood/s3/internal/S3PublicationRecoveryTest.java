/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Timeout(30)
class S3PublicationRecoveryTest {

    private static final int PART_SIZE = 5 * 1024 * 1024;
    private static final String UPLOAD_ID = "owned-upload";
    private static final String KEY = "folder/snow 雪+%.parquet";
    private static final S3Credentials CREDENTIALS = S3Credentials.of("access", "secret");

    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Map<Integer, Integer> partLengths = new ConcurrentHashMap<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private HttpServer server;
    private ExecutorService executor;
    private HttpClient client;
    private volatile Responder responder;
    private volatile String uploadWriteId;
    private volatile String publishedWriteId;
    private volatile long publishedLength;

    @BeforeEach
    void setUp() throws IOException {
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            int length = exchange.getRequestBody().readAllBytes().length;
            Request request = new Request(exchange.getRequestMethod(), exchange.getRequestURI(), length,
                    exchange.getRequestHeaders().getFirst(S3Api.WRITE_ID_HEADER), exchange.getRequestHeaders().getFirst("Authorization"));
            requests.add(request);
            responder.respond(exchange, request);
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
    @ValueSource(ints = { 0, 1, PART_SIZE, PART_SIZE + 1 })
    void matchingMetadataRecoversLostPublicationResponseWithoutAbort(int length) throws Exception {
        responder = this::publishAndLoseResponse;
        S3OutputFile out = output(length, 2);
        assertThatCode(out::close).doesNotThrowAnyException();

        assertThat(publishedLength).isEqualTo(length);
        assertThat(UUID.fromString(publishedWriteId)).isNotNull();
        assertThat(count("HEAD")).isEqualTo(1);
        assertThat(count("DELETE")).isZero();
        assertPublicationCount(length, 1);
        Request head = requests.stream().filter(r -> "HEAD".equals(r.method())).findFirst().orElseThrow();
        assertThat(head.uri().getRawPath()).isEqualTo("/bucket/folder/snow%20%E9%9B%AA%2B%25.parquet");
        assertThat(head.uri().getRawQuery()).isNull();
        assertThat(head.length()).isZero();
        assertThat(head.authorization()).contains("SignedHeaders=");
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, PART_SIZE + 1 })
    void acknowledgedPublicationDoesNotReadMetadata(int length) throws Exception {
        S3OutputFile out = output(length, 3);
        out.close();
        assertThat(count("HEAD")).isZero();
        assertThat(count("DELETE")).isZero();
        assertPublicationCount(length, 1);
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "malformed", "missing-etag", "oversized", "timeout", "server-error", "embedded-transient" })
    void recoversUncertainMultipartResponsesUsingMetadata(String failure) throws Exception {
        responder = (exchange, request) -> {
            if (!isPublication(request)) {
                respondNormally(exchange, request);
                return;
            }
            publish(request);
            switch (failure) {
                case "malformed" -> respond(exchange, 200, "<CompleteMultipartUploadResult>");
                case "missing-etag" -> respond(exchange, 200, "<CompleteMultipartUploadResult/>");
                case "oversized" -> {
                    exchange.sendResponseHeaders(200, S3Xml.MAX_RESPONSE_SIZE + 1);
                    try (OutputStream body = exchange.getResponseBody()) {
                        body.write(new byte[S3Xml.MAX_RESPONSE_SIZE + 1]);
                    }
                    catch (IOException ignored) {
                        // The response subscriber cancels reception at its limit.
                    }
                }
                case "timeout" -> {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write(' ');
                    exchange.getResponseBody().flush();
                    awaitRelease();
                    exchange.close();
                }
                case "server-error" -> respond(exchange, 503, "<Error><Code>InternalError</Code></Error>");
                case "embedded-transient" -> respond(exchange, 200, "<Error><Code>InternalError</Code></Error>");
                default -> throw new AssertionError(failure);
            }
        };
        S3OutputFile out = output(PART_SIZE + 1, 0, Duration.ofMillis(500), () -> CREDENTIALS);
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(count("HEAD")).isEqualTo(1);
        assertThat(count("DELETE")).isZero();
        assertPublicationCount(PART_SIZE + 1, 1);
        assertTerminated(out);
    }

    @Test
    void pendingMetadataAndTransientErrorsShareOneBudgetBeforeSuccess() throws Exception {
        AtomicInteger checks = new AtomicInteger();
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                switch (checks.incrementAndGet()) {
                    case 1 -> head(exchange, 404, null, null);
                    case 2 -> head(exchange, 503, null, null);
                    case 3 -> head(exchange, 200, UUID.randomUUID().toString(), Long.toString(publishedLength));
                    default -> respondNormally(exchange, request);
                }
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 3);
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(count("HEAD")).isEqualTo(4);
        assertThat(count("DELETE")).isZero();
        assertPublicationCount(PART_SIZE, 1);
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(ints = { 500, 503 })
    void headNetworkFailureAndServerErrorShareOneBudgetAndRefreshCredentials(int status) throws Exception {
        AtomicInteger checks = new AtomicInteger();
        AtomicInteger resolutions = new AtomicInteger();
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                if (checks.incrementAndGet() == 1) {
                    awaitRelease();
                    exchange.close();
                }
                else if (checks.get() == 2) {
                    head(exchange, status, null, null);
                }
                else {
                    respondNormally(exchange, request);
                }
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(1, 2, Duration.ofMillis(500), () ->
                S3Credentials.of("access-" + resolutions.incrementAndGet(), "secret"));
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(count("HEAD")).isEqualTo(3);
        assertThat(resolutions.get()).isEqualTo(4);
        for (int i = 0; i < requests.size(); i++) {
            assertThat(requests.get(i).authorization()).contains("Credential=access-" + (i + 1) + "/");
        }
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-id", "other-id", "duplicate-id", "duplicate-identical-id", "missing-length", "wrong-length", "negative-length", "overflow-length", "malformed-length", "duplicate-length" })
    void neverAcceptsMissingAmbiguousOrInvalidMetadata(String metadata) throws Exception {
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                String id = "missing-id".equals(metadata) ? null : publishedWriteId;
                if ("other-id".equals(metadata)) {
                    id = UUID.randomUUID().toString();
                }
                String length = switch (metadata) {
                    case "missing-length" -> null;
                    case "wrong-length" -> Long.toString(publishedLength + 1);
                    case "negative-length" -> "-1";
                    case "overflow-length" -> "9223372036854775808";
                    case "malformed-length" -> "1, 2";
                    default -> Long.toString(publishedLength);
                };
                if (metadata.startsWith("duplicate-") && metadata.endsWith("id")) {
                    exchange.getResponseHeaders().add(S3Api.WRITE_ID_HEADER,
                            "duplicate-identical-id".equals(metadata) ? publishedWriteId : UUID.randomUUID().toString());
                }
                if ("duplicate-length".equals(metadata)) {
                    exchange.getResponseHeaders().add("Content-Length", "2");
                }
                head(exchange, 200, id, length);
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(1, 0);
        Throwable failure = catchThrowable(out::close);
        assertUnconfirmed(failure, "verification");
        assertThat(count("HEAD")).isEqualTo(1);
        assertThat(count("DELETE")).isZero();
        assertTerminated(out);
    }

    @Test
    void matchingMetadataHeaderNamesAreCaseInsensitive() throws Exception {
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                exchange.getResponseHeaders().add("X-AMZ-META-HARDWOOD-WRITE-ID", publishedWriteId);
                exchange.getResponseHeaders().add("content-length", Long.toString(publishedLength));
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(1, 0);
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(count("HEAD")).isEqualTo(1);
        assertTerminated(out);
    }

    @Test
    void verifiesLongContentLengthsWithoutNarrowing() throws Exception {
        long length = 3_000_000_000L;
        responder = (exchange, request) -> {
            if (isPublication(request)) {
                publish(request);
                publishedLength = length;
                loseResponse(exchange);
            }
            else {
                respondNormally(exchange, request);
            }
        };
        S3OutputFile out = output(1, 0);
        // Exercise metadata for a large logical output without allocating a 3 GB fixture.
        Field position = S3OutputFile.class.getDeclaredField("position");
        position.setAccessible(true);
        position.setLong(out, length);
        assertThatCode(out::close).doesNotThrowAnyException();
        assertThat(count("HEAD")).isEqualTo(1);
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(ints = { 400, 403, 409, 502 })
    void nonTransientMetadataErrorsStopVerificationWithoutClaimingAbsence(int status) throws Exception {
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                head(exchange, status, null, null);
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 3);
        Throwable failure = catchThrowable(out::close);
        assertUnconfirmed(failure, "HTTP " + status);
        assertThat(count("HEAD")).isEqualTo(1);
        assertThat(count("DELETE")).isEqualTo(1);
        assertThat(requests.getLast().method()).isEqualTo("DELETE");
        assertTerminated(out);
    }

    @Test
    void exhaustionVerifiesBeforeAbortAndNoSuchUploadDoesNotProvePublicationFailed() throws Exception {
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                head(exchange, 404, null, null);
            }
            else if ("DELETE".equals(request.method())) {
                respond(exchange, 404, "<Error><Code>NoSuchUpload</Code></Error>");
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 2);
        Throwable failure = catchThrowable(out::close);
        assertUnconfirmed(failure, "exhausted");
        assertThat(count("HEAD")).isEqualTo(3);
        assertThat(count("DELETE")).isEqualTo(1);
        assertThat(requests.stream().skip(3).map(Request::method)).containsExactly("HEAD", "HEAD", "HEAD", "DELETE");
        assertThat(publishedWriteId).isNotNull();
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, PART_SIZE })
    void anotherWriterReplacingObjectPreventsRecoveryAndIsNeverDeleted(int length) throws Exception {
        String otherId = UUID.randomUUID().toString();
        AtomicReference<String> ourId = new AtomicReference<>();
        responder = (exchange, request) -> {
            if (isPublication(request)) {
                publish(request);
                ourId.set(publishedWriteId);
                publishedWriteId = otherId;
                loseResponse(exchange);
            }
            else {
                respondNormally(exchange, request);
            }
        };
        S3OutputFile out = output(length, 1);
        Throwable failure = catchThrowable(out::close);
        assertThat(failure).isInstanceOf(IOException.class);
        assertThat(messages(failure)).anyMatch(message -> message.contains(ourId.get()) && message.contains("may contain"));
        assertThat(publishedWriteId).isEqualTo(otherId);
        assertThat(count("HEAD")).isEqualTo(2);
        assertThat(requests.stream().filter(r -> "DELETE".equals(r.method())).map(r -> r.uri().getRawQuery()))
                .allMatch(query -> "uploadId=owned-upload".equals(query));
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "InvalidPart", "InvalidPartOrder", "EntityTooSmall", "NoSuchUpload" })
    void definitiveCompletionErrorsAreNotConvertedToSuccess(String code) throws Exception {
        responder = (exchange, request) -> {
            if (isPublication(request)) {
                publish(request);
                respond(exchange, 200, "<Error><Code>" + code + "</Code></Error>");
            }
            else {
                respondNormally(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 2);
        Throwable failure = catchThrowable(out::close);
        assertThat(failure).isInstanceOf(S3Xml.ServiceException.class).hasMessageContaining(code);
        assertThat(count("HEAD")).isZero();
        assertThat(count("DELETE")).isEqualTo(1);
        assertTerminated(out);
    }

    @Test
    void definitivePutRejectionDoesNotUseEvenMatchingMetadata() throws Exception {
        responder = (exchange, request) -> {
            if (isPublication(request)) {
                publish(request);
                respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
            }
            else {
                respondNormally(exchange, request);
            }
        };
        S3OutputFile out = output(1, 2);
        assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining("AccessDenied");
        assertThat(count("HEAD")).isZero();
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "InvalidPart", "AccessDenied" })
    void serverStatusDoesNotOverrideExplicitValidationOrPermissionError(String code) throws Exception {
        responder = (exchange, request) -> {
            if (isPublication(request)) {
                publish(request);
                respond(exchange, 503, "<Error><Code>" + code + "</Code></Error>");
            }
            else {
                respondNormally(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 2);
        assertThatThrownBy(out::close).isInstanceOf(IOException.class).hasMessageContaining(code);
        assertThat(count("HEAD")).isZero();
        assertThat(count("DELETE")).isEqualTo(1);
        assertTerminated(out);
    }

    @Test
    void failedFinalPartNeverStartsPublicationVerification() throws Exception {
        S3OutputFile out = output(PART_SIZE + 1, 0);
        responder = (exchange, request) -> {
            if ("PUT".equals(request.method())) {
                loseResponse(exchange);
            }
            else {
                respondNormally(exchange, request);
            }
        };
        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
        assertThat(count("HEAD")).isZero();
        assertThat(requests.stream().filter(this::isPublication)).isEmpty();
        assertTerminated(out);
    }

    @Test
    void verificationFailureAndCleanupFailureAreSuppressedOnOriginalTimeout() throws Exception {
        responder = (exchange, request) -> {
            if (isPublication(request)) {
                publish(request);
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write(' ');
                exchange.getResponseBody().flush();
                awaitRelease();
                exchange.close();
            }
            else if ("HEAD".equals(request.method())) {
                head(exchange, 403, null, null);
            }
            else if ("DELETE".equals(request.method())) {
                respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
            }
            else {
                respondNormally(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 0, Duration.ofMillis(500), () -> CREDENTIALS);
        Throwable failure = catchThrowable(out::close);
        assertThat(failure).isInstanceOf(HttpTimeoutException.class);
        assertUnconfirmed(failure, "HTTP 403");
        assertThat(messages(failure)).anyMatch(message -> message.contains("AccessDenied"));
        int operations = requests.size();
        out.close();
        assertThat(requests).hasSize(operations);
        responder = this::respondNormally;
        out.discard();
        assertThat(count("DELETE")).isEqualTo(2);
        assertThat(publishedWriteId).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "runtime", "error" })
    void metadataCredentialFailureKeepsOriginalPublicationFailure(String kind) throws Exception {
        Throwable credentialFailure = "runtime".equals(kind) ? new IllegalStateException("HEAD credentials failed") : new AssertionError("HEAD credentials failed");
        AtomicInteger resolutions = new AtomicInteger();
        S3CredentialsProvider credentials = () -> {
            if (resolutions.incrementAndGet() == 2) {
                if (credentialFailure instanceof Error error) {
                    throw error;
                }
                if (credentialFailure instanceof RuntimeException runtime) {
                    throw runtime;
                }
            }
            return CREDENTIALS;
        };
        responder = this::publishAndLoseResponse;
        S3OutputFile out = output(1, 3, Duration.ofSeconds(5), credentials);
        Throwable failure = catchThrowable(out::close);
        assertThat(failure).isInstanceOf(IOException.class);
        assertThat(failure.getSuppressed()).contains(credentialFailure);
        assertUnconfirmed(failure, "failed");
        assertThat(resolutions.get()).isEqualTo(2);
        assertThat(count("HEAD")).isZero();
        assertTerminated(out);
    }

    @ParameterizedTest
    @ValueSource(strings = { "publication", "verification" })
    void interruptionStopsRecoveryAndRestoresFlagWhileAttemptingCleanup(String stage) throws Exception {
        CountDownLatch waiting = new CountDownLatch(1);
        responder = (exchange, request) -> {
            boolean block = "publication".equals(stage) ? isPublication(request) : "HEAD".equals(request.method());
            if (block) {
                if (isPublication(request)) {
                    publish(request);
                }
                waiting.countDown();
                awaitRelease();
                exchange.close();
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread closer = new Thread(() -> {
            failure.set(catchThrowable(out::close));
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        closer.start();
        try {
            assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
            closer.interrupt();
            closer.join(5000);
            assertThat(closer.isAlive()).isFalse();
            assertThat(interrupted.get()).isTrue();
            assertUnconfirmed(failure.get(), "interruption");
            assertThat(count("HEAD")).isEqualTo("publication".equals(stage) ? 0 : 1);
            assertThat(count("DELETE")).isEqualTo(1);
            assertThat(publishedWriteId).isNotNull();
            assertTerminated(out);
        }
        finally {
            release.countDown();
            closer.interrupt();
            closer.join(5000);
        }
    }

    private void assertUnconfirmed(Throwable failure, String detail) {
        assertThat(failure).isInstanceOf(IOException.class);
        assertThat(messages(failure)).anyMatch(message -> message.contains("may contain")
                && message.contains("write ID " + publishedWriteId) && message.contains(detail));
    }

    @Test
    void interruptionDuringVerificationBackoffStopsFurtherChecks() throws Exception {
        responder = (exchange, request) -> {
            if ("HEAD".equals(request.method())) {
                head(exchange, 404, null, null);
            }
            else {
                publishAndLoseResponse(exchange, request);
            }
        };
        S3OutputFile out = output(PART_SIZE, 5);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread closer = new Thread(() -> {
            failure.set(catchThrowable(out::close));
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        closer.start();
        try {
            assertThat(awaitBackoff(closer)).isTrue();
            closer.interrupt();
            closer.join(5000);
            assertThat(closer.isAlive()).isFalse();
            assertThat(interrupted.get()).isTrue();
            assertUnconfirmed(failure.get(), "interruption");
            assertThat(count("HEAD")).isEqualTo(1);
            assertThat(count("DELETE")).isEqualTo(1);
            assertTerminated(out);
        }
        finally {
            closer.interrupt();
            closer.join(5000);
        }
    }

    private static boolean awaitBackoff(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            for (StackTraceElement frame : thread.getStackTrace()) {
                if (S3Api.class.getName().equals(frame.getClassName()) && "sleepBeforeRetry".equals(frame.getMethodName())) {
                    return true;
                }
            }
            Thread.sleep(1);
        }
        return false;
    }

    private List<String> messages(Throwable failure) {
        return Arrays.stream(failure.getSuppressed()).map(Throwable::getMessage).toList();
    }

    private void assertPublicationCount(int length, long expected) {
        assertThat(requests.stream().filter(this::isPublication).count()).isEqualTo(expected);
        if (length >= PART_SIZE) {
            assertThat(requests.stream().filter(r -> "POST".equals(r.method()) && "uploads=".equals(r.uri().getRawQuery())).count()).isEqualTo(1);
        }
    }

    private void assertTerminated(S3OutputFile out) throws Exception {
        int operations = requests.size();
        out.close();
        out.discard();
        out.close();
        assertThat(requests).hasSize(operations);
        assertThatThrownBy(out::position).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> out.write(ByteBuffer.allocate(0))).isInstanceOf(IllegalStateException.class);
        Field buffer = S3OutputFile.class.getDeclaredField("buffer");
        buffer.setAccessible(true);
        assertThat(buffer.get(out)).isNull();
        assertThat(client.isTerminated()).isFalse();
    }

    private S3OutputFile output(int length, int retries) throws IOException {
        return output(length, retries, Duration.ofSeconds(5), () -> CREDENTIALS);
    }

    private S3OutputFile output(int length, int retries, Duration timeout, S3CredentialsProvider credentials) throws IOException {
        S3Api api = new S3Api(client, credentials, "us-east-1", URI.create("http://localhost:" + server.getAddress().getPort()), true, timeout, retries);
        S3OutputFile out = new S3OutputFile(api, "bucket", KEY, PART_SIZE);
        out.create();
        out.write(ByteBuffer.wrap(new byte[length]));
        return out;
    }

    private boolean isPublication(Request request) {
        return "PUT".equals(request.method()) && request.uri().getRawQuery() == null
                || "POST".equals(request.method()) && !"uploads=".equals(request.uri().getRawQuery());
    }

    private long count(String method) {
        return requests.stream().filter(r -> method.equals(r.method())).count();
    }

    private void publish(Request request) {
        if ("PUT".equals(request.method())) {
            publishedWriteId = request.writeId();
            publishedLength = request.length();
        }
        else {
            publishedWriteId = uploadWriteId;
            publishedLength = partLengths.values().stream().mapToLong(Integer::longValue).sum();
        }
    }

    private void publishAndLoseResponse(HttpExchange exchange, Request request) throws IOException {
        if (isPublication(request)) {
            publish(request);
            loseResponse(exchange);
        }
        else {
            respondNormally(exchange, request);
        }
    }

    private void respondNormally(HttpExchange exchange, Request request) throws IOException {
        String query = request.uri().getRawQuery();
        if ("POST".equals(request.method()) && "uploads=".equals(query)) {
            uploadWriteId = request.writeId();
            respond(exchange, 200, "<InitiateMultipartUploadResult><UploadId>" + UPLOAD_ID + "</UploadId></InitiateMultipartUploadResult>");
        }
        else if ("PUT".equals(request.method()) && query != null) {
            int number = Integer.parseInt(query.substring("partNumber=".length(), query.indexOf('&')));
            partLengths.put(number, request.length());
            exchange.getResponseHeaders().add("ETag", "\"part-" + number + "\"");
            respond(exchange, 200, "");
        }
        else if (isPublication(request)) {
            publish(request);
            respond(exchange, 200, "POST".equals(request.method())
                    ? "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>" : "");
        }
        else if ("HEAD".equals(request.method())) {
            head(exchange, publishedWriteId == null ? 404 : 200, publishedWriteId, Long.toString(publishedLength));
        }
        else if ("DELETE".equals(request.method())) {
            assertThat(query).isEqualTo("uploadId=owned-upload");
            respond(exchange, 204, "");
        }
        else if ("GET".equals(request.method())) {
            respond(exchange, 404, "<Error><Code>NoSuchUpload</Code></Error>");
        }
        else {
            throw new IOException("Unexpected request: " + request);
        }
    }

    private static void head(HttpExchange exchange, int status, String id, String length) throws IOException {
        if (id != null) {
            exchange.getResponseHeaders().add(S3Api.WRITE_ID_HEADER, id);
        }
        if (length != null) {
            exchange.getResponseHeaders().add("Content-Length", length);
        }
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static void loseResponse(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, 100);
        exchange.getResponseBody().write(1);
        exchange.close();
    }

    private static void respond(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private void awaitRelease() {
        try {
            release.await(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    private interface Responder {
        void respond(HttpExchange exchange, Request request) throws IOException;
    }

    private record Request(String method, URI uri, int length, String writeId, String authorization) {
    }
}
