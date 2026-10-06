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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(15)
class S3WriteApiTest {

    private static final String UPLOAD_ID = "opaque+/=% 雪";
    private static final String WRITE_ID = "031de2e0-08aa-4f35-b068-67bb33be5302";
    private static final String KEY = "folder//../snow 雪+%?#.parquet";
    private static final S3Credentials CREDENTIALS = new S3Credentials("access", "secret", "session-token");

    private final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
    private final AtomicInteger requestCount = new AtomicInteger();
    private final CountDownLatch releaseResponse = new CountDownLatch(1);
    private ExecutorService executor;
    private HttpServer server;
    private HttpClient client;
    private volatile HttpHandler responder;

    @BeforeEach
    void setUp() throws IOException {
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            Map<String, List<String>> headers = new LinkedHashMap<>(exchange.getRequestHeaders());
            requestCount.incrementAndGet();
            requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestURI(), headers,
                    exchange.getRequestBody().readAllBytes()));
            responder.handle(exchange);
        });
        server.setExecutor(executor);
        server.start();
        client = HttpClient.newBuilder().proxy(ProxySelector.of(server.getAddress())).build();
    }

    @AfterEach
    void tearDown() {
        releaseResponse.countDown();
        client.shutdownNow();
        server.stop(0);
        executor.shutdownNow();
    }

    @ParameterizedTest
    @CsvSource({ "true, http://s3.test:80", "false, http://s3.test:80", "true, http://s3.test:9000", "false, http://s3.test:9000" })
    void signsMultipartOperationsAndPreservesKeysAndUploadIds(boolean pathStyle, String endpoint) throws Exception {
        responder = exchange -> {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getRawQuery();
            if ("POST".equals(method) && "uploads=".equals(query)) {
                respond(exchange, 200, "<InitiateMultipartUploadResult><UploadId>" + UPLOAD_ID + "</UploadId></InitiateMultipartUploadResult>");
            }
            else if ("PUT".equals(method)) {
                exchange.getResponseHeaders().add("ETag", "\"opaque-etag\"");
                respond(exchange, 200, "");
            }
            else if ("POST".equals(method)) {
                respond(exchange, 200, "<CompleteMultipartUploadResult><ETag>\"whole-etag\"</ETag></CompleteMultipartUploadResult>");
            }
            else {
                respond(exchange, 204, "");
            }
        };
        S3Api api = new S3Api(client, () -> CREDENTIALS, "us-east-1", URI.create(endpoint), pathStyle, Duration.ofSeconds(5), 0);

        String id = api.initiateMultipartUpload("bucket", KEY, WRITE_ID);
        String etag = api.uploadPart("bucket", KEY, id, 1, new byte[] { 1, 2, 3, 99 }, 3);
        assertThat(api.completeMultipartUpload("bucket", KEY, id, List.of(new S3Xml.Part(1, etag)))).isEqualTo("\"whole-etag\"");
        api.abortMultipartUpload("bucket", KEY, id);

        Request initiation = nextRequest();
        Request part = nextRequest();
        Request completion = nextRequest();
        Request abort = nextRequest();
        assertThat(id).isEqualTo(UPLOAD_ID);
        assertThat(initiation.header("x-amz-meta-hardwood-write-id")).isEqualTo(WRITE_ID);
        assertThat(initiation.header("Authorization")).contains("x-amz-meta-hardwood-write-id");
        assertThat(part.body()).containsExactly(1, 2, 3);
        assertThat(part.uri().getRawQuery()).contains("partNumber=1", "uploadId=opaque%2B%2F%3D%25%20%E9%9B%AA");
        assertThat(part.header("x-amz-meta-hardwood-write-id")).isNull();
        assertThat(new String(completion.body(), StandardCharsets.UTF_8)).contains("<ETag>\"opaque-etag\"</ETag>");
        assertThat(abort.method()).isEqualTo("DELETE");
        for (Request request : List.of(initiation, part, completion, abort)) {
            assertThat(request.uri().getRawPath()).endsWith("/folder//../snow%20%E9%9B%AA%2B%25%3F%23.parquet");
            String port = endpoint.endsWith(":9000") ? ":9000" : "";
            assertThat(request.header("Host")).isEqualTo((pathStyle ? "s3.test" : "bucket.s3.test") + port);
            assertSigned(request, CREDENTIALS);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void smallPutSignsOnlyValidPrefixAndIncludesWriteMetadata(int length) throws Exception {
        responder = exchange -> respond(exchange, 200, "");
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);

        api.putObject("bucket", "file", new byte[] { 1, 2, 3, 99 }, length, WRITE_ID);

        Request request = nextRequest();
        assertThat(request.body()).isEqualTo(Arrays.copyOf(new byte[] { 1, 2, 3 }, length));
        assertThat(request.header("x-amz-meta-hardwood-write-id")).isEqualTo(WRITE_ID);
        assertSigned(request, CREDENTIALS);
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 500, 503 })
    void retriesPartsWithSamePayloadAndFreshCredentials(int status) throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        responder = exchange -> {
            if (attempts.getAndIncrement() == 0) {
                respond(exchange, status, "<Error><Code>InternalError</Code></Error>");
            }
            else {
                exchange.getResponseHeaders().add("ETag", "\"etag\"");
                respond(exchange, 200, "");
            }
        };
        AtomicInteger resolutions = new AtomicInteger();
        S3Api api = api(true, 1, Duration.ofSeconds(5), () ->
                new S3Credentials("access-" + resolutions.incrementAndGet(), "secret", "session-token"));

        assertThat(api.uploadPart("bucket", "file", UPLOAD_ID, 4, new byte[] { 1, 2, 99 }, 2)).isEqualTo("\"etag\"");

        Request first = nextRequest();
        Request second = nextRequest();
        assertThat(first.uri()).isEqualTo(second.uri());
        assertThat(first.body()).containsExactly(1, 2);
        assertThat(second.body()).isEqualTo(first.body());
        assertSigned(first, new S3Credentials("access-1", "secret", "session-token"));
        assertSigned(second, new S3Credentials("access-2", "secret", "session-token"));
        assertThat(resolutions.get()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(ints = { 400, 403, 404 })
    void doesNotRetryDefinitivePartRejection(int status) {
        responder = exchange -> respond(exchange, status, "<Error><Code>AccessDenied</Code><RequestId>request-id</RequestId></Error>");

        assertThatThrownBy(() -> api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS)
                .uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 1))
                .isInstanceOf(IOException.class).hasMessageContaining("HTTP " + status).hasMessageContaining("request-id");
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void doesNotRetryMissingOrAmbiguousPartETag() {
        responder = exchange -> respond(exchange, 200, "");
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        assertThatThrownBy(() -> api.uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 1))
                .isInstanceOf(IOException.class).hasMessageContaining("ETag");
        assertThat(requestCount.get()).isEqualTo(1);

        responder = exchange -> {
            exchange.getResponseHeaders().add("ETag", "one");
            exchange.getResponseHeaders().add("ETag", "two");
            respond(exchange, 200, "");
        };
        assertThatThrownBy(() -> api.uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 1))
                .isInstanceOf(IOException.class).hasMessageContaining("ETag");
        assertThat(requestCount.get()).isEqualTo(2);
    }

    @Test
    void exhaustionHonorsPartRetryBudget() {
        responder = exchange -> respond(exchange, 503, "<Error><Code>SlowDown</Code></Error>");
        assertThatThrownBy(() -> api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS)
                .uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 1))
                .isInstanceOf(IOException.class).hasMessageContaining("SlowDown");
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void retriesPartAfterTruncatedTransportResponse() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        responder = exchange -> {
            if (attempts.getAndIncrement() == 0) {
                exchange.sendResponseHeaders(200, 100);
                exchange.getResponseBody().write(1);
                exchange.close();
            }
            else {
                exchange.getResponseHeaders().add("ETag", "\"etag\"");
                respond(exchange, 200, "");
            }
        };
        assertThat(api(true, 1, Duration.ofSeconds(5), () -> CREDENTIALS)
                .uploadPart("bucket", "file", UPLOAD_ID, 2, new byte[] { 1, 2, 99 }, 2)).isEqualTo("\"etag\"");
        Request first = nextRequest();
        Request second = nextRequest();
        assertThat(first.uri()).isEqualTo(second.uri());
        assertThat(first.body()).containsExactly(1, 2);
        assertThat(second.body()).isEqualTo(first.body());
        assertThat(requestCount.get()).isEqualTo(2);
    }

    @Test
    void acceptsExplicitlyUnboundedRequestTimeout() throws Exception {
        responder = exchange -> respond(exchange, 200, "<InitiateMultipartUploadResult><UploadId>id</UploadId></InitiateMultipartUploadResult>");
        assertThat(api(true, 0, null, () -> CREDENTIALS).initiateMultipartUpload("bucket", "file", WRITE_ID)).isEqualTo("id");
    }

    @Test
    void doesNotReplayInitiationOrPublicationOnServerError() {
        responder = exchange -> respond(exchange, 503, "<Error><Code>InternalError</Code></Error>");
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        assertThatThrownBy(() -> api.initiateMultipartUpload("bucket", "file", WRITE_ID)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> api.putObject("bucket", "file", new byte[] { 1 }, 1, WRITE_ID)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> api.completeMultipartUpload("bucket", "file", UPLOAD_ID, List.of(new S3Xml.Part(1, "etag"))))
                .isInstanceOf(IOException.class);
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void doesNotReplayLostInitiationOrPublicationResponse() {
        responder = exchange -> {
            exchange.sendResponseHeaders(200, 100);
            exchange.getResponseBody().write(1);
            exchange.close();
        };
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        assertThatThrownBy(() -> api.initiateMultipartUpload("bucket", "file", WRITE_ID)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> api.putObject("bucket", "file", new byte[] { 1 }, 1, WRITE_ID)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> api.completeMultipartUpload("bucket", "file", UPLOAD_ID, List.of(new S3Xml.Part(1, "etag"))))
                .isInstanceOf(IOException.class);
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void waitsForEntireCompletionResponse() throws Exception {
        CountDownLatch whitespaceSent = new CountDownLatch(1);
        responder = exchange -> {
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(bytes(" \n"));
                body.flush();
                whitespaceSent.countDown();
                awaitRelease();
                body.write(bytes("<Error><Code>InvalidPart</Code></Error>"));
            }
        };
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        Future<String> result = executor.submit(() -> api.completeMultipartUpload("bucket", "file", UPLOAD_ID, List.of(new S3Xml.Part(1, "etag"))));

        assertThat(whitespaceSent.await(5, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> result.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        releaseResponse.countDown();
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(S3Xml.ServiceException.class);
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void timeoutIncludesResponseBodyAndCancelsReception() throws Exception {
        CountDownLatch headersSent = new CountDownLatch(1);
        responder = exchange -> {
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write(bytes(" \n"));
            exchange.getResponseBody().flush();
            headersSent.countDown();
            awaitRelease();
            exchange.close();
        };
        long start = System.nanoTime();
        assertThatThrownBy(() -> api(true, 2, Duration.ofMillis(500), () -> CREDENTIALS)
                .completeMultipartUpload("bucket", "file", UPLOAD_ID, List.of(new S3Xml.Part(1, "etag"))))
                .isInstanceOf(HttpTimeoutException.class);
        assertThat(headersSent.getCount()).isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void boundsBodyReceptionAndDoesNotRetryOversizedResponse() throws Exception {
        CountDownLatch responseReleased = new CountDownLatch(1);
        responder = exchange -> {
            try {
                exchange.sendResponseHeaders(200, 16 * 1024 * 1024);
                try (OutputStream body = exchange.getResponseBody()) {
                    body.write(new byte[16 * 1024 * 1024]);
                }
            }
            catch (IOException e) {
                // The client stops reading at the response limit.
            }
            finally {
                responseReleased.countDown();
            }
        };
        assertThatThrownBy(() -> api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS)
                .uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 1))
                .isInstanceOf(IOException.class).hasMessageContaining("exceeds");
        assertThat(responseReleased.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void interruptionStopsRetriesAndRestoresFlag() throws Exception {
        responder = exchange -> {
            awaitRelease();
            exchange.close();
        };
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread worker = new Thread(() -> {
            try {
                api.uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 1);
            }
            catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        worker.start();
        try {
            nextRequest();
            worker.interrupt();
            worker.join(5000);
            assertThat(worker.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(IOException.class);
            assertThat(interrupted.get()).isTrue();
            assertThat(requestCount.get()).isEqualTo(1);
        }
        finally {
            releaseResponse.countDown();
            worker.interrupt();
            worker.join(5000);
        }
    }

    @Test
    void abortRetriesButSingleAttemptCleanupDoesNotMultiplyBudget() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        responder = exchange -> respond(exchange, attempts.getAndIncrement() == 0 ? 503 : 204, "");
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        api.abortMultipartUpload("bucket", "file", UPLOAD_ID);
        assertThat(requestCount.get()).isEqualTo(2);
        responder = exchange -> respond(exchange, 503, "<Error><Code>SlowDown</Code></Error>");
        assertThatThrownBy(() -> api.abortMultipartUploadOnce("bucket", "file", UPLOAD_ID)).isInstanceOf(IOException.class);
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void abortAcceptsOnlyParsedNoSuchUpload() throws Exception {
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        responder = exchange -> respond(exchange, 404, "<Error><Code>NoSuchUpload</Code></Error>");
        api.abortMultipartUpload("bucket", "file", UPLOAD_ID);
        responder = exchange -> respond(exchange, 404, "<Code>NoSuchUpload</Code>");
        assertThatThrownBy(() -> api.abortMultipartUpload("bucket", "file", UPLOAD_ID)).isInstanceOf(IOException.class);
        responder = exchange -> respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>");
        assertThatThrownBy(() -> api.abortMultipartUpload("bucket", "file", UPLOAD_ID)).isInstanceOf(IOException.class);
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(ints = { 200, 403, 404, 503 })
    void headReturnsMetadataAndStatusWithoutHiddenRetries(int status) throws Exception {
        responder = exchange -> {
            exchange.getResponseHeaders().add("x-amz-meta-hardwood-write-id", WRITE_ID);
            exchange.getResponseHeaders().add("Content-Length", "123");
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        };

        HttpResponse<Void> response = api(false, 2, Duration.ofSeconds(5), () -> CREDENTIALS).headObject("bucket", KEY);

        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.body()).isNull();
        assertThat(response.headers().firstValue("x-amz-meta-hardwood-write-id")).contains(WRITE_ID);
        assertThat(response.headers().firstValue("Content-Length")).contains("123");
        Request request = nextRequest();
        assertThat(request.method()).isEqualTo("HEAD");
        assertThat(request.body()).isEmpty();
        assertSigned(request, CREDENTIALS);
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void followsListPartsPaginationWithoutTreatingIncompletePageAsEmpty() throws Exception {
        responder = exchange -> {
            String query = exchange.getRequestURI().getRawQuery();
            if (query.contains("part-number-marker=7")) {
                respond(exchange, 200, "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>8</PartNumber><ETag>etag</ETag></Part></ListPartsResult>");
            }
            else {
                respond(exchange, 200, "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>7</NextPartNumberMarker></ListPartsResult>");
            }
        };

        assertThat(api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS).inspectMultipartUpload("bucket", "file", UPLOAD_ID))
                .isEqualTo(S3Api.UploadState.HAS_PARTS);
        assertSigned(nextRequest(), CREDENTIALS);
        assertSigned(nextRequest(), CREDENTIALS);
        assertThat(requestCount.get()).isEqualTo(2);
    }

    @Test
    void distinguishesEmptyUploadFromMissingUpload() throws Exception {
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        responder = exchange -> respond(exchange, 200, "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>");
        assertThat(api.inspectMultipartUpload("bucket", "file", UPLOAD_ID)).isEqualTo(S3Api.UploadState.EMPTY);
        responder = exchange -> respond(exchange, 404, "<Error><Code>NoSuchUpload</Code></Error>");
        assertThat(api.inspectMultipartUpload("bucket", "file", UPLOAD_ID)).isEqualTo(S3Api.UploadState.MISSING);
        responder = exchange -> respond(exchange, 404, "missing");
        assertThatThrownBy(() -> api.inspectMultipartUpload("bucket", "file", UPLOAD_ID)).isInstanceOf(IOException.class);
    }

    @Test
    void rejectsStuckListPartsMarkerAndDoesNotRetryListErrors() {
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        responder = exchange -> respond(exchange, 200, "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>1</NextPartNumberMarker></ListPartsResult>");
        assertThatThrownBy(() -> api.inspectMultipartUpload("bucket", "file", UPLOAD_ID))
                .isInstanceOf(IOException.class).hasMessageContaining("marker");
        assertThat(requestCount.get()).isEqualTo(2);
        responder = exchange -> respond(exchange, 503, "<Error><Code>SlowDown</Code></Error>");
        assertThatThrownBy(() -> api.inspectMultipartUpload("bucket", "file", UPLOAD_ID)).isInstanceOf(IOException.class);
        assertThat(requestCount.get()).isEqualTo(3);
    }

    @Test
    void rejectsInvalidPayloadAndUploadInputsBeforeRequests() {
        S3Api api = api(true, 2, Duration.ofSeconds(5), () -> CREDENTIALS);
        assertThatThrownBy(() -> api.uploadPart("bucket", "file", UPLOAD_ID, 0, new byte[] { 1 }, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.uploadPart("bucket", "file", "", 1, new byte[] { 1 }, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.uploadPart("bucket", "file", UPLOAD_ID, 1, new byte[] { 1 }, 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> api.initiateMultipartUpload("bucket", "file", " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(requestCount.get()).isZero();
    }

    private S3Api api(boolean pathStyle, int retries, Duration timeout, S3CredentialsProvider credentials) {
        return new S3Api(client, credentials, "us-east-1", URI.create("http://s3.test:80"), pathStyle, timeout, retries);
    }

    private Request nextRequest() throws InterruptedException {
        Request request = requests.poll(5, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        return request;
    }

    private void awaitRelease() throws IOException {
        try {
            if (!releaseResponse.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting to release test response");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static void assertSigned(Request request, S3Credentials credentials) throws Exception {
        String authorization = request.header("Authorization");
        String signedNames = authorization.substring(authorization.indexOf("SignedHeaders=") + "SignedHeaders=".length()).split(",")[0];
        Map<String, String> headers = new LinkedHashMap<>();
        for (String name : signedNames.split(";")) {
            headers.put(name, request.header(name));
        }
        String hash = Aws4Signer.hexEncode(MessageDigest.getInstance("SHA-256").digest(request.body()));
        assertThat(request.header("x-amz-content-sha256")).isEqualTo(hash);
        assertThat(request.header("x-amz-security-token")).isEqualTo(credentials.sessionToken());
        ZonedDateTime date = LocalDateTime.parse(request.header("x-amz-date"), Aws4Signer.TIMESTAMP_FORMAT).atZone(ZoneOffset.UTC);
        assertThat(authorization).isEqualTo(Aws4Signer.sign(request.method(), request.uri(), headers, hash,
                credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken(), "us-east-1", "s3", date).authorizationHeader());
    }

    private static void respond(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = bytes(text);
        if (status == 204) {
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
            return;
        }
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private record Request(String method, URI uri, Map<String, List<String>> headers, byte[] body) {

        String header(String name) {
            return headers.entrySet().stream().filter(entry -> name.equalsIgnoreCase(entry.getKey()))
                    .map(entry -> entry.getValue().getFirst()).findFirst().orElse(null);
        }
    }
}
