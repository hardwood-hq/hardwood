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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.S3Credentials;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks how [S3Api] retries a range `GET`, against a local endpoint whose first answer fails
/// as [#firstAnswer] says and whose second carries the range from the start it asks for.
class S3ApiRetryTest {

    enum FirstAnswer {
        /// A `503` with a body too large to fit in the socket buffers. The endpoint's write of
        /// that body completes only once the client reads or closes the response.
        LARGE_ERROR,
        /// A `206` whose connection closes after half of its body.
        TRUNCATED_BODY
    }

    private static final int ERROR_BODY_SIZE = 16 * 1024 * 1024;
    private static final byte[] RANGE_BODY = { 1, 2, 3, 4 };

    private final AtomicInteger requests = new AtomicInteger();
    private final List<String> ranges = new CopyOnWriteArrayList<>();
    private final CountDownLatch errorResponseReleased = new CountDownLatch(1);
    private volatile FirstAnswer firstAnswer = FirstAnswer.LARGE_ERROR;
    private ExecutorService executor;
    private HttpServer server;
    private HttpClient httpClient;
    private S3Api api;

    @BeforeEach
    void setUp() throws IOException {
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.setExecutor(executor);
        server.start();

        httpClient = HttpClient.newHttpClient();
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        S3Credentials credentials = S3Credentials.of("access", "secret");
        api = new S3Api(httpClient, () -> credentials, "us-east-1", endpoint, true,
                Duration.ofSeconds(10), 1);
    }

    @AfterEach
    void tearDown() {
        httpClient.shutdownNow();
        server.stop(0);
        executor.shutdownNow();
    }

    private void respond(HttpExchange exchange) throws IOException {
        if (requests.getAndIncrement() == 0) {
            switch (firstAnswer) {
                case LARGE_ERROR -> respondLargeError(exchange);
                case TRUNCATED_BODY -> respondTruncated(exchange);
            }
            return;
        }
        String range = exchange.getRequestHeaders().getFirst("Range");
        ranges.add(range);
        // Serves the bytes from the requested start, which a resumed request moves
        int from = Integer.parseInt(range.substring("bytes=".length(), range.indexOf('-')));
        exchange.getResponseHeaders().add("Content-Range", "bytes " + from + "-3/4");
        exchange.sendResponseHeaders(206, RANGE_BODY.length - from);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(RANGE_BODY, from, RANGE_BODY.length - from);
        }
    }

    private void respondLargeError(HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(503, ERROR_BODY_SIZE);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(new byte[ERROR_BODY_SIZE]);
            }
        }
        catch (IOException e) {
            // The client closed the connection instead of reading the body
        }
        finally {
            errorResponseReleased.countDown();
        }
    }

    private static void respondTruncated(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().add("Content-Range", "bytes 0-3/4");
        exchange.sendResponseHeaders(206, RANGE_BODY.length);
        OutputStream body = exchange.getResponseBody();
        body.write(RANGE_BODY, 0, RANGE_BODY.length / 2);
        body.flush();
        // Fails for the bytes missing; the server closes the connection of a handler that throws
        body.close();
    }

    @Test
    void rangeRetryReleasesFailedResponse() throws Exception {
        HttpResponse<RangeBody> response = api.getRange("bucket", "object.parquet", 0, RANGE_BODY.length,
                null);

        assertThat(response.statusCode()).isEqualTo(206);
        assertThat(bytes(response.body())).isEqualTo(RANGE_BODY);
        assertThat(requests.get()).isEqualTo(2);
        assertThat(errorResponseReleased.await(5, TimeUnit.SECONDS))
                .as("503 response body released before the retry")
                .isTrue();
    }

    @Test
    void rangeResumesBodyFailingMidway() throws Exception {
        firstAnswer = FirstAnswer.TRUNCATED_BODY;

        // Only a conditional range resumes; the endpoint sends no ETag and ignores If-Match
        HttpResponse<RangeBody> response = api.getRange("bucket", "object.parquet", 0, RANGE_BODY.length,
                "\"v1\"");

        assertThat(response.statusCode()).isEqualTo(206);
        assertThat(response.body().overflow()).isFalse();
        assertThat(bytes(response.body())).isEqualTo(RANGE_BODY);
        assertThat(requests.get()).isEqualTo(2);
        assertThat(ranges).containsExactly("bytes=2-3");
    }

    private static byte[] bytes(RangeBody body) {
        ByteBuffer buffer = body.bytes();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
