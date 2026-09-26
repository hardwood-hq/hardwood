/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;
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

/// Checks that [S3Api] releases the response of a failed attempt before it
/// retries, against a local endpoint whose first answer is a `503` with a
/// body too large to fit in the socket buffers. The endpoint's write of
/// that body completes only once the client reads or closes the response.
class S3ApiRetryTest {

    private static final int ERROR_BODY_SIZE = 16 * 1024 * 1024;
    private static final byte[] RANGE_BODY = { 1, 2, 3, 4 };

    private final AtomicInteger requests = new AtomicInteger();
    private final CountDownLatch errorResponseReleased = new CountDownLatch(1);
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
            return;
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes 0-3/4");
        exchange.sendResponseHeaders(206, RANGE_BODY.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(RANGE_BODY);
        }
    }

    @Test
    void streamRetryReleasesFailedResponse() throws Exception {
        HttpResponse<InputStream> response = api.getStream("bucket", "object.parquet", "bytes=0-3");
        try (InputStream body = response.body()) {
            assertThat(response.statusCode()).isEqualTo(206);
            assertThat(body.readAllBytes()).isEqualTo(RANGE_BODY);
        }

        assertThat(requests.get()).isEqualTo(2);
        assertThat(errorResponseReleased.await(5, TimeUnit.SECONDS))
                .as("503 response body released before the retry")
                .isTrue();
    }
}
