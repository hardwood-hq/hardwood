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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.S3Credentials;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks that the `host` [S3Api] signs is the `Host` header the JDK sends.
/// A local server, reached as an HTTP proxy so that any endpoint host and
/// port lands on it, recomputes the signature from the headers it received,
/// as S3 does, and answers `403` on a mismatch. HTTP/2 sends a request
/// URI's port in `:authority` even when it is the default, so request URIs
/// must not carry a default port at all.
class S3ApiHostSigningTest {

    private static final String ACCESS_KEY = "access";
    private static final String SECRET_KEY = "secret";
    private static final String REGION = "us-east-1";
    private static final byte[] RANGE_BODY = { 1, 2, 3, 4 };

    private ExecutorService executor;
    private HttpServer server;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws IOException {
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.setExecutor(executor);
        server.start();

        httpClient = HttpClient.newBuilder()
                .proxy(ProxySelector.of(server.getAddress()))
                .build();
    }

    @AfterEach
    void tearDown() {
        httpClient.shutdownNow();
        server.stop(0);
        executor.shutdownNow();
    }

    private void respond(HttpExchange exchange) throws IOException {
        Headers received = exchange.getRequestHeaders();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Host", received.getFirst("Host"));
        headers.put("x-amz-content-sha256", received.getFirst("x-amz-content-sha256"));
        headers.put("Range", received.getFirst("Range"));
        ZonedDateTime date = LocalDateTime.parse(received.getFirst("x-amz-date"), Aws4Signer.TIMESTAMP_FORMAT)
                .atZone(ZoneOffset.UTC);
        String expected = Aws4Signer.sign("GET", exchange.getRequestURI(), headers,
                received.getFirst("x-amz-content-sha256"), ACCESS_KEY, SECRET_KEY, null,
                REGION, "s3", date).authorizationHeader();

        if (!expected.equals(received.getFirst("Authorization"))) {
            byte[] error = "<Code>SignatureDoesNotMatch</Code>".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(403, error.length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(error);
            }
            return;
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes 0-3/4");
        exchange.sendResponseHeaders(206, RANGE_BODY.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(RANGE_BODY);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "http://s3.test", "http://s3.test:80", "http://s3.test:9000" })
    void signedHostMatchesSentHost(String endpoint) throws IOException {
        S3Api api = new S3Api(httpClient, () -> S3Credentials.of(ACCESS_KEY, SECRET_KEY),
                REGION, URI.create(endpoint), true, Duration.ofSeconds(10), 0);

        HttpResponse<byte[]> response = api.getBytes("bucket", "object.parquet", "bytes=0-3");

        assertThat(response.statusCode()).isEqualTo(206);
        assertThat(response.body()).isEqualTo(RANGE_BODY);
    }

    @ParameterizedTest
    @CsvSource({
            "http://s3.test:80, true, http://s3.test/bucket/object.parquet",
            "HTTP://s3.test:80/, true, HTTP://s3.test/bucket/object.parquet",
            "https://s3.test:443, true, https://s3.test/bucket/object.parquet",
            "https://s3.test:80, true, https://s3.test:80/bucket/object.parquet",
            "http://s3.test:9000, true, http://s3.test:9000/bucket/object.parquet",
            "http://s3.test:80, false, http://bucket.s3.test/object.parquet",
            "https://s3.test:443, false, https://bucket.s3.test/object.parquet",
            "http://s3.test:9000, false, http://bucket.s3.test:9000/object.parquet"
    })
    void requestUriOmitsDefaultPort(String endpoint, boolean pathStyle, String expected) {
        S3Api api = new S3Api(httpClient, () -> S3Credentials.of(ACCESS_KEY, SECRET_KEY),
                REGION, URI.create(endpoint), pathStyle, Duration.ofSeconds(10), 0);

        assertThat(api.objectUri("bucket", "object.parquet")).isEqualTo(URI.create(expected));
    }
}
