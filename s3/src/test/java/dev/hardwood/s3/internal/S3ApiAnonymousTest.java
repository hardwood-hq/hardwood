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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks that an anonymous [S3Api] sends its requests unsigned, retries included,
/// against a local endpoint that answers the first request with a `503`.
class S3ApiAnonymousTest {

    private static final byte[] RANGE_BODY = { 1, 2, 3, 4 };

    private final List<Map<String, List<String>>> requestHeaders = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private HttpClient httpClient;
    private S3Api api;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.start();

        httpClient = HttpClient.newHttpClient();
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        api = new S3Api(httpClient, null, "us-east-1", endpoint, true, Duration.ofSeconds(10), 1);
    }

    @AfterEach
    void tearDown() {
        httpClient.close();
        server.stop(0);
    }

    private void respond(HttpExchange exchange) throws IOException {
        requestHeaders.add(Map.copyOf(exchange.getRequestHeaders()));
        if (requestHeaders.size() == 1) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
            return;
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes 0-3/4");
        exchange.sendResponseHeaders(206, RANGE_BODY.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(RANGE_BODY);
        }
    }

    @Test
    void requestsAreUnsigned() throws IOException {
        HttpResponse<RangeBody> response = api.getRange("bucket", "object.parquet", 0,
                RANGE_BODY.length, null);

        assertThat(api.anonymous()).isTrue();
        assertThat(response.statusCode()).isEqualTo(206);
        assertThat(response.body().bytes()).isEqualTo(ByteBuffer.wrap(RANGE_BODY));
        assertThat(requestHeaders).hasSize(2);
        for (Map<String, List<String>> headers : requestHeaders) {
            List<String> names = headers.keySet().stream().map(name -> name.toLowerCase(Locale.ROOT)).toList();
            assertThat(names).doesNotContain("authorization");
            assertThat(names).noneMatch(name -> name.startsWith("x-amz-"));
            assertThat(headers.get("Range")).containsExactly("bytes=0-3");
        }
    }
}
