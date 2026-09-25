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
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.internal.S3Api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Checks how [S3Fetcher] treats the HTTP status of its range GETs, against
/// a local endpoint that ignores the `Range` header and answers `200` with
/// the whole object. Unless told to ignore it as well, the endpoint answers
/// the open-time suffix range with `206`.
class S3FetcherStatusTest {

    private static final int OBJECT_SIZE = 200 * 1024;

    private final byte[] object = new byte[OBJECT_SIZE];
    private volatile boolean ignoreSuffixRange;
    private HttpServer server;
    private HttpClient httpClient;
    private S3Fetcher fetcher;

    @BeforeEach
    void setUp() throws IOException {
        for (int i = 0; i < object.length; i++) {
            object[i] = (byte) i;
        }
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.start();

        httpClient = HttpClient.newHttpClient();
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        S3Credentials credentials = S3Credentials.of("access", "secret");
        S3Api api = new S3Api(httpClient, () -> credentials,
                "us-east-1", endpoint, true, Duration.ofSeconds(10), 0);
        fetcher = new S3Fetcher(api, "bucket", "object.parquet");
    }

    @AfterEach
    void tearDown() {
        httpClient.close();
        server.stop(0);
    }

    private void respond(HttpExchange exchange) throws IOException {
        String range = exchange.getRequestHeaders().getFirst("Range");
        int from = 0;
        int status = 200;
        if (range != null && range.startsWith("bytes=-") && !ignoreSuffixRange) {
            from = OBJECT_SIZE - Integer.parseInt(range.substring("bytes=-".length()));
            status = 206;
            exchange.getResponseHeaders().add("Content-Range",
                    "bytes " + from + "-" + (OBJECT_SIZE - 1) + "/" + OBJECT_SIZE);
        }
        exchange.sendResponseHeaders(status, OBJECT_SIZE - from);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(object, from, OBJECT_SIZE - from);
        }
    }

    @Test
    void rangeReadRejectsResponseIgnoringRange() throws IOException {
        fetcher.open();
        long requestsAfterOpen = fetcher.networkRequestCount();

        assertThatThrownBy(() -> fetcher.readRange(100_000, 100))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to read range [100000, 100100) from s3://bucket/object.parquet:"
                        + " HTTP 200, expected 206; the endpoint ignored the Range header");
        assertThat(fetcher.networkRequestCount()).isEqualTo(requestsAfterOpen + 1);
    }

    @Test
    void openAcceptsWholeObjectAnswer() throws IOException {
        ignoreSuffixRange = true;
        fetcher.open();
        long requestsAfterOpen = fetcher.networkRequestCount();

        assertThat(fetcher.length()).isEqualTo(OBJECT_SIZE);
        ByteBuffer range = fetcher.readRange(100_000, 100);
        byte[] actual = new byte[100];
        range.get(actual);
        assertThat(actual).isEqualTo(Arrays.copyOfRange(object, 100_000, 100_100));
        assertThat(fetcher.networkRequestCount()).isEqualTo(requestsAfterOpen);
    }
}
