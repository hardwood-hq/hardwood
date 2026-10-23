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
import java.nio.charset.StandardCharsets;
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
/// the open-time suffix range with `206`. It denies `private.parquet` with `403`, and
/// `tail-only.parquet` likewise except for the suffix range.
class S3FetcherStatusTest {

    private static final int OBJECT_SIZE = 200 * 1024;
    private static final String ACCESS_DENIED = "<Error><Code>AccessDenied</Code></Error>";

    private final byte[] object = new byte[OBJECT_SIZE];
    private volatile boolean ignoreSuffixRange;
    private HttpServer server;
    private HttpClient httpClient;
    private S3Api signedApi;
    private S3Api anonymousApi;
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
        signedApi = new S3Api(httpClient, () -> credentials,
                "us-east-1", endpoint, true, Duration.ofSeconds(10), 0);
        anonymousApi = new S3Api(httpClient, null,
                "us-east-1", endpoint, true, Duration.ofSeconds(10), 0);
        // A tail smaller than the object, so a range read outside it goes to the endpoint
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(64 * 1024));
        fetcher = new S3Fetcher(signedApi, "bucket", "object.parquet");
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(S3Fetcher.TAIL_BYTES_PROPERTY);
        httpClient.close();
        server.stop(0);
    }

    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        boolean suffixRange = exchange.getRequestHeaders().getFirst("Range").startsWith("bytes=-");
        if (path.endsWith("/private.parquet") || (path.endsWith("/tail-only.parquet") && !suffixRange)) {
            byte[] denied = ACCESS_DENIED.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(403, denied.length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(denied);
            }
            return;
        }
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
    void deniedAnonymousOpenNamesUnsignedRequest() throws IOException {
        S3Fetcher anonymousFetcher = new S3Fetcher(anonymousApi, "bucket", "private.parquet");

        assertThatThrownBy(anonymousFetcher::open)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to open s3://bucket/private.parquet: HTTP 403 " + ACCESS_DENIED
                        + " (the request was sent unsigned, without credentials)");
    }

    @Test
    void deniedAnonymousRangeReadNamesUnsignedRequest() throws IOException {
        S3Fetcher anonymousFetcher = new S3Fetcher(anonymousApi, "bucket", "tail-only.parquet");
        anonymousFetcher.open();

        assertThatThrownBy(() -> anonymousFetcher.readRange(100, 100))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to read range [100, 200) from s3://bucket/tail-only.parquet: HTTP 403 "
                        + ACCESS_DENIED + " (the request was sent unsigned, without credentials)");
    }

    @Test
    void deniedSignedOpenHasNoUnsignedHint() {
        S3Fetcher signedFetcher = new S3Fetcher(signedApi, "bucket", "private.parquet");

        assertThatThrownBy(signedFetcher::open)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to open s3://bucket/private.parquet: HTTP 403 " + ACCESS_DENIED);
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
