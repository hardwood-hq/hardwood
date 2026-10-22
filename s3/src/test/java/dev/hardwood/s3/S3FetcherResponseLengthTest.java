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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.internal.S3Api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Checks that [S3Fetcher] reads each range response to its end, against a local endpoint
/// that answers a range with `extra` bytes more than it asked for and records the client
/// port of every request. A response closed before its end is cancelled, and the client
/// closes its connection instead of returning it to the pool.
class S3FetcherResponseLengthTest {

    private static final int OBJECT_SIZE = 4 * 1024 * 1024;
    private static final int RANGE_LENGTH = 128 * 1024;
    private static final int THREADS = 8;
    private static final int READS_PER_THREAD = 100;

    private final byte[] object = new byte[OBJECT_SIZE];
    private final Set<Integer> clientPorts = ConcurrentHashMap.newKeySet();
    private volatile int extra;
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
        server.setExecutor(Executors.newFixedThreadPool(THREADS));
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
        clientPorts.add(exchange.getRemoteAddress().getPort());
        String range = exchange.getRequestHeaders().getFirst("Range").substring("bytes=".length());
        int from;
        int to;
        if (range.startsWith("-")) {
            from = OBJECT_SIZE - Integer.parseInt(range.substring(1));
            to = OBJECT_SIZE - 1;
        }
        else {
            int dash = range.indexOf('-');
            from = Integer.parseInt(range.substring(0, dash));
            to = Math.min(Integer.parseInt(range.substring(dash + 1)) + extra, OBJECT_SIZE - 1);
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes " + from + "-" + to + "/" + OBJECT_SIZE);
        exchange.sendResponseHeaders(206, to - from + 1);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(object, from, to - from + 1);
        }
    }

    @Test
    void rangeReadReturnsExactResponse() throws IOException {
        fetcher.open();

        ByteBuffer range = fetcher.readRange(1000, 100);

        assertThat(range.remaining()).isEqualTo(100);
        assertThat(range.get(99)).isEqualTo(object[1099]);
    }

    @Test
    void concurrentRangeReadsReuseConnections() throws Exception {
        fetcher.open();

        try (ExecutorService readers = Executors.newFixedThreadPool(THREADS)) {
            List<Future<?>> results = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                int thread = t;
                results.add(readers.submit(() -> {
                    for (int i = 0; i < READS_PER_THREAD; i++) {
                        int offset = ((thread * READS_PER_THREAD + i) * 1024) % (OBJECT_SIZE - RANGE_LENGTH);
                        fetcher.readRange(offset, RANGE_LENGTH);
                    }
                    return null;
                }));
            }
            for (Future<?> result : results) {
                result.get();
            }
        }

        assertThat(clientPorts).hasSizeLessThanOrEqualTo(THREADS);
    }

    @Test
    void rangeReadRejectsLongerResponse() throws IOException {
        fetcher.open();
        extra = 1;

        assertThatThrownBy(() -> fetcher.readRange(1000, 100))
                .isInstanceOf(IOException.class)
                .hasMessage("Long read from s3://bucket/object.parquet: expected 100 bytes but received more");
    }
}
