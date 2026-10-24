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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.s3.internal.IdleTimer;
import dev.hardwood.s3.internal.S3Api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Checks that [S3Fetcher] fails a response body that stops delivering bytes, against a local
/// endpoint that sends the headers and the first half of a body, then stalls until the test ends.
/// The endpoint stalls the open-time suffix range while [#stallOpen] is set, and every other
/// range otherwise. The idle checks run once the endpoint has stalled, each as soon as it is
/// scheduled, so a check that finds no bytes since the previous one fails the body without
/// waiting for the timeout to pass.
class S3FetcherStallTest {

    private static final int OBJECT_SIZE = 1024 * 1024;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final byte[] object = new byte[OBJECT_SIZE];
    private final CountDownLatch stalled = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private volatile boolean stallOpen;
    private ExecutorService executor;
    private HttpServer server;
    private HttpClient httpClient;
    private S3Fetcher fetcher;

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
        IdleTimer onceStalled = (check, delay) -> executor.execute(() -> {
            awaitUninterruptibly(stalled);
            check.run();
        });
        S3Api api = new S3Api(httpClient, () -> credentials, "us-east-1", endpoint, true, TIMEOUT, 0, onceStalled);
        fetcher = new S3Fetcher(api, "bucket", "object.parquet");
    }

    @AfterEach
    void tearDown() {
        released.countDown();
        httpClient.shutdownNow();
        server.stop(0);
        executor.shutdownNow();
    }

    private void respond(HttpExchange exchange) throws IOException {
        String range = exchange.getRequestHeaders().getFirst("Range").substring("bytes=".length());
        boolean suffix = range.startsWith("-");
        int from;
        int to;
        if (suffix) {
            from = OBJECT_SIZE - Integer.parseInt(range.substring(1));
            to = OBJECT_SIZE - 1;
        }
        else {
            int dash = range.indexOf('-');
            from = Integer.parseInt(range.substring(0, dash));
            to = Integer.parseInt(range.substring(dash + 1));
        }
        int length = to - from + 1;
        exchange.getResponseHeaders().add("Content-Range", "bytes " + from + "-" + to + "/" + OBJECT_SIZE);
        exchange.sendResponseHeaders(206, length);
        OutputStream body = exchange.getResponseBody();
        if (suffix == stallOpen) {
            body.write(object, from, length / 2);
            body.flush();
            stalled.countDown();
            awaitUninterruptibly(released);
            return;
        }
        body.write(object, from, length);
        body.close();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void stalledRangeBodyFailsRead() throws IOException {
        fetcher.open();

        assertThatThrownBy(() -> fetcher.readRange(100_000, 1000))
                .isInstanceOf(IOException.class)
                .hasMessage("GET s3://bucket/object.parquet range bytes=100000-100999 timed out:"
                        + " no body bytes for 30000 ms");
    }

    @Test
    void stalledOpenBodyFailsOpen() {
        stallOpen = true;

        assertThatThrownBy(fetcher::open)
                .isInstanceOf(IOException.class)
                .hasMessage("GET s3://bucket/object.parquet range bytes=-" + TailedInputFile.tailBytes() + " timed out:"
                        + " no body bytes for 30000 ms");
    }
}
