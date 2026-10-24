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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.s3.internal.IdleTimer;
import dev.hardwood.s3.internal.S3Api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Checks that [S3Fetcher] resumes a range read whose body fails midway, against a local endpoint
/// that answers range request `n` as entry `n - 1` of [#answers] says, and with the whole range
/// once the list runs out. Range requests after the first carry [#resumedEtag] and the bytes of
/// [#resumedObject], and the endpoint ignores `If-Match`; [#etag] is `null` for an endpoint that
/// sends no `ETag`. A stall comes before any body byte, so the bytes received are known whenever a
/// check runs. The idle checks of the first range request run once it has stalled, each as soon
/// as it is scheduled; those of every later request never run.
class S3FetcherResumeTest {

    /// How the endpoint answers one range request.
    enum Answer {
        /// The whole range.
        WHOLE,
        /// Half of the bytes asked for, then the connection closes.
        HALF_THEN_CLOSE,
        /// Every byte asked for in a chunked body, then the connection closes before its last chunk.
        ALL_THEN_CLOSE,
        /// The headers of a `206`, then nothing until the test ends.
        STALL,
        /// A `503`.
        UNAVAILABLE
    }

    private static final int OBJECT_SIZE = 1024 * 1024;
    private static final int MAX_RETRIES = 3;
    private static final String NAME = "s3://bucket/object.parquet";

    private final byte[] object = new byte[OBJECT_SIZE];
    private final byte[] replaced = new byte[OBJECT_SIZE];
    private final List<String> ranges = new CopyOnWriteArrayList<>();
    private final List<String> ifMatches = new CopyOnWriteArrayList<>();
    private final AtomicInteger rangeRequests = new AtomicInteger();
    private final CountDownLatch stalled = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private volatile List<Answer> answers = List.of();
    private volatile String etag = "\"v1\"";
    private volatile String resumedEtag = "\"v1\"";
    private volatile byte[] resumedObject = object;
    private ExecutorService executor;
    private HttpServer server;
    private HttpClient httpClient;
    private S3Fetcher fetcher;

    @BeforeEach
    void setUp() throws IOException {
        for (int i = 0; i < object.length; i++) {
            object[i] = (byte) (i * 31);
            replaced[i] = (byte) (i * 7);
        }
        executor = Executors.newCachedThreadPool();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.setExecutor(executor);
        server.start();

        httpClient = HttpClient.newHttpClient();
        URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort());
        S3Credentials credentials = S3Credentials.of("access", "secret");
        IdleTimer firstRequestOnly = (check, delay) -> {
            if (rangeRequests.get() <= 1) {
                executor.execute(() -> {
                    awaitUninterruptibly(stalled);
                    check.run();
                });
            }
        };
        S3Api api = new S3Api(httpClient, () -> credentials, "us-east-1", endpoint, true,
                Duration.ofSeconds(30), MAX_RETRIES, firstRequestOnly);
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
        if (range.startsWith("-")) {
            int from = OBJECT_SIZE - Integer.parseInt(range.substring(1));
            sendHeaders(exchange, etag, from, OBJECT_SIZE - 1, OBJECT_SIZE - from);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(object, from, OBJECT_SIZE - from);
            }
            return;
        }
        int attempt = rangeRequests.incrementAndGet();
        ranges.add(range);
        ifMatches.add(String.valueOf(exchange.getRequestHeaders().getFirst("If-Match")));
        int dash = range.indexOf('-');
        int from = Integer.parseInt(range.substring(0, dash));
        int to = Integer.parseInt(range.substring(dash + 1));
        int length = to - from + 1;
        byte[] current = attempt == 1 ? object : resumedObject;
        String currentEtag = attempt == 1 ? etag : resumedEtag;
        List<Answer> planned = answers;
        Answer answer = attempt <= planned.size() ? planned.get(attempt - 1) : Answer.WHOLE;
        if (answer == Answer.UNAVAILABLE) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
            return;
        }
        // A chunked body, announced by a length of 0, has no length the client could check
        sendHeaders(exchange, currentEtag, from, to, answer == Answer.ALL_THEN_CLOSE ? 0 : length);
        OutputStream body = exchange.getResponseBody();
        switch (answer) {
            case WHOLE -> {
                body.write(current, from, length);
                body.close();
            }
            case HALF_THEN_CLOSE -> {
                body.write(current, from, length / 2);
                body.flush();
                // Fails for the bytes missing; the server closes the connection of a handler that throws
                body.close();
            }
            case ALL_THEN_CLOSE -> {
                body.write(current, from, length);
                body.flush();
                // Ends the exchange without the last chunk, closing the connection
                throw new IOException("abandoned after the last byte");
            }
            case STALL -> {
                // Sends the headers, which the server holds back until the first write or flush
                body.flush();
                stalled.countDown();
                awaitUninterruptibly(released);
            }
            default -> throw new IllegalStateException(answer.name());
        }
    }

    private static void sendHeaders(HttpExchange exchange, String etag, int from, int to, int length)
            throws IOException {
        if (etag != null) {
            exchange.getResponseHeaders().add("ETag", etag);
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes " + from + "-" + to + "/" + OBJECT_SIZE);
        exchange.sendResponseHeaders(206, length);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void assertRange(ByteBuffer range, int offset, int length) {
        byte[] actual = new byte[range.remaining()];
        range.get(actual);
        assertThat(actual).isEqualTo(Arrays.copyOfRange(object, offset, offset + length));
    }

    @Test
    void bodyClosedMidwayIsResumed() throws IOException {
        fetcher.open();
        answers = List.of(Answer.HALF_THEN_CLOSE);

        ByteBuffer range = fetcher.readRange(100_000, 1000);

        assertThat(range.isDirect()).isTrue();
        assertRange(range, 100_000, 1000);
        assertThat(ranges).containsExactly("100000-100999", "100500-100999");
        assertThat(ifMatches).containsExactly("\"v1\"", "\"v1\"");
        assertThat(fetcher.networkRequestCount()).isEqualTo(2);
    }

    @Test
    void stalledBodyIsResumed() throws IOException {
        fetcher.open();
        answers = List.of(Answer.STALL);

        ByteBuffer range = fetcher.readRange(100_000, 1000);

        assertRange(range, 100_000, 1000);
        assertThat(ranges).containsExactly("100000-100999", "100000-100999");
    }

    @Test
    void resumedBodyOfAnotherVersionFailsRead() throws IOException {
        fetcher.open();
        answers = List.of(Answer.HALF_THEN_CLOSE);
        resumedEtag = "\"v2\"";

        assertThatThrownBy(() -> fetcher.readRange(100_000, 1000))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Failed to read range [100000, 101000) from " + NAME
                        + ": the file changed since it was opened (ETag \"v2\" for If-Match \"v1\")");
    }

    @Test
    void bodyFailingOnEveryAttemptExhaustsRetries() throws IOException {
        fetcher.open();
        answers = List.of(Answer.HALF_THEN_CLOSE, Answer.HALF_THEN_CLOSE, Answer.HALF_THEN_CLOSE,
                Answer.HALF_THEN_CLOSE);

        assertThatThrownBy(() -> fetcher.readRange(100_000, 1000))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to read range [100000, 101000) from " + NAME
                        + ": received 937 of 1000 bytes in 4 attempts");
        assertThat(ranges).containsExactly("100000-100999", "100500-100999", "100750-100999", "100875-100999");
    }

    @Test
    void rangeFullBeforeItsBodyEndsResumesAtItsLastByte() throws IOException {
        fetcher.open();
        answers = List.of(Answer.ALL_THEN_CLOSE);

        ByteBuffer range = fetcher.readRange(100_000, 1000);

        assertRange(range, 100_000, 1000);
        assertThat(ranges).containsExactly("100000-100999", "100999-100999");
    }

    @Test
    void unavailableAnswerBetweenAttemptsKeepsTheBytesReceived() throws IOException {
        fetcher.open();
        answers = List.of(Answer.HALF_THEN_CLOSE, Answer.UNAVAILABLE);

        ByteBuffer range = fetcher.readRange(100_000, 1000);

        assertRange(range, 100_000, 1000);
        assertThat(ranges).containsExactly("100000-100999", "100500-100999", "100500-100999");
    }

    @Test
    void rangeWithoutEtagStartsOverOnRetry() throws IOException {
        etag = null;
        resumedEtag = null;
        resumedObject = replaced;
        fetcher.open();
        answers = List.of(Answer.HALF_THEN_CLOSE);

        ByteBuffer range = fetcher.readRange(100_000, 1000);

        byte[] actual = new byte[range.remaining()];
        range.get(actual);
        assertThat(actual).isEqualTo(Arrays.copyOfRange(replaced, 100_000, 101_000));
        assertThat(ranges).containsExactly("100000-100999", "100000-100999");
        assertThat(ifMatches).containsExactly("null", "null");
    }
}
