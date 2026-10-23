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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.s3.internal.S3Api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The tail [S3Fetcher#open()] fetches (#1399): its size is the suffix of the open's request, a
/// file no longer than it is fetched whole, an open for a supplied footer fetches at most
/// [S3Fetcher#SUPPLIED_FOOTER_TAIL_SIZE], and a released tail serves no further read. Against a
/// local endpoint that answers a suffix longer than the object with the whole object, as S3 does,
/// and records the `Range` header of every request.
class S3FetcherTailTest {

    private static final int OBJECT_SIZE = 200 * 1024;

    private final byte[] object = new byte[OBJECT_SIZE];
    private final List<String> ranges = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private HttpClient httpClient;
    private S3Api api;

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
        api = new S3Api(httpClient, () -> credentials, "us-east-1", endpoint, true, Duration.ofSeconds(10), 0);
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(S3Fetcher.TAIL_BYTES_PROPERTY);
        httpClient.close();
        server.stop(0);
    }

    private void respond(HttpExchange exchange) throws IOException {
        String range = exchange.getRequestHeaders().getFirst("Range");
        ranges.add(range);
        String spec = range.substring("bytes=".length());
        int from;
        int to;
        if (spec.startsWith("-")) {
            from = Math.max(0, OBJECT_SIZE - Integer.parseInt(spec.substring(1)));
            to = OBJECT_SIZE - 1;
        }
        else {
            int dash = spec.indexOf('-');
            from = Integer.parseInt(spec.substring(0, dash));
            to = Integer.parseInt(spec.substring(dash + 1));
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes " + from + "-" + to + "/" + OBJECT_SIZE);
        exchange.sendResponseHeaders(206, to - from + 1);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(object, from, to - from + 1);
        }
    }

    @Test
    void anObjectNoLongerThanTheTailIsFetchedWholeByTheOpen() throws IOException {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(256 * 1024));
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");

        fetcher.open();
        ByteBuffer first = fetcher.readRange(0, 1000);
        ByteBuffer whole = fetcher.readRange(0, OBJECT_SIZE);

        assertThat(ranges).containsExactly("bytes=-262144");
        assertThat(fetcher.length()).isEqualTo(OBJECT_SIZE);
        assertThat(fetcher.tailBytes()).isEqualTo(OBJECT_SIZE);
        assertThat(first.get(999)).isEqualTo(object[999]);
        assertThat(whole.get(OBJECT_SIZE - 1)).isEqualTo(object[OBJECT_SIZE - 1]);
        assertThat(fetcher.networkRequestCount()).isEqualTo(1);
        assertThat(fetcher.networkBytesFetched()).isEqualTo(OBJECT_SIZE);
    }

    @Test
    void aLongerObjectHasItsLastBytesFetchedByTheOpen() throws IOException {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(64 * 1024));
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");

        fetcher.open();
        fetcher.readRange(OBJECT_SIZE - 1000, 1000);
        fetcher.readRange(0, 1000);

        assertThat(ranges).containsExactly("bytes=-65536", "bytes=0-999");
        assertThat(fetcher.tailBytes()).isEqualTo(64 * 1024);
    }

    @Test
    void aRangeEndingInsideTheTailFetchesOnlyTheBytesBeforeIt() throws IOException {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(64 * 1024));
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");
        fetcher.open();
        int tailStart = OBJECT_SIZE - 64 * 1024;

        ByteBuffer range = fetcher.readRange(tailStart - 1000, 3000);

        assertThat(ranges).containsExactly("bytes=-65536", "bytes=" + (tailStart - 1000) + "-" + (tailStart - 1));
        assertThat(fetcher.networkBytesFetched()).isEqualTo(64 * 1024 + 1000);
        assertThat(range.remaining()).isEqualTo(3000);
        for (int i = 0; i < 3000; i++) {
            assertThat(range.get(i)).as("byte %d", i).isEqualTo(object[tailStart - 1000 + i]);
        }
    }

    @Test
    void anOpenForASuppliedFooterFetchesTheSmallerTail() throws IOException {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(1024 * 1024));
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");
        assertThat(fetcher.tailReservation(false)).isEqualTo(1024 * 1024);
        assertThat(fetcher.tailReservation(true)).isEqualTo(S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE);

        fetcher.open(true);

        assertThat(ranges).containsExactly("bytes=-" + S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE);
        assertThat(fetcher.tailBytes()).isEqualTo(S3Fetcher.SUPPLIED_FOOTER_TAIL_SIZE);
    }

    @Test
    void anOpenForASuppliedFooterFetchesNoMoreThanTheTail() throws IOException {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(16 * 1024));
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");

        fetcher.open(true);

        assertThat(ranges).containsExactly("bytes=-16384");
    }

    @Test
    void aReleasedTailServesNoFurtherRead() throws IOException {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, Integer.toString(256 * 1024));
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");
        fetcher.open();
        ByteBuffer before = fetcher.readRange(0, 1000);

        fetcher.releaseTail();
        ByteBuffer after = fetcher.readRange(0, 1000);

        assertThat(ranges).containsExactly("bytes=-262144", "bytes=0-999");
        assertThat(fetcher.tailBytes()).isZero();
        // A slice handed out before the release stays valid
        assertThat(before).isEqualTo(after);
    }

    @Test
    void closeReleasesTheTail() throws IOException {
        S3Fetcher fetcher = new S3Fetcher(api, "bucket", "object.parquet");
        fetcher.open();
        assertThat(fetcher.tailBytes()).isPositive();

        fetcher.close();

        assertThat(fetcher.tailBytes()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-1", "64k", "", "2147483648" })
    void aTailSizeThatIsNotAPositiveByteCountIsRejected(String value) {
        System.setProperty(S3Fetcher.TAIL_BYTES_PROPERTY, value);

        assertThatThrownBy(() -> new S3Fetcher(api, "bucket", "object.parquet"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.s3TailBytes must be a positive byte count of at most 2147483647,"
                        + " but was: " + value);
    }
}
