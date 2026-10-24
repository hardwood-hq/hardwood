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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Checks that a range read only returns bytes of the object version its file opened, against a
/// local endpoint that sends the current [#etag] with every response, answers a request whose
/// `If-Match` names another one with `412`, and records the `If-Match` of every request. With
/// [#rangePreconditionFailed] it answers every range request with `412`.
class S3FetcherObjectChangeTest {

    private static final Path OBJECT = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources/plain_uncompressed.parquet").normalize();
    private static final String URI = "s3://bucket/object.parquet";

    private final List<String> ifMatches = new CopyOnWriteArrayList<>();
    private volatile byte[] object;
    private volatile String etag;
    private volatile boolean rangePreconditionFailed;
    private HttpServer server;
    private S3Source source;

    @BeforeEach
    void setUp() throws IOException {
        object = Files.readAllBytes(OBJECT);
        etag = "\"v1\"";
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.start();
        source = S3Source.builder()
                .endpoint("http://localhost:" + server.getAddress().getPort())
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .build();
    }

    @AfterEach
    void tearDown() {
        source.close();
        server.stop(0);
    }

    private void respond(HttpExchange exchange) throws IOException {
        byte[] current = object;
        String currentEtag = etag;
        String ifMatch = exchange.getRequestHeaders().getFirst("If-Match");
        ifMatches.add(String.valueOf(ifMatch));
        boolean suffix = exchange.getRequestHeaders().getFirst("Range").startsWith("bytes=-");
        if ((ifMatch != null && !ifMatch.equals(currentEtag)) || (rangePreconditionFailed && !suffix)) {
            exchange.sendResponseHeaders(412, -1);
            exchange.close();
            return;
        }
        String range = exchange.getRequestHeaders().getFirst("Range").substring("bytes=".length());
        int from;
        int to;
        if (range.startsWith("-")) {
            from = Math.max(0, current.length - Integer.parseInt(range.substring(1)));
            to = current.length - 1;
        }
        else {
            int dash = range.indexOf('-');
            from = Integer.parseInt(range.substring(0, dash));
            to = Integer.parseInt(range.substring(dash + 1));
        }
        if (currentEtag != null) {
            exchange.getResponseHeaders().add("ETag", currentEtag);
        }
        exchange.getResponseHeaders().add("Content-Range", "bytes " + from + "-" + to + "/" + current.length);
        exchange.sendResponseHeaders(206, to - from + 1);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(current, from, to - from + 1);
        }
    }

    @Test
    void rangeReadsCarryTheEtagOfTheOpen() throws IOException {
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();
            file.readRange(4, 100);
        }

        assertThat(ifMatches).containsExactly("null", "\"v1\"");
    }

    @Test
    void objectReplacedSinceOpenFailsRangeRead() throws IOException {
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();
            etag = "\"v2\"";

            assertThatThrownBy(() -> file.readRange(4, 100))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Failed to read range [4, 104) from " + URI
                            + ": the file changed since it was opened (HTTP 412 for If-Match \"v1\")");
        }
    }

    @Test
    void objectReplacedMidReadFailsRead() throws IOException {
        S3InputFile file = source.inputFile(URI);
        file.open();
        byte[] replaced = object.clone();
        replaced[4] ^= 1;
        object = replaced;
        etag = "\"v2\"";

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[" + URI + "] Failed to read range [178, 714) from " + URI
                        + ": the file changed since it was opened (HTTP 412 for If-Match \"v1\")");
    }

    @Test
    void objectWithoutEtagIsReadWithoutCondition() throws IOException {
        etag = null;
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();
            file.readRange(4, 100);
        }

        assertThat(ifMatches).containsExactly("null", "null");
    }

    @Test
    void weakEtagIsReadWithoutCondition() throws IOException {
        etag = "W/\"v1\"";
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();
            file.readRange(4, 100);

            assertThat(file.identity()).contains("W/\"v1\"");
        }

        assertThat(ifMatches).containsExactly("null", "null");
    }

    @Test
    void preconditionFailedWithoutConditionIsAnHttpError() throws IOException {
        etag = null;
        rangePreconditionFailed = true;
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();

            assertThatThrownBy(() -> file.readRange(4, 100))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Failed to read range [4, 104) from " + URI + ": HTTP 412 ");
        }
    }
}
