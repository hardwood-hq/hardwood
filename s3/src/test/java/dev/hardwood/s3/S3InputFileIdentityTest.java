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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.hardwood.HardwoodContext;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.StaleMetadataException;
import dev.hardwood.reader.StaleMetadataException.Check;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An S3 object's identity is the `ETag` of the response to the request that opens it, checked
/// against a local endpoint that sends one with every response.
class S3InputFileIdentityTest {

    private static final Path OBJECT = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources/plain_uncompressed.parquet").normalize();
    private static final String URI = "s3://bucket/object.parquet";

    private volatile byte[] object;
    private volatile String etag;
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

    /// Answers the suffix range that opens a file, which covers the whole of this object.
    private void respond(HttpExchange exchange) throws IOException {
        byte[] body = object;
        if (etag != null) {
            exchange.getResponseHeaders().add("ETag", etag);
        }
        exchange.getResponseHeaders().add("Content-Range",
                "bytes 0-" + (body.length - 1) + "/" + body.length);
        exchange.sendResponseHeaders(206, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @ParameterizedTest
    @EnumSource(RangeBacking.class)
    void theIdentityIsTheEtagOfTheOpeningRequest(RangeBacking rangeBacking) throws IOException {
        try (S3Source backed = S3Source.builder()
                .endpoint("http://localhost:" + server.getAddress().getPort())
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .rangeBacking(rangeBacking)
                .build();
                S3InputFile file = backed.inputFile(URI)) {
            file.open();
            etag = "\"v2\"";

            assertThat(file.identity()).contains("\"v1\"");
            assertThat(file.networkRequestCount()).isEqualTo(1);
        }
    }

    @Test
    void anObjectServedWithoutAnEtagHasNoIdentity() throws IOException {
        etag = null;
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();

            assertThat(file.identity()).isEmpty();
        }
    }

    @Test
    void theIdentityIsResolvedByOpen() throws IOException {
        try (S3InputFile file = source.inputFile(URI)) {
            assertThatThrownBy(file::identity)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("File not opened: " + URI);
        }
    }

    @Test
    void aReplacedObjectFailsTheIdentityCheck() throws IOException {
        ParsedFooter footer;
        try (S3InputFile file = source.inputFile(URI)) {
            file.open();
            footer = ParsedFooter.readFrom(file);
        }

        // The same length and footer: only the identity tells the two apart.
        byte[] replaced = object.clone();
        replaced[4] ^= 1;
        object = replaced;
        etag = "\"v2\"";

        try (HardwoodContext context = HardwoodContext.builder().metadataSource(file -> footer).build()) {
            assertThatThrownBy(() -> ParquetFileReader.open(source.inputFile(URI), context))
                    .isInstanceOfSatisfying(StaleMetadataException.class,
                            e -> assertThat(e.check()).isEqualTo(Check.IDENTITY))
                    .hasMessage("[" + URI + "] Footer from the MetadataSource does not describe the file:"
                            + " it was read from content with identity '\"v1\"', the file has identity '\"v2\"'");
        }
    }
}
