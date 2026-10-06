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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3UploadTestClientTest {

    private final List<String> queries = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private S3UploadTestClient client;
    private volatile boolean repeatedMarker;

    @BeforeEach
    void setup() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::respond);
        server.start();
        client = new S3UploadTestClient("http://localhost:" + server.getAddress().getPort());
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.stop(0);
    }

    @Test
    void followsPaginationWithEncodedMarkersAndFiltersExactKey() throws Exception {
        assertThat(client.uploads("bucket", "target"))
                .containsExactly(new S3UploadTestClient.Upload("target", "owned"));
        assertThat(queries).containsExactly("uploads=", "uploads=&key-marker=a%20%2F%3F%2B&upload-id-marker=id%2B%2F");
    }

    @Test
    void repeatedMarkersFailRatherThanReturningIncompleteObservation() {
        repeatedMarker = true;
        assertThatThrownBy(() -> client.uploads("bucket", "target"))
                .isInstanceOf(IOException.class).hasMessageContaining("markers do not advance");
        assertThat(queries).hasSize(2);
    }

    private void respond(HttpExchange exchange) throws IOException {
        queries.add(exchange.getRequestURI().getRawQuery());
        boolean next = queries.size() > 1;
        String upload = next
                ? "<Upload><Key>target</Key><UploadId>owned</UploadId></Upload>"
                : "<Upload><Key>target-suffix</Key><UploadId>other</UploadId></Upload>";
        String body = "<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + upload + "<IsTruncated>" + (!next || repeatedMarker) + "</IsTruncated>"
                + "<NextKeyMarker>a /?+</NextKeyMarker><NextUploadIdMarker>id+/</NextUploadIdMarker>"
                + "</ListMultipartUploadsResult>";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(bytes);
        }
    }
}
