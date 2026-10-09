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
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/// Forwards real S3 operations, with faults only at selected request/response boundaries.
final class S3UploadFaultProxy implements AutoCloseable {

    final List<Request> requests = new CopyOnWriteArrayList<>();
    volatile int rejectedPart;
    volatile boolean losePublication;
    volatile boolean denyHead;
    volatile boolean rejectAbort;
    volatile boolean replaceOnHead;
    private final S3UploadTestClient backend;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    S3UploadFaultProxy(String endpoint) throws IOException {
        backend = new S3UploadTestClient(endpoint);
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(executor);
        server.start();
    }

    String endpoint() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getRawQuery();
        byte[] body = exchange.getRequestBody().readAllBytes();
        requests.add(new Request(method, query, body.length));
        if ((rejectedPart > 0 && "PUT".equals(method) && query != null
                && query.contains("partNumber=" + rejectedPart + "&"))
                || (rejectAbort && "DELETE".equals(method)) || (denyHead && "HEAD".equals(method))) {
            respond(exchange, 403, "<Error><Code>AccessDenied</Code></Error>".getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (replaceOnHead && "HEAD".equals(method)) {
            replaceOnHead = false;
            backend.request("PUT", exchange.getRequestURI().getRawPath(), new byte[] { 9, 8, 7 },
                    Map.of("x-amz-meta-hardwood-write-id", "another-writer"));
        }
        Map<String, String> headers = new LinkedHashMap<>();
        exchange.getRequestHeaders().forEach((name, values) -> {
            if (name.equalsIgnoreCase("Range") || name.equalsIgnoreCase("Content-Type")
                    || name.toLowerCase(Locale.ROOT).startsWith("x-amz-meta-")) {
                headers.put(name, String.join(",", values));
            }
        });
        HttpResponse<byte[]> response = backend.request(method, exchange.getRequestURI().toASCIIString(), body, headers);
        response.headers().map().forEach((name, values) -> {
            if (!List.of("content-length", "transfer-encoding", "connection").contains(name)) {
                exchange.getResponseHeaders().put(name, values);
            }
        });
        if ("HEAD".equals(method)) {
            response.headers().firstValue("Content-Length").ifPresent(value -> exchange.getResponseHeaders().set("Content-Length", value));
            respond(exchange, response.statusCode(), new byte[0]);
        }
        else if (losePublication && response.statusCode() == 200 && publication(method, query)) {
            // The backend has published. A truncated response forces an actual transport failure.
            exchange.sendResponseHeaders(200, response.body().length + 16L);
            try (OutputStream stream = exchange.getResponseBody()) {
                stream.write(response.body());
            }
            catch (IOException ignored) {
                exchange.close();
            }
        }
        else {
            respond(exchange, response.statusCode(), response.body());
        }
    }

    long publicationCount() {
        return requests.stream().filter(request -> publication(request.method(), request.query())).count();
    }

    private static boolean publication(String method, String query) {
        return ("PUT".equals(method) && query == null)
                || ("POST".equals(method) && query != null && query.startsWith("uploadId="));
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 || "HEAD".equals(exchange.getRequestMethod()) ? -1 : body.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            if (!"HEAD".equals(exchange.getRequestMethod())) {
                stream.write(body);
            }
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
        backend.close();
    }

    record Request(String method, String query, int bytes) {
    }
}
