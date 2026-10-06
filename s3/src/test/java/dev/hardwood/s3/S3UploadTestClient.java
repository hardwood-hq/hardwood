/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import dev.hardwood.s3.internal.Aws4Signer;

/// Signed requests used to observe real uploads independently of the output sink.
final class S3UploadTestClient implements AutoCloseable {

    private final String endpoint;
    private final HttpClient client = HttpClient.newHttpClient();

    S3UploadTestClient(String endpoint) {
        this.endpoint = endpoint;
    }

    HttpResponse<byte[]> request(String method, String path, byte[] body, Map<String, String> headers) throws IOException {
        URI uri = URI.create(endpoint + path);
        String hash = sha256(body);
        Map<String, String> signingHeaders = new LinkedHashMap<>(headers);
        signingHeaders.put("Host", uri.getRawAuthority());
        signingHeaders.put("x-amz-content-sha256", hash);
        Aws4Signer.SignResult signed = Aws4Signer.sign(method, uri, signingHeaders, hash,
                S3Proxy.ACCESS_KEY, S3Proxy.SECRET_KEY, null, "auto", "s3", ZonedDateTime.now(ZoneOffset.UTC));
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(10))
                .method(method, body.length == 0 ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Authorization", signed.authorizationHeader());
        signed.headers().forEach((name, value) -> {
            if (!name.equalsIgnoreCase("Host")) {
                builder.header(name, value);
            }
        });
        try {
            return client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        }
        catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted test S3 request", failure);
        }
    }

    HttpResponse<byte[]> head(String bucket, String key) throws IOException {
        return request("HEAD", objectPath(bucket, key), new byte[0], Map.of());
    }

    byte[] get(String bucket, String key) throws IOException {
        HttpResponse<byte[]> response = request("GET", objectPath(bucket, key), new byte[0], Map.of());
        requireStatus(response, 200);
        return response.body();
    }

    void put(String bucket, String key, byte[] body) throws IOException {
        requireStatus(request("PUT", objectPath(bucket, key), body, Map.of()), 200);
    }

    /// Follows response pagination and filters by exact key. The pinned s3proxy does not accept pagination parameters on the first request.
    List<Upload> uploads(String bucket, String key) throws IOException {
        List<Upload> uploads = new ArrayList<>();
        String markers = "";
        Set<String> seenMarkers = new HashSet<>();
        while (true) {
            HttpResponse<byte[]> response = request("GET", "/" + bucket + "?uploads=" + markers,
                    new byte[0], Map.of());
            requireStatus(response, 200);
            Document document = parse(response.body());
            if (!"ListMultipartUploadsResult".equals(document.getDocumentElement().getLocalName())) {
                throw new IOException("Unexpected multipart listing root");
            }
            NodeList entries = document.getElementsByTagNameNS("*", "Upload");
            for (int i = 0; i < entries.getLength(); i++) {
                Element entry = (Element) entries.item(i);
                String uploadKey = required(entry, "Key");
                String uploadId = required(entry, "UploadId");
                if (key == null || key.equals(uploadKey)) {
                    uploads.add(new Upload(uploadKey, uploadId));
                }
            }
            String truncated = required(document.getDocumentElement(), "IsTruncated");
            if ("false".equals(truncated)) {
                return uploads;
            }
            if (!"true".equals(truncated)) {
                throw new IOException("Invalid multipart listing truncation flag");
            }
            markers = "&key-marker=" + encode(required(document.getDocumentElement(), "NextKeyMarker"))
                    + "&upload-id-marker=" + encode(required(document.getDocumentElement(), "NextUploadIdMarker"));
            if (!seenMarkers.add(markers)) {
                throw new IOException("Multipart listing markers do not advance");
            }
        }
    }

    void abort(String bucket, Upload upload) throws IOException {
        HttpResponse<byte[]> response = request("DELETE", objectPath(bucket, upload.key())
                + "?uploadId=" + encode(upload.id()), new byte[0], Map.of());
        requireStatus(response, 204);
    }

    void deleteBucket(String bucket) throws IOException {
        HttpResponse<byte[]> response = request("GET", "/" + bucket, new byte[0], Map.of());
        requireStatus(response, 200);
        Document document = parse(response.body());
        if (!"false".equals(required(document.getDocumentElement(), "IsTruncated"))) {
            throw new IOException("Test bucket has too many objects for cleanup");
        }
        NodeList objects = document.getElementsByTagNameNS("*", "Contents");
        for (int i = 0; i < objects.getLength(); i++) {
            String key = required((Element) objects.item(i), "Key");
            requireStatus(request("DELETE", objectPath(bucket, key), new byte[0], Map.of()), 204);
        }
        requireStatus(request("DELETE", "/" + bucket, new byte[0], Map.of()), 204);
    }

    static String objectPath(String bucket, String key) {
        return "/" + bucket + "/" + encode(key).replace("%2F", "/");
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        }
        catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is required by the JDK", failure);
        }
    }

    private static void requireStatus(HttpResponse<byte[]> response, int expected) throws IOException {
        if (response.statusCode() != expected) {
            throw new IOException("Test S3 request failed: HTTP " + response.statusCode() + ": "
                    + new String(response.body(), StandardCharsets.UTF_8));
        }
    }

    private static Document parse(byte[] body) throws IOException {
        if (body.length > 256 * 1024) {
            throw new IOException("Oversized test multipart listing");
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultNSInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            return factory.newDocumentBuilder().parse(new ByteArrayInputStream(body));
        }
        catch (ParserConfigurationException | SAXException failure) {
            throw new IOException("Invalid test multipart listing", failure);
        }
    }

    private static String required(Element parent, String name) throws IOException {
        NodeList values = parent.getElementsByTagNameNS("*", name);
        if (values.getLength() != 1 || values.item(0).getTextContent().isEmpty()) {
            throw new IOException("Missing or ambiguous " + name + " in test multipart listing");
        }
        return values.item(0).getTextContent();
    }

    @Override
    public void close() {
        client.close();
    }

    record Upload(String key, String id) {
    }
}
