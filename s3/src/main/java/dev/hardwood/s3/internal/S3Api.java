/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;

/// Low-level S3 REST API client. Signs and issues HTTP requests.
///
/// Encapsulates credential resolution, SigV4 signing, URI construction,
/// and HTTP transport. Used by [dev.hardwood.s3.S3InputFile] for reads and
/// by the sequential S3 output backend. Bucket creation and legacy whole-array
/// uploads also support test fixtures.
public final class S3Api {

    static final String WRITE_ID_HEADER = "x-amz-meta-hardwood-write-id";

    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private static final String SERVICE = "s3";
    private static final String SHA256_EMPTY = Aws4Signer.sha256Empty();

    private static final Duration BASE_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_DELAY = Duration.ofSeconds(5);

    private final HttpClient httpClient;
    private final S3CredentialsProvider credentialsProvider;
    private final String region;
    private final URI endpoint;
    private final boolean pathStyle;
    private final Duration requestTimeout;
    private final int maxRetries;

    /// Creates an S3Api instance.
    ///
    /// @param httpClient          the HTTP client to use
    /// @param credentialsProvider provides credentials for signing
    /// @param region              the AWS region (e.g. "us-east-1")
    /// @param endpoint            custom endpoint URI, or `null` for AWS virtual-hosted style
    /// @param pathStyle           if `true`, use path-style access (`endpoint/bucket/key`)
    /// @param requestTimeout      timeout for individual HTTP requests, or `null` for no timeout
    /// @param maxRetries          maximum number of retries for GET requests (0 means no retries)
    public S3Api(HttpClient httpClient, S3CredentialsProvider credentialsProvider,
            String region, URI endpoint, boolean pathStyle,
            Duration requestTimeout, int maxRetries) {
        this.httpClient = httpClient;
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.endpoint = endpoint != null ? withoutDefaultPort(endpoint) : null;
        this.pathStyle = pathStyle;
        this.requestTimeout = requestTimeout;
        this.maxRetries = maxRetries;
    }

    /// Sends a GET request with a `Range` header and returns the full response body as bytes.
    ///
    /// Retries on HTTP 500/503 responses and network errors up to [#maxRetries] times
    /// with exponential backoff and jitter.
    public HttpResponse<byte[]> getBytes(String bucket, String key, String rangeHeader) throws IOException {
        return sendWithRetry(bucket, key, rangeHeader, HttpResponse.BodyHandlers.ofByteArray());
    }

    /// Sends a GET request with a `Range` header and returns a streaming response body.
    ///
    /// Retries on HTTP 500/503 responses and network errors up to [#maxRetries] times
    /// with exponential backoff and jitter.
    public HttpResponse<InputStream> getStream(String bucket, String key, String rangeHeader) throws IOException {
        return sendWithRetry(bucket, key, rangeHeader, HttpResponse.BodyHandlers.ofInputStream());
    }

    private <T> HttpResponse<T> sendWithRetry(String bucket, String key, String rangeHeader,
            HttpResponse.BodyHandler<T> bodyHandler) throws IOException {
        IOException lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0 && lastException != null) {
                sleepBeforeRetry(attempt);
            }
            HttpRequest request = signRequest("GET", objectUri(bucket, key),
                    SHA256_EMPTY, HttpRequest.BodyPublishers.noBody(),
                    "Range", rangeHeader);
            try {
                HttpResponse<T> response = httpClient.send(request, bodyHandler);
                int status = response.statusCode();
                if ((status == HTTP_INTERNAL_SERVER_ERROR || status == HTTP_SERVICE_UNAVAILABLE) && attempt < maxRetries) {
                    lastException = new IOException("GET s3://" + bucket + "/" + key
                            + " failed: HTTP " + status);
                    discardBody(response);
                    continue;
                }
                return response;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during GET s3://" + bucket + "/" + key, e);
            }
            catch (IOException e) {
                lastException = e;
                if (attempt >= maxRetries) {
                    throw e;
                }
            }
        }
        throw lastException;
    }

    /// Closes a streamed body the caller will never see, so a response
    /// abandoned for a retry does not hold its connection until GC.
    /// A byte-array body is already fully read and needs no release.
    private static void discardBody(HttpResponse<?> response) throws IOException {
        if (response.body() instanceof InputStream body) {
            body.close();
        }
    }

    static void sleepBeforeRetry(int attempt) throws IOException {
        Duration delay = BASE_DELAY.multipliedBy(1L << Math.min(attempt - 1, 6));
        if (delay.compareTo(MAX_DELAY) > 0) {
            delay = MAX_DELAY;
        }
        long jitterMs = ThreadLocalRandom.current().nextLong(delay.toMillis() / 2 + 1);
        Duration sleepDuration = delay.plusMillis(jitterMs);
        try {
            Thread.sleep(sleepDuration);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during retry backoff", e);
        }
    }

    /// Sends a PUT request to upload a byte array.
    public void putObject(String bucket, String key, byte[] body) throws IOException {
        String payloadHash = Aws4Signer.hexEncode(Aws4Signer.sha256(body));
        HttpRequest request = signRequest("PUT", objectUri(bucket, key),
                payloadHash, HttpRequest.BodyPublishers.ofByteArray(body));
        sendAndCheck(request, "PUT s3://" + bucket + "/" + key);
    }

    /// Sends a PUT request to create a bucket.
    public void createBucket(String bucket) throws IOException {
        HttpRequest request = signRequest("PUT", bucketUri(bucket),
                SHA256_EMPTY, HttpRequest.BodyPublishers.noBody());
        sendAndCheck(request, "PUT bucket " + bucket);
    }

    // ==================== Multipart output ====================

    String initiateMultipartUpload(String bucket, String key, String writeId) throws IOException {
        requireNonBlank(writeId, "writeId");
        String description = "Initiate multipart upload s3://" + bucket + "/" + key;
        HttpResponse<byte[]> response = sendWrite("POST", URI.create(objectUri(bucket, key) + "?uploads="),
                new byte[0], 0, false, description, WRITE_ID_HEADER, writeId);
        checkResponse(response, 200, description);
        return S3Xml.uploadId(response.body());
    }

    String uploadPart(String bucket, String key, String uploadId, int partNumber, byte[] body, int length) throws IOException {
        S3Xml.validatePartNumber(partNumber);
        String description = "Upload part " + partNumber + " s3://" + bucket + "/" + key;
        URI uri = multipartUri(bucket, key, uploadId, "partNumber=" + partNumber + "&");
        HttpResponse<byte[]> response = sendWrite("PUT", uri, body, length, true, description);
        checkResponse(response, 200, description);
        List<String> etags = response.headers().allValues("ETag");
        if (etags.size() != 1 || etags.getFirst().isBlank()) {
            throw new IOException(description + " returned a missing or ambiguous ETag");
        }
        return etags.getFirst();
    }

    String completeMultipartUpload(String bucket, String key, String uploadId, List<S3Xml.Part> parts) throws IOException {
        byte[] body = S3Xml.completionBody(parts);
        String description = "Complete multipart upload s3://" + bucket + "/" + key;
        HttpResponse<byte[]> response = sendWrite("POST", multipartUri(bucket, key, uploadId, ""),
                body, body.length, false, description, "Content-Type", "application/xml");
        checkResponse(response, 200, description);
        return S3Xml.completedETag(response.body());
    }

    void putObject(String bucket, String key, byte[] body, int length, String writeId) throws IOException {
        requireNonBlank(writeId, "writeId");
        String description = "PUT s3://" + bucket + "/" + key;
        HttpResponse<byte[]> response = sendWrite("PUT", objectUri(bucket, key), body, length, false,
                description, WRITE_ID_HEADER, writeId);
        checkResponse(response, 200, description);
    }

    void abortMultipartUpload(String bucket, String key, String uploadId) throws IOException {
        abortMultipartUpload(bucket, key, uploadId, true);
    }

    void abortMultipartUploadOnce(String bucket, String key, String uploadId) throws IOException {
        abortMultipartUpload(bucket, key, uploadId, false);
    }

    private void abortMultipartUpload(String bucket, String key, String uploadId, boolean retry) throws IOException {
        String description = "Abort multipart upload s3://" + bucket + "/" + key;
        HttpResponse<byte[]> response = sendWrite("DELETE", multipartUri(bucket, key, uploadId, ""),
                new byte[0], 0, retry, description);
        checkUploadResponse(response, 204, description);
    }

    HttpResponse<Void> headObject(String bucket, String key) throws IOException {
        HttpRequest request = signRequest("HEAD", objectUri(bucket, key), SHA256_EMPTY, HttpRequest.BodyPublishers.noBody());
        return sendOnce(request, HttpResponse.BodyHandlers.discarding(), "HEAD s3://" + bucket + "/" + key);
    }

    UploadState inspectMultipartUpload(String bucket, String key, String uploadId) throws IOException {
        int marker = 0;
        String description = "List multipart upload parts s3://" + bucket + "/" + key;
        while (true) {
            String prefix = marker == 0 ? "" : "part-number-marker=" + marker + "&";
            HttpResponse<byte[]> response = sendWrite("GET", multipartUri(bucket, key, uploadId, prefix),
                    new byte[0], 0, false, description);
            if (!checkUploadResponse(response, 200, description)) {
                return UploadState.MISSING;
            }
            S3Xml.PartsPage page = S3Xml.partsPage(response.body());
            if (page.hasParts()) {
                return UploadState.HAS_PARTS;
            }
            if (page.nextMarker() == 0) {
                return UploadState.EMPTY;
            }
            if (page.nextMarker() <= marker) {
                throw new IOException(description + " returned a non-increasing pagination marker");
            }
            marker = page.nextMarker();
        }
    }

    enum UploadState {
        MISSING, EMPTY, HAS_PARTS
    }

    int maxRetries() {
        return maxRetries;
    }

    private HttpResponse<byte[]> sendWrite(String method, URI uri, byte[] body, int length,
            boolean retry, String description, String... headers) throws IOException {
        String hash = Aws4Signer.hexEncode(Aws4Signer.sha256(body, 0, length));
        for (int attempt = 0;; attempt++) {
            if (attempt > 0) {
                sleepBeforeRetry(attempt);
            }
            HttpRequest request = signRequest(method, uri, hash, HttpRequest.BodyPublishers.ofByteArray(body, 0, length), headers);
            HttpResponse<byte[]> response;
            try {
                response = sendOnce(request, info -> new BoundedBodySubscriber(S3Xml.MAX_RESPONSE_SIZE), description);
            }
            catch (IOException e) {
                if (!retry || attempt >= maxRetries || Thread.currentThread().isInterrupted() || bodyLimitFailure(e)) {
                    throw e;
                }
                continue;
            }
            int status = response.statusCode();
            if (retry && attempt < maxRetries && (status == HTTP_INTERNAL_SERVER_ERROR || status == HTTP_SERVICE_UNAVAILABLE)) {
                continue;
            }
            return response;
        }
    }

    private <T> HttpResponse<T> sendOnce(HttpRequest request, HttpResponse.BodyHandler<T> handler, String description) throws IOException {
        CompletableFuture<HttpResponse<T>> response = httpClient.sendAsync(request, handler);
        try {
            if (requestTimeout == null) {
                return response.get();
            }
            Duration maxTimeout = Duration.ofNanos(Long.MAX_VALUE);
            long nanos = requestTimeout.compareTo(maxTimeout) > 0 ? Long.MAX_VALUE : requestTimeout.toNanos();
            return response.get(nanos, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e) {
            response.cancel(true);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during " + description, e);
        }
        catch (TimeoutException e) {
            response.cancel(true);
            HttpTimeoutException failure = new HttpTimeoutException("Timed out during " + description);
            failure.initCause(e);
            throw failure;
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException failure) {
                throw failure;
            }
            if (cause instanceof RuntimeException failure) {
                throw failure;
            }
            if (cause instanceof Error failure) {
                throw failure;
            }
            throw new IOException(description + " failed", cause);
        }
    }

    private static boolean bodyLimitFailure(IOException failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 16; depth++) {
            if (cause instanceof BoundedBodySubscriber.BodyLimitException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static void checkResponse(HttpResponse<byte[]> response, int expectedStatus, String description) throws IOException {
        if (response.statusCode() != expectedStatus) {
            throw responseFailure(response, description);
        }
    }

    private static boolean checkUploadResponse(HttpResponse<byte[]> response, int expectedStatus, String description) throws IOException {
        if (response.statusCode() == expectedStatus) {
            return true;
        }
        HttpException failure = responseFailure(response, description);
        if (response.statusCode() == 404 && failure.getCause() instanceof S3Xml.ServiceException error && "NoSuchUpload".equals(error.code())) {
            return false;
        }
        throw failure;
    }

    private static HttpException responseFailure(HttpResponse<byte[]> response, String description) {
        IOException error;
        try {
            error = S3Xml.error(response.body());
        }
        catch (IOException e) {
            error = e;
        }
        return new HttpException(response.statusCode(), description, error);
    }

    private URI multipartUri(String bucket, String key, String uploadId, String prefix) {
        requireNonBlank(uploadId, "uploadId");
        return URI.create(objectUri(bucket, key) + "?" + prefix + "uploadId=" + Aws4Signer.uriEncode(uploadId));
    }

    private static void requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    static final class HttpException extends IOException {

        private final int statusCode;

        private HttpException(int statusCode, String description, IOException cause) {
            super(description + " failed: HTTP " + statusCode + " " + cause.getMessage(), cause);
            this.statusCode = statusCode;
        }

        int statusCode() {
            return statusCode;
        }
    }

    // ==================== URI construction ====================

    /// Builds the request URI for an object.
    public URI objectUri(String bucket, String key) {
        String encodedKey = uriEncodePath(key);
        if (pathStyle) {
            return URI.create(pathStyleBase(bucket) + "/" + encodedKey);
        }
        return URI.create(virtualHostedBase(bucket) + "/" + encodedKey);
    }

    private URI bucketUri(String bucket) {
        if (pathStyle) {
            return URI.create(pathStyleBase(bucket));
        }
        return URI.create(virtualHostedBase(bucket) + "/");
    }

    /// Path-style: `{endpoint}/{bucket}` or `https://s3.{region}.amazonaws.com/{bucket}`.
    private String pathStyleBase(String bucket) {
        if (endpoint != null) {
            String base = endpoint.toString();
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            return base + "/" + bucket;
        }
        return "https://s3." + region + ".amazonaws.com/" + bucket;
    }

    /// Virtual-hosted: `{scheme}://{bucket}.{host}[:{port}]` or
    /// `https://{bucket}.s3.{region}.amazonaws.com`.
    private String virtualHostedBase(String bucket) {
        if (endpoint != null) {
            String scheme = endpoint.getScheme();
            String host = endpoint.getHost();
            int port = endpoint.getPort();
            String portSuffix = port > 0 ? ":" + port : "";
            return scheme + "://" + bucket + "." + host + portSuffix;
        }
        return "https://" + bucket + ".s3." + region + ".amazonaws.com";
    }

    /// URI-encodes a key path, preserving `/` as path separators.
    private static String uriEncodePath(String key) {
        String[] segments = key.split("/", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(Aws4Signer.uriEncode(segments[i]));
        }
        return sb.toString();
    }

    // ==================== Signing and sending ====================

    private HttpRequest signRequest(String method, URI uri, String payloadHash,
            HttpRequest.BodyPublisher bodyPublisher, String... extraHeaders) {

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Host", hostHeader(uri));
        headers.put("x-amz-content-sha256", payloadHash);
        for (int i = 0; i < extraHeaders.length; i += 2) {
            headers.put(extraHeaders[i], extraHeaders[i + 1]);
        }

        S3Credentials credentials = credentialsProvider.credentials();
        Aws4Signer.SignResult signed = Aws4Signer.sign(
                method, uri, headers, payloadHash,
                credentials.accessKeyId(), credentials.secretAccessKey(),
                credentials.sessionToken(),
                region, SERVICE,
                ZonedDateTime.now(ZoneOffset.UTC));

        HttpRequest.Builder jdkBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .method(method, bodyPublisher);

        if (requestTimeout != null) {
            jdkBuilder.timeout(requestTimeout);
        }

        for (Map.Entry<String, String> entry : signed.headers().entrySet()) {
            if (!"host".equals(entry.getKey())) {
                jdkBuilder.header(entry.getKey(), entry.getValue());
            }
        }
        jdkBuilder.header("Authorization", signed.authorizationHeader());

        return jdkBuilder.build();
    }

    private void sendAndCheck(HttpRequest request, String description) throws IOException {
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new IOException(description + " failed: HTTP "
                        + response.statusCode() + " " + response.body());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during " + description, e);
        }
    }

    private static String hostHeader(URI uri) {
        return uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : "");
    }

    /// Drops a port equal to the scheme's default from `endpoint`. The JDK
    /// `HttpClient` leaves a default port out of the HTTP/1.1 `Host` header
    /// but keeps it in the HTTP/2 `:authority`, so only a request URI
    /// without one is sent with the same host the request is signed for.
    private static URI withoutDefaultPort(URI endpoint) {
        int port = endpoint.getPort();
        int defaultPort = "https".equalsIgnoreCase(endpoint.getScheme()) ? 443 : 80;
        if (port != defaultPort) {
            return endpoint;
        }
        String authority = endpoint.getRawAuthority();
        String hostOnly = authority.substring(0, authority.length() - (":" + port).length());
        return URI.create(endpoint.getScheme() + "://" + hostOnly
                + nullToEmpty(endpoint.getRawPath())
                + (endpoint.getRawQuery() != null ? "?" + endpoint.getRawQuery() : "")
                + (endpoint.getRawFragment() != null ? "#" + endpoint.getRawFragment() : ""));
    }

    private static String nullToEmpty(String value) {
        return value != null ? value : "";
    }
}
