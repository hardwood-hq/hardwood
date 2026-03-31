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
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;

/// Low-level S3 REST API client. Signs and issues HTTP requests.
///
/// Encapsulates credential resolution, SigV4 signing, URI construction,
/// and HTTP transport. Used by [dev.hardwood.s3.S3InputFile] for reads and
/// by test infrastructure for uploads.
public final class S3Api {

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
        this.endpoint = endpoint;
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

    private static void sleepBeforeRetry(int attempt) throws IOException {
        Duration delay = BASE_DELAY.multipliedBy(1L << (attempt - 1));
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
}
