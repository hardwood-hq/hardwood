/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.RangeBackedInputFile;
import dev.hardwood.s3.internal.S3Api;

/// A configured connection to an S3-compatible object store.
///
/// Holds the region, endpoint, credentials, and HTTP client needed
/// to create [S3InputFile] instances. Reuse a single `S3Source` across
/// multiple files for connection pooling and credential reuse.
///
/// **URL styles:**
/// - **Virtual-hosted** (default): `https://{bucket}.s3.{region}.amazonaws.com/{key}`
/// - **Path-style** (opt-in via [Builder#pathStyle]): `{endpoint}/{bucket}/{key}`
public final class S3Source implements Closeable {

    private final S3Api api;
    private final HttpClient httpClient;
    private final boolean externalHttpClient;
    private final RangeBacking rangeBacking;
    private final Path tempDir;

    private S3Source(S3Api api, HttpClient httpClient, boolean externalHttpClient,
                     RangeBacking rangeBacking, Path tempDir) {
        this.api = api;
        this.httpClient = httpClient;
        this.externalHttpClient = externalHttpClient;
        this.rangeBacking = rangeBacking;
        this.tempDir = tempDir;
    }

    /// Creates an [InputFile] for the given bucket and key. Returns an
    /// [S3InputFile] when range backing is [RangeBacking#NONE] (the
    /// default); otherwise wraps that bare file in a
    /// [RangeBackedInputFile] so repeated reads of the same byte range
    /// hit the local mapping.
    public InputFile inputFile(String bucket, String key) {
        Objects.requireNonNull(bucket, "bucket must not be null");
        Objects.requireNonNull(key, "key must not be null");
        S3InputFile bare = new S3InputFile(this, bucket, key);
        return rangeBacking == RangeBacking.SPARSE_TEMPFILE
                ? new RangeBackedInputFile(bare, tempDir)
                : bare;
    }

    /// Creates an [InputFile] from an `s3://bucket/key` URI. See
    /// [#inputFile(String, String)] for the [RangeBacking] policy.
    public InputFile inputFile(String uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        String[] parsed = parseS3Uri(uri);
        return inputFile(parsed[0], parsed[1]);
    }

    /// Creates [InputFile] instances for multiple keys in the same bucket.
    public List<InputFile> inputFilesInBucket(String bucket, String... keys) {
        Objects.requireNonNull(bucket, "bucket must not be null");
        List<InputFile> files = new ArrayList<>(keys.length);
        for (String key : keys) {
            Objects.requireNonNull(key, "key must not be null");
            files.add(inputFile(bucket, key));
        }
        return files;
    }

    /// Creates [InputFile] instances from `s3://` URIs (may span buckets).
    public List<InputFile> inputFiles(String... uris) {
        List<InputFile> files = new ArrayList<>(uris.length);
        for (String uri : uris) {
            Objects.requireNonNull(uri, "uri must not be null");
            files.add(inputFile(uri));
        }
        return files;
    }

    S3Api api() {
        return api;
    }

    @Override
    public void close() {
        if (!externalHttpClient) {
            httpClient.close();
        }
    }

    /// Creates a new [Builder].
    public static Builder builder() {
        return new Builder();
    }

    // ==================== URI parsing ====================

    private static String[] parseS3Uri(String uri) {
        if (!uri.startsWith("s3://")) {
            throw new IllegalArgumentException("Expected s3:// URI, got: " + uri);
        }
        String withoutScheme = uri.substring("s3://".length());
        int slash = withoutScheme.indexOf('/');
        if (slash < 0 || slash == withoutScheme.length() - 1) {
            throw new IllegalArgumentException("Invalid S3 URI (missing key): " + uri);
        }
        return new String[]{ withoutScheme.substring(0, slash), withoutScheme.substring(slash + 1) };
    }

    // ==================== Builder ====================

    /// Builder for [S3Source].
    public static final class Builder {

        private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
        private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
        private static final int DEFAULT_MAX_RETRIES = 3;

        private String region;
        private String endpoint;
        private boolean pathStyle;
        private S3CredentialsProvider credentialsProvider;
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private HttpClient httpClient;
        private RangeBacking rangeBacking = RangeBacking.NONE;
        private Path tempDir;

        private Builder() {
        }

        /// Sets the AWS region (e.g. `"eu-west-1"`, `"auto"`).
        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /// Sets the endpoint for S3-compatible services (e.g. MinIO, R2, GCP).
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /// Forces path-style access (`endpoint/bucket/key` instead of
        /// `bucket.endpoint/key`).
        public Builder pathStyle(boolean pathStyle) {
            this.pathStyle = pathStyle;
            return this;
        }

        /// Sets the credential provider.
        public Builder credentials(S3CredentialsProvider provider) {
            this.credentialsProvider = provider;
            return this;
        }

        /// Sets static credentials (convenience shorthand).
        public Builder credentials(S3Credentials credentials) {
            this.credentialsProvider = () -> credentials;
            return this;
        }

        /// Sets the connect timeout for the HTTP client (default 10 seconds).
        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
            return this;
        }

        /// Sets the per-request timeout for individual HTTP requests (default 30 seconds).
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
            return this;
        }

        /// Sets the maximum number of retries for GET requests on transient failures
        /// (HTTP 500/503 and network errors). Default is 3.
        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must not be negative: " + maxRetries);
            }
            this.maxRetries = maxRetries;
            return this;
        }

        /// Sets a custom [HttpClient] for full control over connection pooling
        /// and transport settings. When provided, the caller is responsible for
        /// closing the client — [S3Source#close()] will not close it.
        /// Connect timeout is ignored when a custom client is supplied.
        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
            return this;
        }

        /// Sets the range-caching strategy for files opened from this
        /// source. Default is [RangeBacking#NONE] (every `readRange`
        /// is a network GET). Opt in to [RangeBacking#SPARSE_TEMPFILE]
        /// for interactive workloads that re-read the same byte ranges
        /// — `dive` is the canonical opt-in caller.
        public Builder rangeBacking(RangeBacking rangeBacking) {
            this.rangeBacking = Objects.requireNonNull(rangeBacking, "rangeBacking must not be null");
            return this;
        }

        /// Sets the temp directory used by [RangeBacking#SPARSE_TEMPFILE]
        /// for the sparse-file backing. Defaults to the JVM's
        /// `java.io.tmpdir`. Ignored when range backing is
        /// [RangeBacking#NONE].
        public Builder tempDir(Path tempDir) {
            this.tempDir = Objects.requireNonNull(tempDir, "tempDir must not be null");
            return this;
        }

        /// Builds the [S3Source].
        ///
        /// Region is required when targeting AWS S3 (no custom endpoint).
        /// When a custom endpoint is set, region is not used for routing
        /// and can be omitted — an arbitrary value is used for signing.
        public S3Source build() {
            if (credentialsProvider == null) {
                throw new IllegalStateException("credentials must be set");
            }
            URI endpointUri = endpoint != null ? URI.create(endpoint) : null;
            String effectiveRegion = region;
            if (effectiveRegion == null) {
                if (endpointUri == null) {
                    throw new IllegalStateException("region must be set when no custom endpoint is configured");
                }
                effectiveRegion = "auto";
            }
            boolean externalClient = httpClient != null;
            HttpClient client = externalClient
                    ? httpClient
                    : HttpClient.newBuilder().connectTimeout(connectTimeout).build();
            S3Api api = new S3Api(client, credentialsProvider, effectiveRegion, endpointUri, pathStyle,
                    requestTimeout, maxRetries);
            Path effectiveTempDir = tempDir != null
                    ? tempDir
                    : Path.of(System.getProperty("java.io.tmpdir"));
            return new S3Source(api, client, externalClient, rangeBacking, effectiveTempDir);
        }
    }
}
