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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import dev.hardwood.OutputFile;
import dev.hardwood.s3.internal.S3Api;
import dev.hardwood.s3.internal.S3OutputFile;

/// A configured connection to an S3-compatible object store.
///
/// Holds the region, endpoint, credentials, and HTTP client needed
/// to create [S3InputFile] and [OutputFile] instances. Reuse a single `S3Source` across
/// multiple files for connection pooling and credential reuse.
/// Keep the source open until its readers and writers have finished.
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
    private final int uploadPartSize;

    private S3Source(S3Api api, HttpClient httpClient, boolean externalHttpClient,
                     RangeBacking rangeBacking, Path tempDir, int uploadPartSize) {
        this.api = api;
        this.httpClient = httpClient;
        this.externalHttpClient = externalHttpClient;
        this.rangeBacking = rangeBacking;
        this.tempDir = tempDir;
        this.uploadPartSize = uploadPartSize;
    }

    /// Creates an [S3InputFile] for the given bucket and key. When the
    /// source is configured with [RangeBacking#SPARSE_TEMPFILE], the
    /// returned file transparently caches fetched byte ranges in a
    /// mmap-backed sparse temp file; the cache is an internal
    /// implementation detail and does not change the returned type.
    public S3InputFile inputFile(String bucket, String key) {
        Objects.requireNonNull(bucket, "bucket must not be null");
        Objects.requireNonNull(key, "key must not be null");
        return new S3InputFile(this, bucket, key);
    }

    /// Creates an [S3InputFile] from an `s3://bucket/key` URI. See
    /// [#inputFile(String, String)] for the [RangeBacking] policy.
    public S3InputFile inputFile(String uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        String[] parsed = parseS3Uri(uri);
        return inputFile(parsed[0], parsed[1]);
    }

    /// Creates an uncreated sequential [OutputFile] for the given bucket and key.
    ///
    /// Neither this method nor [OutputFile#create()] makes network requests.
    /// [dev.hardwood.writer.ParquetFileWriter] calls `create()` automatically.
    /// Each active output holds one payload buffer of [Builder#uploadPartSize(int)]
    /// bytes (8 MiB by default). Outputs smaller than that size use one PUT on
    /// close; outputs reaching that size upload parts sequentially and publish on close.
    /// The bucket must already exist. Successful publication replaces an existing
    /// object at the key; no conditional-write or version-management option is provided.
    ///
    /// A lost publication response triggers bounded HEAD checks for this output's
    /// write UUID and length. A match makes close succeed. Otherwise the original
    /// exception carries a suppressed diagnostic saying publication is unconfirmed;
    /// the destination may contain the completed object. Discard aborts only this
    /// output's known multipart upload and never deletes the destination object.
    /// An output does not close the source or its HTTP client.
    ///
    /// @param bucket non-null, non-empty bucket name
    /// @param key non-null, non-empty object key, used literally without URI decoding
    /// @return a new uncreated output
    /// @throws NullPointerException if bucket or key is null
    /// @throws IllegalArgumentException if bucket or key is empty
    public OutputFile outputFile(String bucket, String key) {
        Objects.requireNonNull(bucket, "bucket must not be null");
        Objects.requireNonNull(key, "key must not be null");
        return new S3OutputFile(api, bucket, key, uploadPartSize);
    }

    /// Creates an uncreated [OutputFile] from an `s3://bucket/key` URI.
    /// The key following the bucket's first `/` is used literally, including
    /// percent signs, `?`, and `#`; it is not URI-decoded or normalized.
    /// See [#outputFile(String, String)] for publication and cleanup behavior.
    ///
    /// @param uri an `s3://bucket/key` URI with a non-empty bucket and key
    /// @return a new uncreated output
    /// @throws NullPointerException if uri is null
    /// @throws IllegalArgumentException if the URI lacks the `s3://` prefix, bucket, or key
    public OutputFile outputFile(String uri) {
        Objects.requireNonNull(uri, "uri must not be null");
        String[] parsed = parseS3Uri(uri);
        return outputFile(parsed[0], parsed[1]);
    }

    /// Creates [S3InputFile] instances for multiple keys in the same bucket.
    public List<S3InputFile> inputFilesInBucket(String bucket, String... keys) {
        Objects.requireNonNull(bucket, "bucket must not be null");
        List<S3InputFile> files = new ArrayList<>(keys.length);
        for (String key : keys) {
            Objects.requireNonNull(key, "key must not be null");
            files.add(inputFile(bucket, key));
        }
        return files;
    }

    /// Creates [S3InputFile] instances from `s3://` URIs (may span buckets).
    public List<S3InputFile> inputFiles(String... uris) {
        List<S3InputFile> files = new ArrayList<>(uris.length);
        for (String uri : uris) {
            Objects.requireNonNull(uri, "uri must not be null");
            files.add(inputFile(uri));
        }
        return files;
    }

    S3Api api() {
        return api;
    }

    RangeBacking rangeBacking() {
        return rangeBacking;
    }

    Path tempDir() {
        return tempDir;
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
        private static final int DEFAULT_UPLOAD_PART_SIZE = 8 * 1024 * 1024;

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
        private int uploadPartSize = DEFAULT_UPLOAD_PART_SIZE;

        private Builder() {
        }

        /// Sets the payload buffer size for each active S3 output (default 8 MiB).
        /// Must be between 5 MiB and `Integer.MAX_VALUE - 8` bytes, inclusive.
        /// At most 10,000 parts can be uploaded, so the maximum output size is
        /// `10,000 * bytes` (78.125 GiB at the default size). The buffer is allocated
        /// by [OutputFile#create()]. This does not configure input ranges, Parquet
        /// pages, or row groups.
        ///
        /// @param bytes payload buffer size in bytes
        /// @return this builder
        /// @throws IllegalArgumentException if bytes is outside the supported range
        public Builder uploadPartSize(int bytes) {
            S3OutputFile.validatePartSize(bytes);
            this.uploadPartSize = bytes;
            return this;
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

        /// Sets the maximum number of retries for GET, part-upload, and abort requests
        /// on transient failures (HTTP 500/503 and network errors). Default is 3.
        /// Uncertain publication and part cleanup each use a shared budget of
        /// `maxRetries + 1` verification or cleanup attempts. Multipart initiation,
        /// whole-object PUT, and multipart completion are never automatically replayed.
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
        ///
        /// Under [RangeBacking#SPARSE_TEMPFILE] the directory must exist
        /// and be writeable; [#build()] rejects it otherwise.
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
            Path effectiveTempDir = tempDir != null
                    ? tempDir
                    : Path.of(System.getProperty("java.io.tmpdir"));
            if (rangeBacking == RangeBacking.SPARSE_TEMPFILE) {
                requireUsableTempDir(effectiveTempDir);
            }
            // Built only after all validation, so a rejected configuration leaves no client behind
            boolean externalClient = httpClient != null;
            HttpClient client = externalClient
                    ? httpClient
                    : HttpClient.newBuilder().connectTimeout(connectTimeout).build();
            S3Api api = new S3Api(client, credentialsProvider, effectiveRegion, endpointUri, pathStyle,
                    requestTimeout, maxRetries);
            return new S3Source(api, client, externalClient, rangeBacking, effectiveTempDir, uploadPartSize);
        }

        /// Rejects a temp directory that [RangeBacking#SPARSE_TEMPFILE]
        /// could not create its backing file in, so the misconfiguration
        /// surfaces here rather than at the first [S3InputFile#open()].
        private static void requireUsableTempDir(Path dir) {
            if (!Files.isDirectory(dir)) {
                throw new IllegalStateException("tempDir does not exist or is not a directory: " + dir
                        + " (required by RangeBacking.SPARSE_TEMPFILE)");
            }
            if (!Files.isWritable(dir)) {
                throw new IllegalStateException("tempDir is not writeable: " + dir
                        + " (required by RangeBacking.SPARSE_TEMPFILE)");
            }
        }
    }
}
