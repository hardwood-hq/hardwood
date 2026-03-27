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
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
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

    private S3Source(S3Api api, HttpClient httpClient) {
        this.api = api;
        this.httpClient = httpClient;
    }

    /// Creates an [S3InputFile] for the given bucket and key.
    public S3InputFile inputFile(String bucket, String key) {
        return new S3InputFile(this, bucket, key);
    }

    /// Creates an [S3InputFile] from an `s3://bucket/key` URI.
    public S3InputFile inputFile(String uri) {
        String[] parsed = parseS3Uri(uri);
        return inputFile(parsed[0], parsed[1]);
    }

    /// Creates [InputFile] instances for multiple keys in the same bucket.
    public List<InputFile> inputFilesInBucket(String bucket, String... keys) {
        List<InputFile> files = new ArrayList<>(keys.length);
        for (String key : keys) {
            files.add(inputFile(bucket, key));
        }
        return files;
    }

    /// Creates [InputFile] instances from `s3://` URIs (may span buckets).
    public List<InputFile> inputFiles(String... uris) {
        List<InputFile> files = new ArrayList<>(uris.length);
        for (String uri : uris) {
            files.add(inputFile(uri));
        }
        return files;
    }

    S3Api api() {
        return api;
    }

    @Override
    public void close() {
        httpClient.close();
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

        private String region;
        private String endpoint;
        private boolean pathStyle;
        private S3CredentialsProvider credentialsProvider;

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
            HttpClient httpClient = HttpClient.newHttpClient();
            S3Api api = new S3Api(httpClient, credentialsProvider, effectiveRegion, endpointUri, pathStyle);
            return new S3Source(api, httpClient);
        }
    }
}
