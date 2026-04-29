/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InputFile;

/// Shim for Hadoop's `HadoopInputFile`.
///
/// Implements [InputFile] (the parquet-java shim interface) and internally
/// wraps a Hardwood [dev.hardwood.InputFile] for actual I/O.
///
/// For local paths, delegates to [dev.hardwood.InputFile#of(java.nio.file.Path)].
/// For S3 paths (`s3://`, `s3a://`, or `s3n://`), uses reflection to load
/// `hardwood-s3` classes, avoiding a compile-time dependency on the S3 module.
public final class HadoopInputFile implements InputFile {

    private static final Set<String> S3_SCHEMES = Set.of("s3", "s3a", "s3n");

    private final dev.hardwood.InputFile delegate;

    private HadoopInputFile(dev.hardwood.InputFile delegate) {
        this.delegate = delegate;
    }

    /// Create an [InputFile] from a Hadoop-style Path and Configuration.
    ///
    /// For local paths, the Configuration is ignored and a local file is returned.
    /// For S3 paths, the Configuration is used to construct an S3 client with properties:
    ///
    /// - `fs.s3a.access.key` + `fs.s3a.secret.key` — static credentials
    /// - `fs.s3a.endpoint` — endpoint override (e.g. LocalStack, MinIO)
    /// - `fs.s3a.path.style.access` — force path-style access
    ///
    /// @param path the Hadoop path
    /// @param conf the Hadoop configuration
    /// @return an InputFile backed by the given path
    public static InputFile fromPath(Path path, Configuration conf) {
        URI uri = path.toUri();

        dev.hardwood.InputFile hardwoodFile;
        if (isS3(uri)) {
            hardwoodFile = createS3InputFile(uri, conf);
        }
        else {
            hardwoodFile = dev.hardwood.InputFile.of(java.nio.file.Path.of(uri));
        }

        return new HadoopInputFile(hardwoodFile);
    }

    // --- org.apache.parquet.io.InputFile ---

    @Override
    public long getLength() throws IOException {
        delegate.open();
        return delegate.length();
    }

    // --- package-private access for InputFiles bridge ---

    dev.hardwood.InputFile delegate() {
        return delegate;
    }

    // --- internal helpers ---

    private static boolean isS3(URI uri) {
        String scheme = uri.getScheme();
        return scheme != null && S3_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
    }

    private static dev.hardwood.InputFile createS3InputFile(URI uri, Configuration conf) {
        String bucket = uri.getHost();
        String key = uri.getPath();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        try {
            // Build S3Credentials.of(accessKey, secretKey)
            Class<?> awsCredsClass = loadClass("dev.hardwood.s3.S3Credentials",
                    "dev.hardwood:hardwood-s3");

            String accessKey = conf.get("fs.s3a.access.key");
            String secretKey = conf.get("fs.s3a.secret.key");
            Object credentials = null;
            if (accessKey != null && secretKey != null) {
                Method ofMethod = awsCredsClass.getMethod("of", String.class, String.class);
                credentials = ofMethod.invoke(null, accessKey, secretKey);
            }

            // S3Source.builder()
            Class<?> s3SourceClass = loadClass("dev.hardwood.s3.S3Source",
                    "dev.hardwood:hardwood-s3");
            Method builderMethod = s3SourceClass.getMethod("builder");
            Object builder = builderMethod.invoke(null);
            Class<?> builderClass = builder.getClass();

            // .region(region)
            String region = conf.get("fs.s3a.endpoint.region", "us-east-1");
            Method regionMethod = builderClass.getMethod("region", String.class);
            regionMethod.invoke(builder, region);

            // .endpoint(endpoint) — optional
            String endpoint = conf.get("fs.s3a.endpoint");
            if (endpoint != null) {
                Method endpointMethod = builderClass.getMethod("endpoint", String.class);
                endpointMethod.invoke(builder, endpoint);
            }

            // .pathStyle(true) — optional
            boolean pathStyle = conf.getBoolean("fs.s3a.path.style.access", false);
            if (pathStyle) {
                Method pathStyleMethod = builderClass.getMethod("pathStyle", boolean.class);
                pathStyleMethod.invoke(builder, true);
            }

            // .credentials(credentials)
            if (credentials != null) {
                Method credsMethod = builderClass.getMethod("credentials", awsCredsClass);
                credsMethod.invoke(builder, credentials);
            }

            // .build()
            Method buildMethod = builderClass.getMethod("build");
            Object source = buildMethod.invoke(builder);

            // source.inputFile(bucket, key)
            Method inputFileMethod = s3SourceClass.getMethod("inputFile", String.class, String.class);
            dev.hardwood.InputFile inputFile = (dev.hardwood.InputFile) inputFileMethod.invoke(source, bucket, key);

            // Wrap so that closing the InputFile also closes the S3Source (which owns the HttpClient)
            return new OwnedSourceInputFile(inputFile, (Closeable) source);
        }
        catch (UnsupportedOperationException e) {
            throw e;
        }
        catch (Exception e) {
            throw new UnsupportedOperationException(
                    "Failed to create S3 input file for " + uri + ": " + e.getMessage(), e);
        }
    }

    private static Class<?> loadClass(String className, String dependency) {
        try {
            return Class.forName(className);
        }
        catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(
                    "Cannot read S3 Parquet file: required library not found. " +
                            "Add the following dependency to your project: " + dependency);
        }
    }

    /// Wraps an [dev.hardwood.InputFile] and closes an owned resource (the [dev.hardwood.s3.S3Source])
    /// when the file is closed. This ensures the `HttpClient` created for a one-off S3 read
    /// via `HadoopInputFile` is properly cleaned up.
    private record OwnedSourceInputFile(
            dev.hardwood.InputFile delegate,
            Closeable ownedResource) implements dev.hardwood.InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public java.nio.ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            }
            finally {
                ownedResource.close();
            }
        }
    }
}
