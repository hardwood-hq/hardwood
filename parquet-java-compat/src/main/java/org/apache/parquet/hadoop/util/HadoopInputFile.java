/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
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
/// For S3 paths (`s3://` or `s3a://`), uses reflection to load
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
        return scheme != null && S3_SCHEMES.contains(scheme.toLowerCase());
    }

    private static dev.hardwood.InputFile createS3InputFile(URI uri, Configuration conf) {
        String bucket = uri.getHost();
        String key = uri.getPath();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        try {
            // Load S3 classes via reflection — no compile-time dependency on hardwood-s3 or AWS SDK
            Class<?> s3ClientClass = loadClass("software.amazon.awssdk.services.s3.S3Client",
                    "dev.hardwood:hardwood-s3");
            Class<?> s3InputFileClass = loadClass("dev.hardwood.s3.S3InputFile",
                    "dev.hardwood:hardwood-s3");

            Object s3Client = buildS3Client(conf, s3ClientClass);

            // S3InputFile.of(S3Client, String, String, boolean) — ownsClient=true
            Method ofMethod = s3InputFileClass.getMethod("of", s3ClientClass, String.class, String.class, boolean.class);
            return (dev.hardwood.InputFile) ofMethod.invoke(null, s3Client, bucket, key, true);
        }
        catch (UnsupportedOperationException e) {
            throw e;
        }
        catch (Exception e) {
            throw new UnsupportedOperationException(
                    "Failed to create S3 input file for " + uri + ": " + e.getMessage(), e);
        }
    }

    private static Object buildS3Client(Configuration conf, Class<?> s3ClientClass) throws Exception {
        // S3Client.builder()
        Method builderMethod = s3ClientClass.getMethod("builder");
        Object builder = builderMethod.invoke(null);
        Class<?> builderClass = builder.getClass();

        String accessKey = conf.get("fs.s3a.access.key");
        String secretKey = conf.get("fs.s3a.secret.key");

        if (accessKey != null && secretKey != null) {
            // AwsBasicCredentials.create(accessKey, secretKey)
            Class<?> basicCredsClass = Class.forName("software.amazon.awssdk.auth.credentials.AwsBasicCredentials");
            Method credsCreate = basicCredsClass.getMethod("create", String.class, String.class);
            Object creds = credsCreate.invoke(null, accessKey, secretKey);

            // StaticCredentialsProvider.create(creds)
            Class<?> staticProviderClass = Class.forName("software.amazon.awssdk.auth.credentials.StaticCredentialsProvider");
            Class<?> awsCredsClass = Class.forName("software.amazon.awssdk.auth.credentials.AwsCredentials");
            Method providerCreate = staticProviderClass.getMethod("create", awsCredsClass);
            Object provider = providerCreate.invoke(null, creds);

            // builder.credentialsProvider(provider)
            Class<?> credentialsProviderClass = Class.forName("software.amazon.awssdk.auth.credentials.AwsCredentialsProvider");
            Method credentialsProvider = findMethod(builderClass, "credentialsProvider", credentialsProviderClass);
            credentialsProvider.invoke(builder, provider);
        }

        String endpoint = conf.get("fs.s3a.endpoint");
        if (endpoint != null) {
            // builder.endpointOverride(URI.create(endpoint))
            Method endpointOverride = findMethod(builderClass, "endpointOverride", URI.class);
            endpointOverride.invoke(builder, URI.create(endpoint));
        }

        String region = conf.get("fs.s3a.endpoint.region", "us-east-1");
        // Region.of(region)
        Class<?> regionClass = Class.forName("software.amazon.awssdk.regions.Region");
        Method regionOf = regionClass.getMethod("of", String.class);
        Object regionObj = regionOf.invoke(null, region);
        Method regionMethod = findMethod(builderClass, "region", regionClass);
        regionMethod.invoke(builder, regionObj);

        boolean pathStyle = conf.getBoolean("fs.s3a.path.style.access", false);
        if (pathStyle) {
            // builder.forcePathStyle(true)
            Method forcePathStyle = findMethod(builderClass, "forcePathStyle", Boolean.class);
            forcePathStyle.invoke(builder, Boolean.TRUE);
        }

        // builder.build()
        Method buildMethod = findBuildMethod(builderClass);
        return buildMethod.invoke(builder);
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

    private static Method findBuildMethod(Class<?> clazz) throws NoSuchMethodException {
        // Search interfaces first for the no-arg build() method
        for (Class<?> iface : clazz.getInterfaces()) {
            try {
                return iface.getMethod("build");
            }
            catch (NoSuchMethodException ignored) {
            }
        }
        // Walk superclass interfaces
        for (Class<?> superClass = clazz.getSuperclass(); superClass != null; superClass = superClass.getSuperclass()) {
            for (Class<?> iface : superClass.getInterfaces()) {
                try {
                    return iface.getMethod("build");
                }
                catch (NoSuchMethodException ignored) {
                }
            }
        }
        Method m = clazz.getMethod("build");
        m.setAccessible(true);
        return m;
    }

    private static Method findMethod(Class<?> clazz, String name, Class<?> paramType) throws NoSuchMethodException {
        // Search through public interfaces first — AWS SDK builder classes are often
        // non-public, but the methods are declared on public interfaces.
        for (Class<?> iface : clazz.getInterfaces()) {
            for (Method m : iface.getMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == 1
                        && m.getParameterTypes()[0].isAssignableFrom(paramType)) {
                    return m;
                }
            }
        }
        // Fall back to searching the class hierarchy
        for (Method m : clazz.getMethods()) {
            if (m.getName().equals(name) && m.getParameterCount() == 1
                    && m.getParameterTypes()[0].isAssignableFrom(paramType)) {
                m.setAccessible(true);
                return m;
            }
        }
        throw new NoSuchMethodException(clazz.getName() + "." + name + "(" + paramType.getName() + ")");
    }
}
