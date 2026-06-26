/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.aws.auth.SdkCredentialsProviders;
import dev.hardwood.s3.RangeBacking;
import dev.hardwood.s3.S3Source;

public abstract class FileCommandBase {

    private static final String[] REMOTE_PREFIXES = { "s3://" };
    private static final String[] UNSUPPORTED_REMOTE_PREFIXES = { "gs://", "gcs://", "hdfs://" };

    @Option(name = "file", shortName = 'f', required = true, description = "Path to the Parquet file.")
    protected String file;

    protected String getCleanedFile() {
        if (file != null && file.length() > 2 && file.charAt(0) == '/' && file.charAt(2) == ':') {
            return file.substring(1);
        }
        return file;
    }

    protected boolean isRemoteUri(CommandInvocation invocation) {
        String cleaned = getCleanedFile();
        for (String prefix : UNSUPPORTED_REMOTE_PREFIXES) {
            if (cleaned.startsWith(prefix)) {
                System.err.println("Remote paths are not implemented yet for this command.");
                return true;
            }
        }
        return false;
    }

    protected InputFile toInputFile(CommandInvocation invocation) {
        return toInputFile(invocation, RangeBacking.NONE);
    }

    /// Resolves the configured `--file` to an [InputFile]. For remote
    /// (`s3://`) URIs, opens an [S3Source] with the given range-caching
    /// strategy. Streaming commands (`print`, `convert`, `info`,
    /// `inspect`) leave the default [RangeBacking#NONE]; interactive
    /// commands like `dive` opt into [RangeBacking#SPARSE_TEMPFILE] so
    /// repeat reads of the same byte ranges hit a local mmap-backed
    /// cache instead of S3.
    protected InputFile toInputFile(CommandInvocation invocation, RangeBacking rangeBacking) {
        String cleaned = getCleanedFile();
        for (String prefix : REMOTE_PREFIXES) {
            if (cleaned.startsWith(prefix)) {
                return createS3InputFile(invocation, rangeBacking);
            }
        }
        for (String prefix : UNSUPPORTED_REMOTE_PREFIXES) {
            if (cleaned.startsWith(prefix)) {
                System.err.println("Remote URIs are not implemented yet.");
                return null;
            }
        }
        return InputFile.of(Path.of(cleaned));
    }

    private InputFile createS3InputFile(CommandInvocation invocation, RangeBacking rangeBacking) {
        String endpointUrl = resolveEndpoint();

        S3Source.Builder builder = S3Source.builder()
                .credentials(SdkCredentialsProviders.defaultChain())
                .rangeBacking(rangeBacking);

        if (endpointUrl != null) {
            builder.endpoint(endpointUrl);
        }

        if (resolvePathStyle()) {
            builder.pathStyle(true);
        }

        // Resolve region from env vars and ~/.aws/config only (no IMDS — instant)
        String region = resolveRegion();
        if (region != null) {
            builder.region(region);
        }
        else if (endpointUrl == null) {
            throw new IllegalArgumentException(
                    "Unable to determine AWS region. Set AWS_REGION or configure a default region in ~/.aws/config.");
        }

        S3Source source = builder.build();
        return source.inputFile(getCleanedFile());
    }

    /// Resolves the S3 endpoint URL from system property or env var.
    private static String resolveEndpoint() {
        String endpoint = System.getProperty("aws.endpointUrl");
        if (endpoint != null) {
            return endpoint;
        }
        return System.getenv("AWS_ENDPOINT_URL");
    }

    /// Resolves whether path-style access is enabled from system property or env var.
    private static boolean resolvePathStyle() {
        String pathStyle = System.getProperty("aws.pathStyle");
        if (pathStyle != null) {
            return "true".equalsIgnoreCase(pathStyle);
        }
        return "true".equalsIgnoreCase(System.getenv("AWS_PATH_STYLE"));
    }

    /// Resolves the AWS region from system property, env vars, and `~/.aws/config` (no network I/O).
    private static String resolveRegion() {
        // 1. System property
        String region = System.getProperty("aws.region");
        if (region != null) {
            return region;
        }

        // 2. Environment variables
        region = System.getenv("AWS_REGION");
        if (region != null) {
            return region;
        }
        region = System.getenv("AWS_DEFAULT_REGION");
        if (region != null) {
            return region;
        }

        // 3. ~/.aws/config [default] profile
        String profile = System.getenv("AWS_PROFILE");
        if (profile == null) {
            profile = "default";
        }
        return resolveRegionFromConfig(profile);
    }

    private static String resolveRegionFromConfig(String profile) {
        Path configFile = Path.of(System.getProperty("user.home"), ".aws", "config");
        if (!Files.exists(configFile)) {
            return null;
        }
        // Profile header is [default] or [profile name]
        String header = "default".equals(profile)
                ? "[default]"
                : "[profile " + profile + "]";
        try (BufferedReader reader = Files.newBufferedReader(configFile)) {
            boolean inProfile = false;
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.startsWith("[")) {
                    inProfile = line.equals(header);
                    continue;
                }
                if (inProfile && line.startsWith("region")) {
                    int eq = line.indexOf('=');
                    if (eq >= 0) {
                        return line.substring(eq + 1).strip();
                    }
                }
            }
        }
        catch (IOException e) {
            // Can't read config — not fatal
        }
        return null;
    }

    protected Path toPath() {
        return Path.of(getCleanedFile());
    }
}
