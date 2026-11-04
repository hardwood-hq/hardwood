/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.nio.file.Path;

import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.aws.auth.SdkCredentialsProviders;
import dev.hardwood.aws.auth.internal.CredentialSources;
import dev.hardwood.s3.RangeBacking;
import dev.hardwood.s3.S3CredentialsProvider;
import dev.hardwood.s3.S3Source;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.profiles.ProfileProperty;

public class FileMixin {

    private static final String[] REMOTE_PREFIXES = { "s3://" };
    private static final String[] UNSUPPORTED_REMOTE_PREFIXES = { "gs://", "gcs://", "hdfs://" };

    @Option(shortName = 'f', name = "file", required = true, description = "Path to the Parquet file.")
    String file;

    private S3Source s3Source;
    private RangeBacking s3RangeBacking;

    boolean isRemoteUri() {
        for (String prefix : UNSUPPORTED_REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                System.err.println("Remote paths are not implemented yet for this command.");
                return true;
            }
        }
        return false;
    }

    InputFile toInputFile() {
        return toInputFile(RangeBacking.NONE);
    }

    /// Resolves the configured `--file` to an [InputFile]. For remote
    /// (`s3://`) URIs, opens an [S3Source] with the given range-caching
    /// strategy. Streaming commands (`print`, `convert`, `info`,
    /// `inspect`) leave the default [RangeBacking#NONE]; interactive
    /// commands like `dive` opt into [RangeBacking#SPARSE_TEMPFILE] so
    /// repeat reads of the same byte ranges hit a local mmap-backed
    /// cache instead of S3.
    InputFile toInputFile(RangeBacking rangeBacking) {
        for (String prefix : REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                return createS3InputFile(rangeBacking);
            }
        }
        for (String prefix : UNSUPPORTED_REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                System.err.println("Remote URIs are not implemented yet.");
                return null;
            }
        }
        return InputFile.of(Path.of(file));
    }

    /// Opens the `s3://` file, or reports why it cannot be opened and returns `null`. The
    /// source behind it is built on the first call and shared by later ones, so a command that
    /// opens the file twice resolves credentials once.
    ///
    /// Requests are signed with the AWS default credential chain. When the chain finds no
    /// credentials and no local credential source is configured, they are sent unsigned, for
    /// public buckets. A configured source that fails is reported instead, so a broken profile
    /// or an expired token does not turn into a denied unsigned request.
    private InputFile createS3InputFile(RangeBacking rangeBacking) {
        if (s3Source == null) {
            s3Source = createS3Source(rangeBacking);
            if (s3Source == null) {
                return null;
            }
        }
        else if (s3RangeBacking != rangeBacking) {
            throw new IllegalStateException("S3 source opened with " + s3RangeBacking + ", requested " + rangeBacking);
        }
        s3RangeBacking = rangeBacking;
        return s3Source.inputFile(file);
    }

    private static S3Source createS3Source(RangeBacking rangeBacking) {
        String endpointUrl = resolveEndpoint();

        S3Source.Builder builder = S3Source.builder()
                .rangeBacking(rangeBacking);

        if (endpointUrl != null) {
            builder.endpoint(endpointUrl);
        }

        if (resolvePathStyle()) {
            builder.pathStyle(true);
        }

        // Resolve region from local settings and profile files only (no IMDS)
        String region = resolveRegion();
        if (region != null) {
            builder.region(region);
        }
        else if (endpointUrl == null) {
            System.err.println(
                    "Unable to determine AWS region. Set AWS_REGION or configure a default region in ~/.aws/config.");
            return null;
        }

        boolean sourceConfigured = CredentialSources.anyConfigured();
        S3CredentialsProvider chain = SdkCredentialsProviders.defaultChain();
        try {
            chain.credentials();
            builder.credentials(chain);
        }
        catch (SdkClientException e) {
            if (sourceConfigured) {
                System.err.println("Unable to load AWS credentials:");
                System.err.println(e.getMessage());
                return null;
            }
            builder.anonymous();
        }

        return builder.build();
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

    /// Resolves the AWS region from system property, env vars, and SDK profile files (no network I/O).
    static String resolveRegion() {
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

        String profile = ProfileFileSystemSetting.AWS_PROFILE.getStringValueOrThrow();
        return ProfileFile.defaultProfileFile().profile(profile)
                .flatMap(p -> p.property(ProfileProperty.REGION))
                .orElse(null);
    }

    Path toPath() {
        return Path.of(file);
    }
}
