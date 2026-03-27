/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.aws.auth;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/// Bridges the AWS SDK credential chain to Hardwood's types.
///
/// This class requires `software.amazon.awssdk:auth` on the classpath.
/// Use the `hardwood-aws-auth` module dependency to pull it in.
public final class SdkCredentialsProviders {

    private SdkCredentialsProviders() {
    }

    /// Returns a provider backed by the full AWS default credential chain
    /// (env vars, `~/.aws/credentials`, EC2/ECS instance profile, SSO, web identity, etc.).
    public static S3CredentialsProvider defaultChain() {
        DefaultCredentialsProvider sdk = DefaultCredentialsProvider.builder().build();
        return () -> toHardwood(sdk.resolveCredentials());
    }

    /// Returns a provider for a specific named profile from `~/.aws/credentials`.
    ///
    /// @param profileName the profile name
    public static S3CredentialsProvider fromProfile(String profileName) {
        ProfileCredentialsProvider sdk = ProfileCredentialsProvider.builder().profileName(profileName).build();
        return () -> toHardwood(sdk.resolveCredentials());
    }

    private static S3Credentials toHardwood(AwsCredentials sdkCreds) {
        if (sdkCreds instanceof AwsSessionCredentials session) {
            return new S3Credentials(session.accessKeyId(), session.secretAccessKey(), session.sessionToken());
        }
        return S3Credentials.of(sdkCreds.accessKeyId(), sdkCreds.secretAccessKey());
    }
}
