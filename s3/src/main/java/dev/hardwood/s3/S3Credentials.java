/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

/// Long-term or temporary AWS credentials.
///
/// This is a plain record with no AWS SDK dependency. Use [#of(String, String)]
/// for long-term credentials (access key + secret key) or the canonical
/// constructor to include a session token for temporary credentials
/// (STS AssumeRole, EC2/ECS instance profile, IRSA, etc.).
///
/// @param accessKeyId     the AWS access key ID
/// @param secretAccessKey the AWS secret access key
/// @param sessionToken    the session token, or `null` for long-term credentials
public record S3Credentials(String accessKeyId, String secretAccessKey, String sessionToken) {

    /// Creates long-term credentials (no session token).
    ///
    /// @param accessKeyId     the AWS access key ID
    /// @param secretAccessKey the AWS secret access key
    /// @return credentials without a session token
    public static S3Credentials of(String accessKeyId, String secretAccessKey) {
        return new S3Credentials(accessKeyId, secretAccessKey, null);
    }
}
