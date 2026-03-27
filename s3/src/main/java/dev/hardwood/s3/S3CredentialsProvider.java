/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

/// Supplies [S3Credentials] for signing S3 requests.
///
/// Called per signing operation. The provider is responsible for caching
/// and refreshing credentials — Hardwood does not cache them.
///
/// Implement as a lambda for static credentials:
/// ```java
/// S3CredentialsProvider provider = () -> S3Credentials.of("AKIA...", "secret");
/// ```
///
/// Or use `hardwood-aws-auth` for the full AWS credential chain.
@FunctionalInterface
public interface S3CredentialsProvider {

    /// Returns the current credentials.
    S3Credentials credentials();
}
