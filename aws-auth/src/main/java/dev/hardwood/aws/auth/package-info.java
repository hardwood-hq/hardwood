/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Bridge between the AWS SDK credential chain and [dev.hardwood.s3.S3Source].
///
/// [SdkCredentialsProviders] adapts AWS SDK `CredentialsProvider` instances
/// into the [dev.hardwood.s3.S3CredentialsProvider] interface used by
/// `hardwood-s3`. The default chain resolves environment variables, profile
/// files and container and instance metadata; SSO and web-identity profiles
/// additionally need the AWS SDK's `sso`, `ssooidc` and `sts` modules on the
/// classpath.
package dev.hardwood.aws.auth;
