/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.aws.auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.s3.S3Credentials;
import dev.hardwood.s3.S3CredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;

class SdkCredentialsProvidersTest {

    @BeforeEach
    void setCredentials() {
        System.setProperty("aws.accessKeyId", "AKID_TEST");
        System.setProperty("aws.secretAccessKey", "secret_test");
    }

    @AfterEach
    void clearCredentials() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.sessionToken");
    }

    @Test
    void defaultChainResolvesFromSystemProperties() {
        S3CredentialsProvider provider = SdkCredentialsProviders.defaultChain();

        S3Credentials creds = provider.credentials();
        assertThat(creds.accessKeyId()).isEqualTo("AKID_TEST");
        assertThat(creds.secretAccessKey()).isEqualTo("secret_test");
        assertThat(creds.sessionToken()).isNull();
    }

    @Test
    void sessionTokenIsPropagated() {
        System.setProperty("aws.sessionToken", "token_test");

        S3CredentialsProvider provider = SdkCredentialsProviders.defaultChain();

        S3Credentials creds = provider.credentials();
        assertThat(creds.accessKeyId()).isEqualTo("AKID_TEST");
        assertThat(creds.secretAccessKey()).isEqualTo("secret_test");
        assertThat(creds.sessionToken()).isEqualTo("token_test");
    }
}
