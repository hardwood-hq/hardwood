/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests the choice between credentials and anonymous access in [S3Source.Builder].
class S3SourceCredentialsValidationTest {

    private static S3Source.Builder builder() {
        return S3Source.builder().region("us-east-1");
    }

    @Test
    void rejectsNeitherCredentialsNorAnonymous() {
        assertThatThrownBy(() -> builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("credentials or anonymous access must be set");
    }

    @Test
    void rejectsCredentialsAndAnonymous() {
        assertThatThrownBy(() -> builder().credentials(S3Credentials.of("access", "secret")).anonymous().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("anonymous access and credentials are mutually exclusive");
    }

    @Test
    void acceptsAnonymous() {
        assertThatNoException().isThrownBy(() -> builder().anonymous().build().close());
    }

    @Test
    void rejectsNullProvider() {
        assertThatThrownBy(() -> builder().credentials((S3CredentialsProvider) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("provider must not be null");
    }

    @Test
    void rejectsNullCredentials() {
        assertThatThrownBy(() -> builder().credentials((S3Credentials) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("credentials must not be null");
    }
}
