/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3SourceOutputTest {

    @Test
    void createsOutputsWithoutNetworkAccess() {
        try (S3Source source = S3Source.builder()
                .endpoint("http://127.0.0.1:1")
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .build()) {
            assertThat(source.outputFile("bucket", "dir/file.parquet")).isNotNull();
            assertThat(source.outputFile("s3://bucket/dir/file.parquet")).isNotNull();
        }
    }

    @Test
    void rejectsAnOutputOnAnAnonymousSource() {
        try (S3Source source = S3Source.builder().region("us-east-1").anonymous().build()) {
            assertThatThrownBy(() -> source.outputFile("bucket", "file.parquet"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot write s3://bucket/file.parquet through an anonymous source");
        }
    }

    @Test
    void rejectsAUriWithoutKey() {
        try (S3Source source = S3Source.builder().region("us-east-1").credentials(S3Credentials.of("a", "s")).build()) {
            assertThatThrownBy(() -> source.outputFile("s3://bucket/"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid S3 URI (missing key): s3://bucket/");
        }
    }

    @Test
    void rejectsAPartSizeBelowTheMinimum() {
        assertThatThrownBy(() -> S3Source.builder().uploadPartSize(5 * 1024 * 1024 - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Upload part size must be between 5242880 and 2147483639 bytes: 5242879");
    }
}
