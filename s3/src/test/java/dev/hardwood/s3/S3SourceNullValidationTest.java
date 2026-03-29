/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for null argument validation in [S3Source] factory methods.
class S3SourceNullValidationTest {

    static S3Source source;

    @BeforeAll
    static void setup() {
        source = S3Source.builder()
                .endpoint("http://localhost:1234")
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"))
                .build();
    }

    @AfterAll
    static void tearDown() {
        source.close();
    }

    @Test
    void inputFileRejectsNullBucket() {
        assertThatThrownBy(() -> source.inputFile(null, "key"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bucket must not be null");
    }

    @Test
    void inputFileRejectsNullKey() {
        assertThatThrownBy(() -> source.inputFile("bucket", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("key must not be null");
    }

    @Test
    void inputFileUriRejectsNull() {
        assertThatThrownBy(() -> source.inputFile((String) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("uri must not be null");
    }

    @Test
    void inputFilesInBucketRejectsNullBucket() {
        assertThatThrownBy(() -> source.inputFilesInBucket(null, "key1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bucket must not be null");
    }

    @Test
    void inputFilesInBucketRejectsNullKey() {
        assertThatThrownBy(() -> source.inputFilesInBucket("bucket", "key1", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("key must not be null");
    }

    @Test
    void inputFilesRejectsNullUri() {
        assertThatThrownBy(() -> source.inputFiles("s3://bucket/key", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("uri must not be null");
    }
}
