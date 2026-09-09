/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests that [S3Source.Builder#build()] rejects a temp directory the
/// [RangeBacking#SPARSE_TEMPFILE] backing could not use, instead of
/// deferring the failure to the first [S3InputFile#open()].
class S3SourceTempDirValidationTest {

    @TempDir
    Path tempDir;

    @Test
    void sparseTempFileRejectsMissingTempDir() {
        Path missing = tempDir.resolve("does-not-exist");

        assertThatThrownBy(() -> builder()
                .rangeBacking(RangeBacking.SPARSE_TEMPFILE)
                .tempDir(missing)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("tempDir does not exist or is not a directory: " + missing
                        + " (required by RangeBacking.SPARSE_TEMPFILE)");
    }

    @Test
    void sparseTempFileAcceptsExistingTempDir() {
        assertThatCode(() -> builder()
                .rangeBacking(RangeBacking.SPARSE_TEMPFILE)
                .tempDir(tempDir)
                .build()
                .close())
                .doesNotThrowAnyException();
    }

    @Test
    void noneIgnoresMissingTempDir() {
        assertThatCode(() -> builder()
                .tempDir(tempDir.resolve("does-not-exist"))
                .build()
                .close())
                .doesNotThrowAnyException();
    }

    private static S3Source.Builder builder() {
        return S3Source.builder()
                .endpoint("http://localhost:1234")
                .pathStyle(true)
                .credentials(S3Credentials.of("access", "secret"));
    }
}
