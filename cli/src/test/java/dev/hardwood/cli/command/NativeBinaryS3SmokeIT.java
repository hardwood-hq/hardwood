/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.quarkus.test.junit.main.QuarkusMainLauncher;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the native CLI binary against S3. Proves the AWS SDK baked
/// into the native image can reach an S3 endpoint, authenticate, and read an
/// object using the `-D` system properties injected by `S3MockTestResource`.
@QuarkusMainIntegrationTest
@WithTestResource(S3MockTestResource.class)
class NativeBinaryS3SmokeIT {

    @Test
    void readsFileFromS3(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", "s3://test-bucket/plain_uncompressed.parquet");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("message schema");
    }
}
