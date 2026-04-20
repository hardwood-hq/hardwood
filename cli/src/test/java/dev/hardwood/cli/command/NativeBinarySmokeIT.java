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

/// Smoke test for the native CLI binary against a local Parquet file. Proves
/// the compiled binary boots, parses arguments, loads a file from disk, and
/// produces expected output. Per-command behavioural coverage lives in the
/// JVM `*CommandTest` classes.
@QuarkusMainIntegrationTest
@WithTestResource(QuietLoggingTestResource.class)
class NativeBinarySmokeIT {

    private final String plainFile = getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void readsLocalFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", plainFile);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("message schema");
    }
}
