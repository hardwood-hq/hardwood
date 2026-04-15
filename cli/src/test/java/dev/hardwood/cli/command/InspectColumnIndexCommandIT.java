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

@QuarkusMainIntegrationTest
@WithTestResource(QuietLoggingTestResource.class)
class InspectColumnIndexCommandIT {

    private final String TEST_FILE = getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void printsNotAvailableMessage(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-index", "-f", TEST_FILE, "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("not yet available");
    }

    @Test
    void requiresColumnOption(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-index", "-f", TEST_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
