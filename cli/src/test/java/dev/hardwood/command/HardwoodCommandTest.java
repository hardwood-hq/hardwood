/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class HardwoodCommandTest {

    @Test
    @Launch("--help")
    void helpFlagPrintsUsage(LaunchResult result) {
        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("Usage:", "hardwood");
    }

    @Test
    @Launch("help")
    void helpSubcommandPrintsUsage(LaunchResult result) {
        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("Usage:", "hardwood");
    }

    @Test
    @Launch("--version")
    void versionFlagPrintsVersion(LaunchResult result) {
        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("hardwood");
    }

    @Test
    void helpForUnknownSubcommandFails(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("help", "unknown");
        assertThat(result.exitCode()).isNotZero();
    }
}
