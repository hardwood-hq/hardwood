/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;

import static org.assertj.core.api.Assertions.assertThat;

/// Shared test contract for the `info` command.
/// Implementing classes provide file paths via [plainFile()] and [nonexistentFile()].
interface InfoCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysFileInfo(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("info", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Format Version:")
                .contains("Created By:")
                .contains("Row Groups:")
                .contains("Total Rows:")
                .contains("Uncompressed Size:")
                .contains("Compressed Size:");
    }

    @Test
    default void displaysCorrectRowCount(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("info", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("Total Rows:        3");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("info", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
