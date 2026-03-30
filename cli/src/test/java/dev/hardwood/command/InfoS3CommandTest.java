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
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class InfoS3CommandTest extends AbstractS3CommandTest {

    @Test
    void displaysFileInfo(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("info", "-f", S3_FILE);

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
    void displaysCorrectRowCount(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("info", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("Total Rows:        3");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("info", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
