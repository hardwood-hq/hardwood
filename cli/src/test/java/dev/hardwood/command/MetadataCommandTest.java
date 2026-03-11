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
class MetadataCommandTest {

    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void displaysMetadata(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Format Version:")
                .contains("Row Groups:")
                .contains("Total Rows:")
                .contains("Row Group 0");
    }

    @Test
    void displaysColumnChunkDetails(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("type:")
                .contains("codec:")
                .contains("compressed:")
                .contains("uncompressed:");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", "hdfs://namenode/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
    }
}
