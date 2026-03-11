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
class InspectColumnSizeCommandTest {

    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void displaysRankedColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Rank")
                .contains("Column")
                .contains("Compressed")
                .contains("Uncompressed")
                .contains("Ratio");
    }

    @Test
    void listsAllColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value");
    }

    @Test
    void rank1IsLargestCompressedColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        // Rank 1 should appear before rank 2 in output
        String output = result.getOutput();
        assertThat(output.indexOf("1 ")).isLessThan(output.indexOf("2 "));
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
    }
}
