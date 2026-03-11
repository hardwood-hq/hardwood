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
class TailCommandTest {

    // plain_uncompressed.parquet: 3 rows — (1,100), (2,200), (3,300)
    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void printsAsciiTableWithHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("+")
                .contains("|");
    }

    @Test
    void printsLastRowByDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", TEST_FILE, "-n", "1");

        assertThat(result.exitCode()).isZero();
        // Last row is (3, 300)
        assertThat(result.getOutput()).contains("3").contains("300");
        // First row (1, 100) should not appear as a data row
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(1);
    }

    @Test
    void printsLastTwoRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", TEST_FILE, "-n", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("2").contains("200");
        assertThat(result.getOutput()).contains("3").contains("300");
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(2);
    }

    @Test
    void defaultsToAllRowsWhenCountExceedsTotal(QuarkusMainLauncher launcher) {
        // File has 3 rows, default -n 10 returns all 3
        LaunchResult result = launcher.launch("tail", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(3);
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
    }
}
