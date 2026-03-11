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
class HeadCommandTest {

    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void printsAsciiTableWithHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("+")
                .contains("|");
    }

    @Test
    void printsFirstRowValues(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("1")
                .contains("100");
    }

    @Test
    void respectsRowLimit(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", TEST_FILE, "-n", "1");

        assertThat(result.exitCode()).isZero();
        // With 1 row the table has: separator, header, separator, 1 data row, separator = 5 lines
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(1);
    }

    @Test
    void defaultsToTenRows(QuarkusMainLauncher launcher) {
        // plain_uncompressed.parquet has 3 rows, so all 3 are returned even with default of 10
        LaunchResult result = launcher.launch("head", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(3);
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
    }
}
