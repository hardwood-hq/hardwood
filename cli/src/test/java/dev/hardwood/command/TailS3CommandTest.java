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
class TailS3CommandTest extends AbstractS3CommandTest {

    @Test
    void printsAsciiTableWithHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("+")
                .contains("|");
    }

    @Test
    void printsLastRowByDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", S3_FILE, "-n", "1");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("3").contains("300");
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(1);
    }

    @Test
    void printsLastTwoRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", S3_FILE, "-n", "2");

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
        LaunchResult result = launcher.launch("tail", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(3);
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
