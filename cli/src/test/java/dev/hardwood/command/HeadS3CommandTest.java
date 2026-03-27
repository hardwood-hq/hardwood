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
class HeadS3CommandTest extends AbstractS3CommandTest {

    @Test
    void printsAsciiTableWithHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("+")
                .contains("|");
    }

    @Test
    void printsFirstRowValues(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("1")
                .contains("100");
    }

    @Test
    void respectsRowLimit(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", S3_FILE, "-n", "1");

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(1);
    }

    @Test
    void defaultsToTenRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(3);
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
