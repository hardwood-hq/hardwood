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
class ConvertS3CommandTest extends AbstractS3CommandTest {

    @Test
    void csvOutputContainsHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", S3_FILE, "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).startsWith("id,value");
    }

    @Test
    void csvOutputContainsRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", S3_FILE, "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("1,100")
                .contains("2,200")
                .contains("3,300");
    }

    @Test
    void jsonOutputIsArray(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", S3_FILE, "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput().trim()).startsWith("[").endsWith("]");
    }

    @Test
    void jsonOutputContainsFields(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", S3_FILE, "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("\"id\"")
                .contains("\"value\"")
                .contains("\"1\"")
                .contains("\"100\"");
    }

    @Test
    void columnsFilterOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", S3_FILE, "--to", "csv", "--columns", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .startsWith("id")
                .doesNotContain("value");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", S3_NONEXISTENT_FILE, "--to", "csv");

        assertThat(result.exitCode()).isNotZero();
    }
}
