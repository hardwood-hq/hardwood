/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class ConvertCommandTest {

    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void csvOutputContainsHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).startsWith("id,value");
    }

    @Test
    void csvOutputContainsRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("1,100")
                .contains("2,200")
                .contains("3,300");
    }

    @Test
    void jsonOutputIsArray(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput().trim()).startsWith("[").endsWith("]");
    }

    @Test
    void jsonOutputContainsFields(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("\"id\"")
                .contains("\"value\"")
                .contains("\"1\"")
                .contains("\"100\"");
    }

    @Test
    void columnsFilterOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "csv", "--columns", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .startsWith("id")
                .doesNotContain("value");
    }

    @Test
    void outputToFile(@TempDir Path tempDir, QuarkusMainLauncher launcher) throws IOException {
        Path out = tempDir.resolve("output.csv");

        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "csv", "-o", out.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(Files.readString(out))
                .startsWith("id,value")
                .contains("1,100");
    }

    @Test
    void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE, "--to", "csv", "--columns", "unknown");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", "gs://bucket/data.parquet", "--to", "csv");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", "nonexistent.parquet", "--to", "csv");

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void requiresFormatFlag(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", TEST_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
