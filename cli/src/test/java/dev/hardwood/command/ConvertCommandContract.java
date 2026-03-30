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

/// Shared test contract for the `convert` command.
interface ConvertCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void csvOutputContainsHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).startsWith("id,value");
    }

    @Test
    default void csvOutputContainsRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("1,100")
                .contains("2,200")
                .contains("3,300");
    }

    @Test
    default void jsonOutputIsArray(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput().trim()).startsWith("[").endsWith("]");
    }

    @Test
    default void jsonOutputContainsFields(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("\"id\"")
                .contains("\"value\"")
                .contains("\"1\"")
                .contains("\"100\"");
    }

    @Test
    default void columnsFilterOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv", "--columns", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .startsWith("id")
                .doesNotContain("value");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", nonexistentFile(), "--to", "csv");

        assertThat(result.exitCode()).isNotZero();
    }
}
