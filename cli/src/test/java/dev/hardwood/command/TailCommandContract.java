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

/// Shared test contract for the `tail` command.
interface TailCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void printsAsciiTableWithHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("+")
                .contains("|");
    }

    @Test
    default void printsLastRowByDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", plainFile(), "-n", "1");

        assertThat(result.exitCode()).isZero();
        // Last row is (3, 300)
        assertThat(result.getOutput()).contains("3").contains("300");
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(1);
    }

    @Test
    default void printsLastTwoRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", plainFile(), "-n", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("2").contains("200");
        assertThat(result.getOutput()).contains("3").contains("300");
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(2);
    }

    @Test
    default void defaultsToAllRowsWhenCountExceedsTotal(QuarkusMainLauncher launcher) {
        // File has 3 rows, default -n 10 returns all 3
        LaunchResult result = launcher.launch("tail", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(3);
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
