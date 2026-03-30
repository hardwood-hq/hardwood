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

/// Shared test contract for the `head` command.
/// Implementing classes provide file paths via [plainFile()] and [nonexistentFile()].
interface HeadCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void printsAsciiTableWithHeaders(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("+")
                .contains("|");
    }

    @Test
    default void printsFirstRowValues(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("1")
                .contains("100");
    }

    @Test
    default void respectsRowLimit(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", plainFile(), "-n", "1");

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(1);
    }

    @Test
    default void defaultsToTenRows(QuarkusMainLauncher launcher) {
        // plain_uncompressed.parquet has 3 rows, so all 3 are returned even with default of 10
        LaunchResult result = launcher.launch("head", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        long dataLines = result.getOutput().lines()
                .filter(l -> l.startsWith("|") && !l.contains("id"))
                .count();
        assertThat(dataLines).isEqualTo(3);
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
