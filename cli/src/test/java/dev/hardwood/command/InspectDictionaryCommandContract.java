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

/// Shared test contract for the `inspect dictionary` command.
interface InspectDictionaryCommandContract {

    String plainFile();

    String dictFile();

    String nonexistentFile();

    @Test
    default void printsDictionaryEntriesForDictColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", dictFile(), "--column", "category");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Dictionary size")
                .contains("Row Group 0");
    }

    @Test
    default void printsNoDictionaryMessageForPlainColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", plainFile(), "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("No dictionary");
    }

    @Test
    default void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", dictFile(), "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", nonexistentFile(), "--column", "id");

        assertThat(result.exitCode()).isNotZero();
    }
}
