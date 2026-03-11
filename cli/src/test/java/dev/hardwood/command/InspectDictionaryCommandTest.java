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
class InspectDictionaryCommandTest {

    private final String PLAIN_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();
    private final String DICT_FILE = this.getClass().getResource("/dictionary_uncompressed.parquet").getPath();

    @Test
    void printsDictionaryEntriesForDictColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", DICT_FILE, "--column", "category");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Dictionary size")
                .contains("Row Group 0");
    }

    @Test
    void printsNoDictionaryMessageForPlainColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", PLAIN_FILE, "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("No dictionary");
    }

    @Test
    void requiresColumnOption(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", DICT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", DICT_FILE, "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", "gs://bucket/data.parquet",
                "--column", "id");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", "nonexistent.parquet",
                "--column", "id");

        assertThat(result.exitCode()).isNotZero();
    }
}
