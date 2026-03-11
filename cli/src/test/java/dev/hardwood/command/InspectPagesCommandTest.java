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
class InspectPagesCommandTest {

    private final String PLAIN_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();
    private final String DICT_FILE = this.getClass().getResource("/dictionary_uncompressed.parquet").getPath();

    @Test
    void printsPageTypeAndEncoding(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", PLAIN_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("DATA_PAGE")
                .contains("encoding:");
    }

    @Test
    void printsRowGroupAndColumnHeader(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", PLAIN_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Row Group 0")
                .contains("id");
    }

    @Test
    void printsDictionaryPageForDictFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", DICT_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("DICTIONARY_PAGE");
    }

    @Test
    void columnFilterRestrictsOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", PLAIN_FILE, "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("/ id");
        assertThat(result.getOutput()).doesNotContain("/ value");
    }

    @Test
    void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", PLAIN_FILE, "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
    }
}
