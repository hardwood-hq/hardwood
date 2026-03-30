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
class InspectPagesS3CommandTest extends AbstractS3CommandTest {

    @Test
    void printsPageTypeAndEncoding(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("DATA_PAGE")
                .contains("Encoding")
                .contains("PLAIN");
    }

    @Test
    void printsRowGroupAndColumnHeader(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Row Group 0")
                .contains("id");
    }

    @Test
    void printsDictionaryPageForDictFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", S3_DICT_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("DICTIONARY_PAGE");
    }

    @Test
    void columnFilterRestrictsOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", S3_FILE, "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("/ id");
        assertThat(result.getOutput()).doesNotContain("/ value");
    }

    @Test
    void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", S3_FILE, "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
