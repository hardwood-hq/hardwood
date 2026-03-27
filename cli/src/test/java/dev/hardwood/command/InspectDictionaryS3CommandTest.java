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
class InspectDictionaryS3CommandTest extends AbstractS3CommandTest {

    @Test
    void printsDictionaryEntriesForDictColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", S3_DICT_FILE, "--column", "category");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Dictionary size")
                .contains("Row Group 0");
    }

    @Test
    void printsNoDictionaryMessageForPlainColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", S3_FILE, "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("No dictionary");
    }

    @Test
    void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", S3_DICT_FILE, "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "dictionary", "-f", S3_NONEXISTENT_FILE, "--column", "id");

        assertThat(result.exitCode()).isNotZero();
    }
}
