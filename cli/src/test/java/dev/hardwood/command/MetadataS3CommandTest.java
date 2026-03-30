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
class MetadataS3CommandTest extends AbstractS3CommandTest {

    @Test
    void displaysMetadata(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Format Version:")
                .contains("Row Groups:")
                .contains("Total Rows:")
                .contains("Row Group 0");
    }

    @Test
    void displaysColumnChunkDetails(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value")
                .contains("Type")
                .contains("Codec")
                .contains("Compressed")
                .contains("Uncompressed");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
