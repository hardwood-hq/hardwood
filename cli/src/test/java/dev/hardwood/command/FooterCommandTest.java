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
class FooterCommandTest {

    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();

    @Test
    void displaysFooterInfo(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("File Size:")
                .contains("Footer Offset:")
                .contains("Footer Length:");
    }

    @Test
    void displaysMagicBytes(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Leading Magic:  PAR1")
                .contains("Trailing Magic: PAR1");
    }

    @Test
    void footerOffsetIsWithinFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        // Extract footer offset and file size, verify offset < file size
        String output = result.getOutput();
        long fileSize = parseLabelledLong(output, "File Size:");
        long footerOffset = parseLabelledLong(output, "Footer Offset:");
        long footerLength = parseLabelledLong(output, "Footer Length:");
        assertThat(footerOffset).isPositive().isLessThan(fileSize);
        assertThat(footerOffset + footerLength).isEqualTo(fileSize - 8);
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
    }

    private static long parseLabelledLong(String output, String label) {
        return output.lines()
                .filter(l -> l.contains(label))
                .mapToLong(l -> Long.parseLong(l.replaceAll("[^0-9]", "")))
                .findFirst()
                .orElseThrow();
    }
}
