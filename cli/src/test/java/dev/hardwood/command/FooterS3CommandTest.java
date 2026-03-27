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
class FooterS3CommandTest extends AbstractS3CommandTest {

    @Test
    void displaysFooterInfo(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("File Size:")
                .contains("Footer Offset:")
                .contains("Footer Length:");
    }

    @Test
    void displaysMagicBytes(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Leading Magic:  PAR1")
                .contains("Trailing Magic: PAR1");
    }

    @Test
    void footerOffsetIsWithinFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        String output = result.getOutput();
        long fileSize = parseLabelledLong(output, "File Size:");
        long footerOffset = parseLabelledLong(output, "Footer Offset:");
        long footerLength = parseLabelledLong(output, "Footer Length:");
        assertThat(footerOffset).isPositive().isLessThan(fileSize);
        assertThat(footerOffset + footerLength).isEqualTo(fileSize - 8);
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", S3_NONEXISTENT_FILE);

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
