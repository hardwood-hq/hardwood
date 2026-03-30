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

/// Shared test contract for the `footer` command.
interface FooterCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysFooterInfo(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("File Size:")
                .contains("Footer Offset:")
                .contains("Footer Length:");
    }

    @Test
    default void displaysMagicBytes(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Leading Magic:  PAR1")
                .contains("Trailing Magic: PAR1");
    }

    @Test
    default void footerOffsetIsWithinFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        String output = result.getOutput();
        long fileSize = parseLabelledLong(output, "File Size:");
        long footerOffset = parseLabelledLong(output, "Footer Offset:");
        long footerLength = parseLabelledLong(output, "Footer Length:");
        assertThat(footerOffset).isPositive().isLessThan(fileSize);
        assertThat(footerOffset + footerLength).isEqualTo(fileSize - 8);
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", nonexistentFile());

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
