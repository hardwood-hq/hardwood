/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

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
        assertThat(result.getOutput()).isEqualTo("""
                File Size:     722 bytes
                Footer Offset: 178 bytes
                Footer Length: 536 bytes
                Leading Magic:  PAR1
                Trailing Magic: PAR1""");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("footer", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
