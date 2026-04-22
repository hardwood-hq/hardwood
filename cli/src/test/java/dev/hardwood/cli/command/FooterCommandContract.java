/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Shared test contract for the `footer` command.
interface FooterCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysFooterInfo() {
        Cli.Result result = Cli.launch("footer", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                File Size:     722 bytes
                Footer Offset: 178 bytes
                Footer Length: 536 bytes
                Leading Magic:  PAR1
                Trailing Magic: PAR1""");
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("footer", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
