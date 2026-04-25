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

/// Shared test contract for the `info` command.
interface InfoCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysFileInfo() {
        Cli.Result result = Cli.launch("info", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                Format Version:    2
                Created By:        parquet-cpp-arrow version
                Row Groups:        1
                Total Rows:        3
                Uncompressed Size: 174 B
                Compressed Size:   174 B""");
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("info", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
