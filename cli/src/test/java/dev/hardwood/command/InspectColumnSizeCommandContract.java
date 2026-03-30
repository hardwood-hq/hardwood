/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;

import static org.assertj.core.api.Assertions.assertThat;

/// Shared test contract for the `inspect column-size` command.
interface InspectColumnSizeCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysRankedColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Rank")
                .contains("Column")
                .contains("Compressed")
                .contains("Uncompressed")
                .contains("Ratio");
    }

    @Test
    default void listsAllColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value");
    }

    @Test
    default void rank1IsLargestCompressedColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        List<String> dataRows = result.getOutput().lines()
                .filter(line -> line.startsWith("|") && !line.contains("Rank"))
                .toList();
        assertThat(dataRows).isNotEmpty();
        assertThat(dataRows.getFirst().split("\\|")[1].strip()).isEqualTo("1");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
