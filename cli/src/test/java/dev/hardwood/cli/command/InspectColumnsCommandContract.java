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

/// Shared test contract for the `inspect columns` command.
interface InspectColumnsCommandContract {

    String plainFile();

    String pageIndexFile();

    String nonexistentFile();

    @Test
    default void displaysRankedColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "columns", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +------+--------+-------+------------+--------------+--------+---------+
                | Rank | Column | Type  | Compressed | Uncompressed | Ratio  | # Pages |
                +------+--------+-------+------------+--------------+--------+---------+
                |    1 |     id | INT64 |       87 B |         87 B | 100.0% |       - |
                |    2 |  value | INT64 |       87 B |         87 B | 100.0% |       - |
                +------+--------+-------+------------+--------------+--------+---------+""");
    }

    @Test
    default void populatesPageCountWhenPageIndexAvailable(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "columns", "-f", pageIndexFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +------+--------+-------+------------+--------------+--------+---------+
                | Rank | Column | Type  | Compressed | Uncompressed | Ratio  | # Pages |
                +------+--------+-------+------------+--------------+--------+---------+
                |    1 |     id | INT64 |    78.4 KB |      78.4 KB | 100.0% |      10 |
                |    2 |  value | INT64 |    78.4 KB |      78.4 KB | 100.0% |      10 |
                +------+--------+-------+------------+--------------+--------+---------+""");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "columns", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
