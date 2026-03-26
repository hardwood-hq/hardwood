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

/// Shared test contract for the `print` command.
interface PrintCommandContract {

    String plainFile();

    String byteArrayFile();

    String deepNestedFile();

    String nonexistentFile();

    @Test
    default void printsAsciiTableDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 1  | 100   |
                | 2  | 200   |
                | 3  | 300   |
                +----+-------+""");
    }

    @Test
    default void lineSeparatorBetweenRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "--row-delimiter");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 1  | 100   |
                +----+-------+
                | 2  | 200   |
                +----+-------+
                | 3  | 300   |
                +----+-------+""");
    }

    @Test
    default void tail(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "-n", "-2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 2  | 200   |
                | 3  | 300   |
                +----+-------+""");
    }

    @Test
    default void head(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "-n", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 1  | 100   |
                | 2  | 200   |
                +----+-------+""");
    }

    @Test
    default void showsRowIndexWhenEnabled(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "-ri");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----------+----+-------+
                | rowIndex | id | value |
                +----------+----+-------+
                | 0        | 1  | 100   |
                | 1        | 2  | 200   |
                | 2        | 3  | 300   |
                +----------+----+-------+""");
    }

    @Test
    default void byteArrayAsString(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", byteArrayFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+----------------+-----------------+
                | id | prefix_strings | varying_strings |
                +----+----------------+-----------------+
                | 1  | apple          | hello           |
                | 2  | application    | world           |
                | 3  | apply          | wonderful       |
                | 4  | banana         | wonder          |
                | 5  | bandana        | wander          |
                | 6  | band           | wandering       |
                | 7  | bandwidth      | test            |
                | 8  | ban            | testing         |
                +----+----------------+-----------------+""");
    }

    @Test
    default void displaysNestedStructFields(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", deepNestedFile(), "-mw", "120");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +-------------+---------+------------------------------------------------------------------------------------------------------------+
                | customer_id | name    | account                                                                                                    |
                +-------------+---------+------------------------------------------------------------------------------------------------------------+
                | 1           | Alice   | {id: ACC-001, organization: {name: Acme Corp, address: {street: 123 Main St, city: New York, zip: 10001}}} |
                | 2           | Bob     | {id: ACC-002, organization: {name: TechStart, address: null}}                                              |
                | 3           | Charlie | {id: ACC-003, organization: null}                                                                          |
                | 4           | Diana   | null                                                                                                       |
                +-------------+---------+------------------------------------------------------------------------------------------------------------+""");
    }

    @Test
    default void wrapsWhenTruncationDisabled(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", byteArrayFile(), "--no-truncate", "-mw", "5");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+-------+-------+
                | id | prefi | varyi |
                |    | x_str | ng_st |
                |    | ings  | rings |
                +----+-------+-------+
                | 1  | apple | hello |
                | 2  | appli | world |
                |    | catio |       |
                |    | n     |       |
                | 3  | apply | wonde |
                |    |       | rful  |
                | 4  | banan | wonde |
                |    | a     | r     |
                | 5  | banda | wande |
                |    | na    | r     |
                | 6  | band  | wande |
                |    |       | ring  |
                | 7  | bandw | test  |
                |    | idth  |       |
                | 8  | ban   | testi |
                |    |       | ng    |
                +----+-------+-------+""");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
