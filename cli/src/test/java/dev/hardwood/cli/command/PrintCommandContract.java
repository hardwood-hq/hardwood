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

/// Shared test contract for the `print` command.
interface PrintCommandContract {

    String plainFile();

    String byteArrayFile();

    String deepNestedFile();

    String listFile();

    String nonexistentFile();

    String unsignedIntFile();

    String multiRowGroupIntFile();

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
    default void tailOnMultipleRowGroups(QuarkusMainLauncher launcher) {
        // filter_pushdown_int.parquet has three row groups of 100 rows each.
        // The tail must reflect the last rows of the file regardless of the
        // row-group layout; this also exercises the code path that skips
        // row groups outside the tail.
        LaunchResult result = launcher.launch("print", "-f", multiRowGroupIntFile(), "-n", "-3");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +-----+-------+---------+
                | id  | value | label   |
                +-----+-------+---------+
                | 298 | 298   | rg3_298 |
                | 299 | 299   | rg3_299 |
                | 300 | 300   | rg3_300 |
                +-----+-------+---------+""");
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
    default void columnsFilterOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "--columns", "value");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +-------+
                | value |
                +-------+
                | 100   |
                | 200   |
                | 300   |
                +-------+""");
    }

    @Test
    default void columnsFilterWithNestedStruct(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", deepNestedFile(), "--columns", "name,account", "-mw", "150");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +---------+-------------------------------------------------------------------------------------------------------------------------+
                | name    | account                                                                                                                 |
                +---------+-------------------------------------------------------------------------------------------------------------------------+
                | Alice   | { id : ACC-001, organization : { name : Acme Corp, address : { street : 123 Main St, city : New York, zip : 10001 } } } |
                | Bob     | { id : ACC-002, organization : { name : TechStart, address : null } }                                                   |
                | Charlie | { id : ACC-003, organization : null }                                                                                   |
                | Diana   | null                                                                                                                    |
                +---------+-------------------------------------------------------------------------------------------------------------------------+""");
    }

    @Test
    default void columnsFilterExcludesNestedColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", deepNestedFile(), "--columns", "customer_id,name");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +-------------+---------+
                | customer_id | name    |
                +-------------+---------+
                | 1           | Alice   |
                | 2           | Bob     |
                | 3           | Charlie |
                | 4           | Diana   |
                +-------------+---------+""");
    }

    @Test
    default void columnsFilterWithNestedSubField(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", deepNestedFile(), "--columns", "name,account.id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +---------+------------------+
                | name    | account          |
                +---------+------------------+
                | Alice   | { id : ACC-001 } |
                | Bob     | { id : ACC-002 } |
                | Charlie | { id : ACC-003 } |
                | Diana   | null             |
                +---------+------------------+""");
    }

    @Test
    default void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "--columns", "unknown");

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    default void explicitAllShowsEveryRow(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", plainFile(), "-n", "ALL");

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
    default void transposesRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", byteArrayFile(), "-tp", "-n", "3");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +-----------------+-------+
                |              id |     1 |
                +-----------------+-------+
                |  prefix_strings | apple |
                +-----------------+-------+
                | varying_strings | hello |
                +-----------------+-------+
                +-----------------+-------------+
                |              id |           2 |
                +-----------------+-------------+
                |  prefix_strings | application |
                +-----------------+-------------+
                | varying_strings |       world |
                +-----------------+-------------+
                +-----------------+-----------+
                |              id |         3 |
                +-----------------+-----------+
                |  prefix_strings |     apply |
                +-----------------+-----------+
                | varying_strings | wonderful |
                +-----------------+-----------+""");
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
        LaunchResult result = launcher.launch("print", "-f", deepNestedFile(), "-mw", "150");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +-------------+---------+-------------------------------------------------------------------------------------------------------------------------+
                | customer_id | name    | account                                                                                                                 |
                +-------------+---------+-------------------------------------------------------------------------------------------------------------------------+
                | 1           | Alice   | { id : ACC-001, organization : { name : Acme Corp, address : { street : 123 Main St, city : New York, zip : 10001 } } } |
                | 2           | Bob     | { id : ACC-002, organization : { name : TechStart, address : null } }                                                   |
                | 3           | Charlie | { id : ACC-003, organization : null }                                                                                   |
                | 4           | Diana   | null                                                                                                                    |
                +-------------+---------+-------------------------------------------------------------------------------------------------------------------------+""");
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
    default void displaysListColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", listFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                +----+-----------+--------------+
                | id | tags      | scores       |
                +----+-----------+--------------+
                | 1  | [a, b, c] | [10, 20, 30] |
                | 2  | []        | [100]        |
                | 3  | null      | [1, 2]       |
                | 4  | [single]  | null         |
                +----+-----------+--------------+""");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    default void displaysUnsignedIntegersAsPositiveNumbers(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", unsignedIntFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
            +----+------------+----------------------+
            | id | uint32_val | uint64_val           |
            +----+------------+----------------------+
            | 1  | 0          | 0                    |
            | 2  | 2147483647 | 9223372036854775807  |
            | 3  | 4294967295 | 18446744073709551615 |
            +----+------------+----------------------+""");
    }
}
