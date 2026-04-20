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

/// Shared test contract for the `inspect pages` command.
interface InspectPagesCommandContract {

    String plainFile();

    String dictFile();

    String pageIndexFile();

    String nestedFile();

    String nonexistentFile();

    @Test
    default void printsPageDetails(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+------+----------+------------+--------+
                | RG | Page  | Type | Encoding | Compressed | Values |
                +----+-------+------+----------+------------+--------+
                |  0 |     0 | DATA |    PLAIN |       24 B |      3 |
                +====+=======+======+==========+============+========+
                |    | Total |      |          |       24 B |      3 |
                +----+-------+------+----------+------------+--------+

                value
                +----+-------+------+----------+------------+--------+
                | RG | Page  | Type | Encoding | Compressed | Values |
                +----+-------+------+----------+------------+--------+
                |  0 |     0 | DATA |    PLAIN |       24 B |      3 |
                +====+=======+======+==========+============+========+
                |    | Total |      |          |       24 B |      3 |
                +----+-------+------+----------+------------+--------+""");
    }

    @Test
    default void printsDictionaryPageForDictFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", dictFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+------+----------+------------+--------+
                | RG | Page  | Type | Encoding | Compressed | Values |
                +----+-------+------+----------+------------+--------+
                |  0 |     0 | DATA |    PLAIN |       40 B |      5 |
                +====+=======+======+==========+============+========+
                |    | Total |      |          |       40 B |      5 |
                +----+-------+------+----------+------------+--------+

                category
                +----+-------+------+----------+------------+--------+
                | RG | Page  | Type | Encoding | Compressed | Values |
                +----+-------+------+----------+------------+--------+
                |  0 |  dict | DICT |    PLAIN |       15 B |      3 |
                |    |     0 | DATA | RLE_DICT |        4 B |      5 |
                +====+=======+======+==========+============+========+
                |    | Total |      |          |       19 B |      5 |
                +----+-------+------+----------+------------+--------+""");
    }

    @Test
    default void columnFilterRestrictsOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", plainFile(), "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+------+----------+------------+--------+
                | RG | Page  | Type | Encoding | Compressed | Values |
                +----+-------+------+----------+------------+--------+
                |  0 |     0 | DATA |    PLAIN |       24 B |      3 |
                +====+=======+======+==========+============+========+
                |    | Total |      |          |       24 B |      3 |
                +----+-------+------+----------+------------+--------+""");
    }

    @Test
    default void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", plainFile(), "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    default void enrichesOutputWithPageIndex(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", pageIndexFile(), "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+---------+----------+-----------+------------+--------+------+------+-------+
                | RG | Page  | Type    | Encoding | First Row | Compressed | Values | Min  | Max  | Nulls |
                +----+-------+---------+----------+-----------+------------+--------+------+------+-------+
                |  0 |     0 | DATA_V2 |    PLAIN |         0 |     8.0 KB |   1024 |    0 | 1023 |     0 |
                |    |     1 | DATA_V2 |    PLAIN |      1024 |     8.0 KB |   1024 | 1024 | 2047 |     0 |
                |    |     2 | DATA_V2 |    PLAIN |      2048 |     8.0 KB |   1024 | 2048 | 3071 |     0 |
                |    |     3 | DATA_V2 |    PLAIN |      3072 |     8.0 KB |   1024 | 3072 | 4095 |     0 |
                |    |     4 | DATA_V2 |    PLAIN |      4096 |     8.0 KB |   1024 | 4096 | 5119 |     0 |
                |    |     5 | DATA_V2 |    PLAIN |      5120 |     8.0 KB |   1024 | 5120 | 6143 |     0 |
                |    |     6 | DATA_V2 |    PLAIN |      6144 |     8.0 KB |   1024 | 6144 | 7167 |     0 |
                |    |     7 | DATA_V2 |    PLAIN |      7168 |     8.0 KB |   1024 | 7168 | 8191 |     0 |
                |    |     8 | DATA_V2 |    PLAIN |      8192 |     8.0 KB |   1024 | 8192 | 9215 |     0 |
                |    |     9 | DATA_V2 |    PLAIN |      9216 |     6.1 KB |    784 | 9216 | 9999 |     0 |
                +====+=======+=========+==========+===========+============+========+======+======+=======+
                |    | Total |         |          |           |    78.1 KB |  10000 |      |      |     0 |
                +----+-------+---------+----------+-----------+------------+--------+------+------+-------+""");
    }

    @Test
    default void noStatsSuppressesPageIndexColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", pageIndexFile(), "--column", "id", "--no-stats");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("| RG | Page  | Type    | Encoding | Compressed | Values |")
                .doesNotContain("First Row")
                .doesNotContain("Min")
                .doesNotContain("Max")
                .doesNotContain("Nulls");
    }

    @Test
    default void columnFilterAcceptsNestedPath(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", nestedFile(), "--column", "tags.list.element");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .startsWith("tags.list.element\n")
                .contains("| DATA |");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
