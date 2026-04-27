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

/// Shared test contract for the `inspect dictionary` command.
interface InspectDictionaryCommandContract {

    String plainFile();

    String dictFile();

    String nonexistentFile();

    @Test
    default void printsDictionaryEntriesForDictColumn() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile(), "--column", "category");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                category
                +----+-------+--------+-------+
                | RG | Index | Length | Value |
                +----+-------+--------+-------+
                |  0 |     0 |      1 |     A |
                |    |     1 |      1 |     B |
                |    |     2 |      1 |     C |
                +----+-------+--------+-------+""");
    }

    @Test
    default void printsNoDictionaryMessageForPlainColumn() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", plainFile(), "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id
                Row Group 0: no dictionary (column is not dictionary-encoded)""");
    }

    @Test
    default void limitsDictionaryEntries() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile(), "--column", "category",
                "--limit", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                category
                Row Group 0 - dictionary has 3 entries (showing first 2)
                +----+-------+--------+-------+
                | RG | Index | Length | Value |
                +----+-------+--------+-------+
                |  0 |     0 |      1 |     A |
                |    |     1 |      1 |     B |
                +----+-------+--------+-------+""");
    }

    @Test
    default void zeroLimitPrintsAllDictionaryEntries() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile(), "--column", "category",
                "--limit", "0");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).doesNotContain("showing first")
                .contains("|    |     2 |      1 |     C |");
    }

    @Test
    default void rejectsUnknownColumn() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile(), "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("Unknown column");
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", nonexistentFile(), "--column", "id");

        assertThat(result.exitCode()).isNotZero();
    }
}
