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
                Row Group 0 / category
                  Dictionary size: 3 entries
                  [   0] A
                  [   1] B
                  [   2] C""");
    }

    @Test
    default void printsNoDictionaryMessageForPlainColumn() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", plainFile(), "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                Row Group 0 / id
                  No dictionary (column is not dictionary-encoded)""");
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
