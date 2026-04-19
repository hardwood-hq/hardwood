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

    String nonexistentFile();

    @Test
    default void printsPageDetails(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+-----------+----------+------------+--------+
                | RG | Page  | Type      | Encoding | Compressed | Values |
                +----+-------+-----------+----------+------------+--------+
                |  0 |     0 | DATA_PAGE |    PLAIN |       24 B |      3 |
                +====+=======+===========+==========+============+========+
                |    | Total |           |          |       24 B |      3 |
                +----+-------+-----------+----------+------------+--------+

                value
                +----+-------+-----------+----------+------------+--------+
                | RG | Page  | Type      | Encoding | Compressed | Values |
                +----+-------+-----------+----------+------------+--------+
                |  0 |     0 | DATA_PAGE |    PLAIN |       24 B |      3 |
                +====+=======+===========+==========+============+========+
                |    | Total |           |          |       24 B |      3 |
                +----+-------+-----------+----------+------------+--------+""");
    }

    @Test
    default void printsDictionaryPageForDictFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", dictFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+-----------+----------+------------+--------+
                | RG | Page  | Type      | Encoding | Compressed | Values |
                +----+-------+-----------+----------+------------+--------+
                |  0 |     0 | DATA_PAGE |    PLAIN |       40 B |      5 |
                +====+=======+===========+==========+============+========+
                |    | Total |           |          |       40 B |      5 |
                +----+-------+-----------+----------+------------+--------+

                category
                +----+-------+-----------------+----------------+------------+--------+
                | RG | Page  | Type            | Encoding       | Compressed | Values |
                +----+-------+-----------------+----------------+------------+--------+
                |  0 |  dict | DICTIONARY_PAGE |          PLAIN |       15 B |      3 |
                |    |     0 |       DATA_PAGE | RLE_DICTIONARY |        4 B |      5 |
                +====+=======+=================+================+============+========+
                |    | Total |                 |                |       19 B |      5 |
                +----+-------+-----------------+----------------+------------+--------+""");
    }

    @Test
    default void columnFilterRestrictsOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", plainFile(), "--column", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                +----+-------+-----------+----------+------------+--------+
                | RG | Page  | Type      | Encoding | Compressed | Values |
                +----+-------+-----------+----------+------------+--------+
                |  0 |     0 | DATA_PAGE |    PLAIN |       24 B |      3 |
                +====+=======+===========+==========+============+========+
                |    | Total |           |          |       24 B |      3 |
                +----+-------+-----------+----------+------------+--------+""");
    }

    @Test
    default void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", plainFile(), "--column", "nonexistent");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("Unknown column");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "pages", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
