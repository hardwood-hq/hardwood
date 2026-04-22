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

/// Shared test contract for the `inspect rowgroups` command.
interface InspectRowGroupsCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysRowGroups() {
        Cli.Result result = Cli.launch("inspect", "rowgroups", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                Row Group 0  (3 rows, 174 B uncompressed)
                +--------+-------+--------------+------------+--------------+
                | Column | Type  | Codec        | Compressed | Uncompressed |
                +--------+-------+--------------+------------+--------------+
                |     id | INT64 | UNCOMPRESSED |       87 B |         87 B |
                |  value | INT64 | UNCOMPRESSED |       87 B |         87 B |
                +--------+-------+--------------+------------+--------------+""");
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("inspect", "rowgroups", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
