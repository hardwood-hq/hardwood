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

/// Shared test contract for the `inspect columns` command.
interface InspectColumnsCommandContract {

    String plainFile();

    String pageIndexFile();

    String nonexistentFile();

    @Test
    default void displaysRankedColumns() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +------+--------+-------+--------------+------------+-------+-------------+----------+-----------+---------+
                | Rank | Column | Type  | Codec        | Compressed | Share | Compression | Encoding | Unencoded | # Pages |
                +------+--------+-------+--------------+------------+-------+-------------+----------+-----------+---------+
                |    1 |     id | INT64 | UNCOMPRESSED |       87 B | 50.0% |      100.0% |    PLAIN |      24 B |       — |
                |    2 |  value | INT64 | UNCOMPRESSED |       87 B | 50.0% |      100.0% |    PLAIN |      24 B |       — |
                +------+--------+-------+--------------+------------+-------+-------------+----------+-----------+---------+""");
    }

    @Test
    default void populatesPageCountWhenPageIndexAvailable() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", pageIndexFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +------+--------+-------+--------------+------------+-------+-------------+----------+-----------+---------+
                | Rank | Column | Type  | Codec        | Compressed | Share | Compression | Encoding | Unencoded | # Pages |
                +------+--------+-------+--------------+------------+-------+-------------+----------+-----------+---------+
                |    1 |     id | INT64 | UNCOMPRESSED |   78.4 KiB | 50.0% |      100.0% |    PLAIN |  78.1 KiB |      10 |
                |    2 |  value | INT64 | UNCOMPRESSED |   78.4 KiB | 50.0% |      100.0% |    PLAIN |  78.1 KiB |      10 |
                +------+--------+-------+--------------+------------+-------+-------------+----------+-----------+---------+""");
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("inspect", "columns", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
