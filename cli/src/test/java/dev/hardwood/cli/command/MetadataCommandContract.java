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

/// Shared test contract for the `metadata` command.
interface MetadataCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysMetadata(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                Format Version: 2
                Created By:     parquet-cpp-arrow version 22.0.0
                Row Groups:     1
                Total Rows:     3

                Row Group 0  (3 rows, 174 B uncompressed)
                +--------+-------+--------------+------------+--------------+
                | Column | Type  | Codec        | Compressed | Uncompressed |
                +--------+-------+--------------+------------+--------------+
                |     id | INT64 | UNCOMPRESSED |       87 B |         87 B |
                |  value | INT64 | UNCOMPRESSED |       87 B |         87 B |
                +--------+-------+--------------+------------+--------------+""");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("metadata", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
