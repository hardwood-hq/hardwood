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
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class PrintCommandTest {

    private final String TEST_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();
    private final String BYTE_ARRAY_FILE = this.getClass().getResource("/delta_byte_array_test.parquet").getPath();

    @Test
    void printsAsciiTableDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", TEST_FILE);

        assertThat(result.exitCode()).isZero();
        String output = result.getOutput().replace(System.lineSeparator(), "\n");

        // For default small file, we know exact ASCII output
        assertThat(output).isEqualTo("""
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
    void noLineSeparatorBetweenRows(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", TEST_FILE, "--no-row-delimiter");

        assertThat(result.exitCode()).isZero();
        String output = result.getOutput().replace(System.lineSeparator(), "\n");

        // For default small file, we know exact ASCII output
        assertThat(output).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 1  | 100   |
                | 2  | 200   |
                | 3  | 300   |
                +----+-------+""");
    }

    @Test
    void tail(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", TEST_FILE, "-n", "-2");

        assertThat(result.exitCode()).isZero();
        String output = result.getOutput().replace(System.lineSeparator(), "\n");

        // For default small file, we know exact ASCII output
        assertThat(output).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 2  | 200   |
                +----+-------+
                | 3  | 300   |
                +----+-------+""");
    }

    @Test
    void head(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", TEST_FILE, "-n", "2");

        assertThat(result.exitCode()).isZero();
        String output = result.getOutput().replace(System.lineSeparator(), "\n");

        // For default small file, we know exact ASCII output
        assertThat(output).isEqualTo("""
                +----+-------+
                | id | value |
                +----+-------+
                | 1  | 100   |
                +----+-------+
                | 2  | 200   |
                +----+-------+""");
    }

    @Test
    void byteArrayAsString(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", BYTE_ARRAY_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput().replace(System.lineSeparator(), "\n"))
                .isEqualTo("""
                        +----+----------------+-----------------+
                        | id | prefix_strings | varying_strings |
                        +----+----------------+-----------------+
                        | 1  | apple          | hello           |
                        +----+----------------+-----------------+
                        | 2  | application    | world           |
                        +----+----------------+-----------------+
                        | 3  | apply          | wonderful       |
                        +----+----------------+-----------------+
                        | 4  | banana         | wonder          |
                        +----+----------------+-----------------+
                        | 5  | bandana        | wander          |
                        +----+----------------+-----------------+
                        | 6  | band           | wandering       |
                        +----+----------------+-----------------+
                        | 7  | bandwidth      | test            |
                        +----+----------------+-----------------+
                        | 8  | ban            | testing         |
                        +----+----------------+-----------------+""");
    }

    @Test
    void showsRowIndexWhenEnabled(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", TEST_FILE, "-ri");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput().replace(System.lineSeparator(), "\n"))
                .isEqualTo("""
                        +----------+----+-------+
                        | rowIndex | id | value |
                        +----------+----+-------+
                        | 0        | 1  | 100   |
                        +----------+----+-------+
                        | 1        | 2  | 200   |
                        +----------+----+-------+
                        | 2        | 3  | 300   |
                        +----------+----+-------+""");
    }

    @Test
    void truncatesWhenEnabled(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", BYTE_ARRAY_FILE, "--no-truncate", "-mw", "5");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput().replace(System.lineSeparator(), "\n"))
                .isEqualTo("""
                        +----+-------+-------+
                        | id | prefi | varyi |
                        |    | x_str | ng_st |
                        |    | ings  | rings |
                        +----+-------+-------+
                        | 1  | apple | hello |
                        +----+-------+-------+
                        | 2  | appli | world |
                        |    | catio |       |
                        |    | n     |       |
                        +----+-------+-------+
                        | 3  | apply | wonde |
                        |    |       | rful  |
                        +----+-------+-------+
                        | 4  | banan | wonde |
                        |    | a     | r     |
                        +----+-------+-------+
                        | 5  | banda | wande |
                        |    | na    | r     |
                        +----+-------+-------+
                        | 6  | band  | wande |
                        |    |       | ring  |
                        +----+-------+-------+
                        | 7  | bandw | test  |
                        |    | idth  |       |
                        +----+-------+-------+
                        | 8  | ban   | testi |
                        |    |       | ng    |
                        +----+-------+-------+""");
    }

    @Test
    void failsOnMissingFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", "nonexistent.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getOutput()).isEqualTo("");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput().replace(System.lineSeparator(), "\n"))
                .isEqualTo("Remote URIs are not implemented yet.");
    }
}
