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

class InspectRowGroupsCommandTest implements InspectRowGroupsCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void groupsTheRowCount() {
        Cli.Result result = Cli.launch("inspect", "rowgroups", "-f",
                getClass().getResource("/misaligned_pages.parquet").getPath());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).startsWith("Row Group 0  (10,000 rows, ");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "rowgroups", "-f", "hdfs://namenode/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
