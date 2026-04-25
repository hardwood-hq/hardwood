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

class InspectDictionaryCommandTest implements InspectDictionaryCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String dictFile() {
        return getClass().getResource("/dictionary_uncompressed.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void requiresColumnOption() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile());

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void rejectsNegativeLimit() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", dictFile(), "--column", "category",
                "--limit", "-1");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("--limit must be greater than or equal to 0");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "dictionary", "-f", "gs://bucket/data.parquet",
                "--column", "id");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
