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

class InspectPagesCommandTest implements InspectPagesCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String dictFile() {
        return getClass().getResource("/dictionary_uncompressed.parquet").getPath();
    }

    @Override
    public String pageIndexFile() {
        return getClass().getResource("/column_index_pushdown.parquet").getPath();
    }

    @Override
    public String longValueFile() {
        return getClass().getResource("/cli_long_value_test.parquet").getPath();
    }

    @Override
    public String nestedFile() {
        return getClass().getResource("/list_basic_test.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    /// Values, nulls and first-row indexes are counts, so they group in threes
    /// like every other count the CLI prints.
    @Test
    void groupsCountsWithThousandsSeparators() {
        Cli.Result result = Cli.launch("inspect", "pages", "-f",
                getClass().getResource("/misaligned_pages.parquet").getPath(), "-c", "narrow");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains(" 10,000 ").doesNotContain(" 10000 ");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("inspect", "pages", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
