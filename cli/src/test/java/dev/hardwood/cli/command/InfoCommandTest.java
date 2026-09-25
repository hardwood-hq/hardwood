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

class InfoCommandTest implements InfoCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Override
    public String kvMetadataFile() {
        return getClass().getResource("/cli_info_kv_metadata_test.parquet").getPath();
    }

    /// Local-only: the file has no file-level key-value metadata at all (it carries
    /// *column*-level metadata instead, which `info` does not report), so the whole
    /// section is left out rather than printed with a count of zero. Nothing about
    /// the branch depends on how the bytes were fetched, so it doesn't earn a place
    /// in the shared contract.
    @Test
    void omitsKeyValueMetadataSectionWhenAbsent() {
        Cli.Result result = Cli.launch("info", "-f",
                getClass().getResource("/column_kv_metadata_test.parquet").getPath());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).doesNotContain("Key/Value Metadata");
    }

    @Test
    void groupsTheTotalRowCount() {
        Cli.Result result = Cli.launch("info", "-f",
                getClass().getResource("/misaligned_pages.parquet").getPath());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("Total Rows:        10,000\n");
    }

    /// A key is writer-supplied bytes just as a value is, so its control
    /// characters are replaced the same way.
    @Test
    void replacesControlCharactersInKeys() {
        Cli.Result result = Cli.launch("info", "-f",
                getClass().getResource("/cli_wide_value_test.parquet").getPath());

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("  ctl·[31m.key").doesNotContain("\u001b");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("info", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
}
