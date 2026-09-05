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
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("info", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }
    @Test
    void sanitizesControlCharactersInDisplayedMetadataKeys() {
        assertThat(InfoCommand.displayKey("line1\nline2\033[31m"))
                .isEqualTo("line1·line2·[31m");
        assertThat(InfoCommand.displayKey("\u0000\u0001"))
                .startsWith("0x");
    }
}
