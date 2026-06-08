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

class FooterCommandTest implements FooterCommandContract {

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("footer", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }

    @Test
    void reportsEncryptedFooterGracefully() {
        // Encrypted-footer mode: 'PARE' magic instead of 'PAR1'.
        String file = getClass().getResource("/encrypted_footer.parquet").getPath();

        Cli.Result result = Cli.launch("footer", "-f", file);

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("Encrypted Parquet files are not supported");
    }

    @Test
    @Override
    public void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("info", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("File not found");
        assertThat(result.errorOutput()).contains(nonexistentFile());
    }
}
