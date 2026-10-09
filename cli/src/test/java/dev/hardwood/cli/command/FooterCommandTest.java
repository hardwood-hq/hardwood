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
        return Cli.resourcePath("/plain_uncompressed.parquet");
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
        String file = Cli.resourcePath("/encrypted_footer.parquet");

        Cli.Result result = Cli.launch("footer", "-f", file);

        assertThat(result.exitCode()).isNotZero();
        // The command reports the canonical message without the `[file] ` prefix the
        // reader attaches: it was given the file, so naming it back is noise.
        assertThat(result.errorOutput().strip()).isEqualTo(
                "Encrypted Parquet files are not supported (Parquet Modular Encryption).");
    }
}
