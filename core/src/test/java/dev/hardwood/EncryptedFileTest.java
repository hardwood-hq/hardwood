/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Hardwood does not support Parquet Modular Encryption (hardwood-hq/hardwood#600).
/// Both on-disk encryption shapes must fail at [ParquetFileReader#open] with a
/// clear, file-attributed error rather than a misleading "invalid magic number"
/// or an unattributable page-scan crash.
class EncryptedFileTest {

    @Test
    void encryptedFooterFailsGracefully() {
        // Encrypted-footer mode: magic bytes are 'PARE' instead of 'PAR1'.
        Path file = Paths.get("src/test/resources/encrypted_footer.parquet");
        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(file)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("encrypted_footer.parquet")
                .hasMessageContaining("Encrypted Parquet files are not supported");
    }

    @Test
    void plaintextFooterFailsGracefully() {
        // Plaintext-footer mode: footer is readable (PAR1 magic) but carries
        // encryption_algorithm and the column data is encrypted.
        Path file = Paths.get("src/test/resources/encrypted_plaintext_footer.parquet");
        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(file)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("encrypted_plaintext_footer.parquet")
                .hasMessageContaining("Encrypted Parquet files are not supported");
    }
}
