/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Reading a file's footer.
class ParquetMetadataReaderTest {

    private static final Path FILE = Path.of("src/test/resources/plain_uncompressed.parquet");

    @Test
    void theFooterIsReadFromTheEndOfTheFileAlone() throws Exception {
        // The leading magic sits at the far end of the file from the footer, so reading it
        // would cost a remote file a request of its own.
        CountingInputFile file = new CountingInputFile(InputFile.of(FILE));
        file.open();

        ParquetMetadataReader.readMetadata(file);

        assertThat(file.readCount()).isEqualTo(2);
        assertThat(file.footerReadCount()).isEqualTo(2);
    }

    @Test
    void aDamagedLeadingMagicLeavesTheFileReadable() throws Exception {
        // The four bytes carry nothing a read uses: every page is located through the footer.
        byte[] bytes = Files.readAllBytes(FILE);
        bytes[0] = 'X';
        bytes[1] = 'X';
        bytes[2] = 'X';
        bytes[3] = 'X';

        assertThat(countRows(InputFile.of(ByteBuffer.wrap(bytes)))).isEqualTo(countRows(InputFile.of(FILE)));
    }

    private static long countRows(InputFile inputFile) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                RowReader rowReader = reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }
}
