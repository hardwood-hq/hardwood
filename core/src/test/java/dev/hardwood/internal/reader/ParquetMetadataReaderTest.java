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
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void aNegativeFooterLengthIsRejected() throws Exception {
        // FF FF FF FF is -1 as a signed int: subtracted from the file size, it moves the
        // footer start past the end of the file rather than before its beginning.
        byte[] bytes = Files.readAllBytes(FILE);
        int lengthPos = bytes.length - 8;
        for (int i = 0; i < 4; i++) {
            bytes[lengthPos + i] = (byte) 0xFF;
        }

        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] Invalid footer length: -1");
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
