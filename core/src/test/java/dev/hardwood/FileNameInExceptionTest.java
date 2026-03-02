/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that every exception escaping a Hardwood reader includes the originating file name.
 */
class FileNameInExceptionTest {

    @Test
    void openCorruptFileShouldIncludeFileName() {
        // A buffer with garbage data that is not a valid Parquet file
        ByteBuffer corrupt = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        InputFile inputFile = InputFile.of(corrupt);

        assertThatThrownBy(() -> ParquetFileReader.open(inputFile))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(inputFile.name());
    }

    @Test
    void openCorruptFileWithContextShouldIncludeFileName() throws Exception {
        ByteBuffer corrupt = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        InputFile inputFile = InputFile.of(corrupt);

        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> hardwood.open(inputFile))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(inputFile.name());
        }
    }

    @Test
    void multiFileOpenCorruptShouldIncludeFileName() {
        ByteBuffer corrupt = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        InputFile inputFile = InputFile.of(corrupt);

        assertThatThrownBy(() -> {
            try (Hardwood hardwood = Hardwood.create()) {
                hardwood.openAll(List.of(inputFile));
            }
        }).hasMessageContaining(inputFile.name());
    }

    @Test
    void rowReaderOnCorruptDataShouldIncludeFileName() throws Exception {
        // Create a buffer that has valid Parquet magic but corrupt content.
        // The error occurs during open() since the metadata is invalid.
        byte[] corruptData = createMinimalCorruptParquet();
        InputFile inputFile = InputFile.of(ByteBuffer.wrap(corruptData));

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                RowReader rows = reader.createRowReader();
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).hasMessageContaining(inputFile.name());
    }

    @Test
    void columnReaderOnCorruptDataShouldIncludeFileName() throws Exception {
        byte[] corruptData = createMinimalCorruptParquet();
        InputFile inputFile = InputFile.of(ByteBuffer.wrap(corruptData));

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                try (ColumnReader colReader = reader.createColumnReader(0)) {
                    while (colReader.nextBatch()) {
                        colReader.getRecordCount();
                    }
                }
            }
        }).hasMessageContaining(inputFile.name());
    }

    @Test
    void validFileOpenShouldWorkNormally() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            RowReader rows = reader.createRowReader();
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            org.assertj.core.api.Assertions.assertThat(count).isEqualTo(3);
        }
    }

    @Test
    void validFileColumnReaderShouldWorkNormally() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (ColumnReader colReader = reader.createColumnReader("id")) {
                org.assertj.core.api.Assertions.assertThat(colReader.nextBatch()).isTrue();
                org.assertj.core.api.Assertions.assertThat(colReader.getRecordCount()).isEqualTo(3);
            }
        }
    }

    /**
     * Creates a byte array that has valid Parquet header/footer magic bytes
     * but corrupt internal content, so it can be opened but will fail during reading.
     */
    private static byte[] createMinimalCorruptParquet() {
        // Parquet magic: PAR1 at start and end, with garbage in between
        byte[] magic = "PAR1".getBytes();
        // A minimal footer that points to invalid metadata
        // Footer format: [metadata_length (4 bytes LE)] [PAR1]
        int fakeMetadataLength = 4;
        byte[] data = new byte[magic.length + fakeMetadataLength + 4 + magic.length];
        // Header magic
        System.arraycopy(magic, 0, data, 0, magic.length);
        // Fake metadata (all zeros - will cause parsing errors)
        // Metadata length (little-endian)
        data[data.length - magic.length - 4] = (byte) fakeMetadataLength;
        data[data.length - magic.length - 3] = 0;
        data[data.length - magic.length - 2] = 0;
        data[data.length - magic.length - 1] = 0;
        // Footer magic
        System.arraycopy(magic, 0, data, data.length - magic.length, magic.length);
        return data;
    }
}
