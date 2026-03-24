/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for [InputFile#of(ByteBuffer)] and [InputFile#ofBuffers(List)].
class ByteBufferInputFileTest {

    @Test
    void testReadFromByteBuffer() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");
        byte[] bytes = Files.readAllBytes(parquetFile);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(buffer))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            assertThat(reader.getFileSchema().getColumnCount()).isEqualTo(2);

            try (RowReader rowReader = reader.createRowReader()) {
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getLong("id")).isEqualTo(1L);
                assertThat(rowReader.getLong("value")).isEqualTo(100L);

                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getLong("id")).isEqualTo(2L);
                assertThat(rowReader.getLong("value")).isEqualTo(200L);

                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getLong("id")).isEqualTo(3L);
                assertThat(rowReader.getLong("value")).isEqualTo(300L);

                assertThat(rowReader.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testInputFileOfByteBufferProperties() throws Exception {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        InputFile inputFile = InputFile.of(ByteBuffer.wrap(data));

        assertThat(inputFile.name()).isEqualTo("<memory>");
        inputFile.open(); // no-op, should not throw
        assertThat(inputFile.length()).isEqualTo(5);
        assertThat(inputFile.readRange(1, 3).remaining()).isEqualTo(3);
        inputFile.close(); // no-op, should not throw
    }

    @Test
    void testOfBuffers() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");
        byte[] bytes = Files.readAllBytes(parquetFile);

        List<InputFile> files = InputFile.ofBuffers(List.of(
                ByteBuffer.wrap(bytes),
                ByteBuffer.wrap(bytes)));

        assertThat(files).hasSize(2);
        for (InputFile file : files) {
            assertThat(file.name()).isEqualTo("<memory>");
            assertThat(file.length()).isEqualTo(bytes.length);
        }
    }
}
