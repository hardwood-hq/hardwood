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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

            try (RowReader rowReader = reader.rowReader()) {
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
    void testReadFromBufferWithinLargerArray() throws Exception {
        byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/plain_uncompressed.parquet"));
        byte[] larger = new byte[bytes.length + 20];
        System.arraycopy(bytes, 0, larger, 7, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(larger, 7, bytes.length);

        assertReadsPlainUncompressed(InputFile.of(buffer));

        assertThat(buffer.position()).isEqualTo(7);
        assertThat(buffer.limit()).isEqualTo(7 + bytes.length);
    }

    @Test
    void testReadFromBufferWithNonZeroPosition() throws Exception {
        byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/plain_uncompressed.parquet"));
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 5);
        buffer.put(new byte[5]).put(bytes).position(5);

        assertReadsPlainUncompressed(InputFile.of(buffer));

        assertThat(buffer.position()).isEqualTo(5);
        assertThat(buffer.limit()).isEqualTo(bytes.length + 5);
    }

    @Test
    void testReadFromDirectBufferWithNonZeroPosition() throws Exception {
        byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/plain_uncompressed.parquet"));
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length + 5);
        buffer.put(new byte[5]).put(bytes).position(5);

        assertReadsPlainUncompressed(InputFile.of(buffer));

        assertThat(buffer.position()).isEqualTo(5);
    }

    @Test
    void testReadFromReadOnlyBufferWithNonZeroPosition() throws Exception {
        byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/plain_uncompressed.parquet"));
        byte[] larger = new byte[bytes.length + 5];
        System.arraycopy(bytes, 0, larger, 5, bytes.length);
        ByteBuffer buffer = ByteBuffer.wrap(larger).position(5).asReadOnlyBuffer();

        assertReadsPlainUncompressed(InputFile.of(buffer));

        assertThat(buffer.position()).isEqualTo(5);
    }

    @Test
    void testRemainingContentIsCapturedOnCreation() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}, 2, 4);
        InputFile inputFile = InputFile.of(buffer);
        buffer.position(0).limit(8);

        assertThat(inputFile.length()).isEqualTo(4);
        ByteBuffer range = inputFile.readRange(1, 3);
        assertThat(range.remaining()).isEqualTo(3);
        assertThat(range.get(0)).isEqualTo((byte) 3);
        assertThat(range.get(2)).isEqualTo((byte) 5);
        assertThatThrownBy(() -> inputFile.readRange(2, 3))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Range [2, 2 + 3) out of bounds for length 4");
    }

    private static void assertReadsPlainUncompressed(InputFile inputFile) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                RowReader rowReader = reader.rowReader()) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            for (long id = 1; id <= 3; id++) {
                assertThat(rowReader.hasNext()).isTrue();
                rowReader.next();
                assertThat(rowReader.getLong("id")).isEqualTo(id);
                assertThat(rowReader.getLong("value")).isEqualTo(id * 100);
            }
            assertThat(rowReader.hasNext()).isFalse();
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

    @Test
    void testOfBuffersVarargs() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");
        byte[] bytes = Files.readAllBytes(parquetFile);

        List<InputFile> files = InputFile.ofBuffers(
                ByteBuffer.wrap(bytes),
                ByteBuffer.wrap(bytes),
                ByteBuffer.wrap(bytes));

        assertThat(files).hasSize(3);
        for (InputFile file : files) {
            assertThat(file.name()).isEqualTo("<memory>");
            assertThat(file.length()).isEqualTo(bytes.length);
        }
    }

    @Test
    void testOfBuffersVarargsRejectsNullFirst() {
        assertThatThrownBy(() -> InputFile.ofBuffers((ByteBuffer) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("first buffer must not be null");
    }

    @Test
    void testOfBuffersVarargsRejectsNullElement() {
        assertThatThrownBy(() -> InputFile.ofBuffers(ByteBuffer.wrap(new byte[]{1}), (ByteBuffer) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("buffer must not be null");
    }

    @Test
    void testOfBuffersVarargsSingleBuffer() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");
        byte[] bytes = Files.readAllBytes(parquetFile);

        List<InputFile> files = InputFile.ofBuffers(ByteBuffer.wrap(bytes));

        assertThat(files).hasSize(1);
        assertThat(files.get(0).name()).isEqualTo("<memory>");
        assertThat(files.get(0).length()).isEqualTo(bytes.length);
    }
}
