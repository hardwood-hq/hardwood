/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A buffer an [InputFile] returns from `readRange` holds the range at position `0` with exactly
/// the requested bytes remaining. The reader slices it by absolute index, so a buffer that does
/// not would be read as the wrong bytes; it fails as soon as the reader receives it.
class ReadRangeContractTest {

    private static final Path FILE = Path.of("src/test/resources/plain_uncompressed.parquet");

    /// The tail core's tests read of each file: its trailer.
    private static final int TAIL = 8;

    @Test
    void aBufferNotAtPositionZeroFails() throws IOException {
        InputFile file = new ArrayInputFile(Files.readAllBytes(FILE), false);

        assertThatThrownBy(() -> ParquetFileReader.open(file).close())
                .isExactlyInstanceOf(IOException.class)
                .hasMessage("[array] readRange(" + (file.length() - TAIL) + ", " + TAIL + ") returned a buffer at"
                        + " position " + (file.length() - TAIL + 7) + " with " + TAIL + " bytes remaining, not at"
                        + " position 0 with " + TAIL + " bytes remaining");
    }

    @Test
    void aBufferOfTheWrongLengthFails() throws IOException {
        InputFile file = new ArrayInputFile(Files.readAllBytes(FILE), true);

        assertThatThrownBy(() -> ParquetFileReader.open(file).close())
                .isExactlyInstanceOf(IOException.class)
                .hasMessage("[array] readRange(" + (file.length() - TAIL) + ", " + TAIL + ") returned a buffer at"
                        + " position 0 with " + (TAIL - 1) + " bytes remaining, not at position 0 with " + TAIL
                        + " bytes remaining");
    }

    @Test
    void aConformingBufferIsRead() throws IOException {
        byte[] bytes = Files.readAllBytes(FILE);

        assertThat(ReadRanges.read(new ArrayInputFile(bytes, false) {
            @Override
            public ByteBuffer readRange(long offset, int length) {
                return ByteBuffer.wrap(bytes, Math.toIntExact(offset), length).slice();
            }
        }, 4, 8).remaining()).isEqualTo(8);
    }

    /// A file over an array, which returns each range as a view at the range's position in the
    /// array, after seven bytes of padding, or one byte short.
    private static class ArrayInputFile implements InputFile {

        private final byte[] padded;
        private final boolean shortByOne;

        ArrayInputFile(byte[] bytes, boolean shortByOne) {
            this.padded = new byte[bytes.length + 7];
            System.arraycopy(bytes, 0, padded, 7, bytes.length);
            this.shortByOne = shortByOne;
        }

        @Override
        public void open() {
        }

        @Override
        public ByteBuffer readRange(long offset, int length) {
            ReadRanges.checkBounds(name(), offset, length, length());
            if (shortByOne) {
                return ByteBuffer.wrap(padded, Math.toIntExact(offset) + 7, length - 1).slice();
            }
            return ByteBuffer.wrap(padded, Math.toIntExact(offset) + 7, length);
        }

        @Override
        public long length() {
            return padded.length - 7;
        }

        @Override
        public String name() {
            return "array";
        }

        @Override
        public void close() {
        }
    }
}
