/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What a read failure tells the user, for the failures that had no test at all.
///
/// These messages are the whole of what someone gets when a file will not read,
/// so they are asserted entire rather than by fragment: a `contains` check
/// passes just as happily on a message that has quietly lost the offset it was
/// added to carry.
class ReadFailureMessageTest {

    private static final byte[] MAGIC = { 'P', 'A', 'R', '1' };

    @Test
    void aFileTooShortToHoldAFooterSaysHowShort() {
        ByteBuffer tiny = ByteBuffer.allocate(8);

        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(tiny)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] File too small to be a valid Parquet file (8 bytes)");
    }

    @Test
    void aTrailingMagicThatIsNotPar1NamesTheByteItWasReadFrom() {
        ByteBuffer file = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        file.put(MAGIC);
        file.put(new byte[] { 0, 0, 0, 0 });
        file.putInt(4);
        file.put(new byte[] { 'N', 'O', 'P', 'E' });
        file.flip();

        // 16 bytes long, so the trailing marker begins at 12.
        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(file)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] magic bytes at byte 12 (0x00000c) — Not a Parquet file"
                        + " (invalid magic number at end)");
    }

    @Test
    void aFooterLengthReachingBackPastTheHeaderNamesWhereItWasRead() {
        ByteBuffer file = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        file.put(MAGIC);
        file.put(new byte[] { 0, 0, 0, 0 });
        file.putInt(9999);
        file.put(MAGIC);
        file.flip();

        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(file)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>] footer length at byte 8 (0x000008) — 9999 would start the"
                        + " footer in front of the file's opening magic");
    }

}
