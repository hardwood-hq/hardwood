/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests that [InputFile#of(Path)] reads files larger than 2 GB by mapping each
/// requested region on demand, rather than mapping the whole file at once.
///
/// The temp file exceeds [Integer#MAX_VALUE] but is sparse: only the pages holding
/// the sentinel bytes are materialized, so on a sparse-capable filesystem (Linux
/// ext4/xfs/tmpfs) it costs kilobytes and a few milliseconds.
class MappedInputFileLargeFileTest {

    private static final long TWO_GB = (long) Integer.MAX_VALUE + 1; // 2_147_483_648

    @Test
    void readsRegionsBeyondTwoGigabytes(@TempDir Path tmpDir) throws Exception {
        Path file = tmpDir.resolve("large.bin");

        byte[] atZero = "START-OF-FILE---".getBytes(StandardCharsets.US_ASCII);   // 16 bytes
        byte[] straddle = "STRADDLE-2GB-MRK".getBytes(StandardCharsets.US_ASCII); // 16 bytes
        byte[] beyond = "WELL-BEYOND-2GB!".getBytes(StandardCharsets.US_ASCII);   // 16 bytes

        long straddleOffset = TWO_GB - 8;     // the [straddleOffset, +16) region crosses the 2 GB mark
        long beyondOffset = TWO_GB + 4096;    // start offset itself exceeds Integer.MAX_VALUE
        long fileSize = beyondOffset + beyond.length;

        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(fileSize);          // sparse: holes consume no blocks
            raf.seek(0);
            raf.write(atZero);
            raf.seek(straddleOffset);
            raf.write(straddle);
            raf.seek(beyondOffset);
            raf.write(beyond);
        }

        try (InputFile inputFile = InputFile.of(file)) {
            inputFile.open();

            // The whole-file 2 GB guard is gone: a >2 GB file opens and reports its full size.
            assertThat(inputFile.length()).isEqualTo(fileSize);

            assertThat(read(inputFile, 0, atZero.length)).isEqualTo(atZero);
            assertThat(read(inputFile, straddleOffset, straddle.length)).isEqualTo(straddle);

            // The decisive case: a start offset past Integer.MAX_VALUE, which an int cast would corrupt.
            assertThat(read(inputFile, beyondOffset, beyond.length)).isEqualTo(beyond);

            // A hole reads back as zeros.
            assertThat(read(inputFile, TWO_GB + 1024, 8)).containsOnly((byte) 0);

            // A range past EOF fails cleanly with file context, rather than SIGBUS-ing
            // when the per-region mapping is touched.
            assertThatThrownBy(() -> inputFile.readRange(fileSize, 16))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessageContaining("large.bin");
            assertThatThrownBy(() -> inputFile.readRange(fileSize - 4, 16))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    private static byte[] read(InputFile inputFile, long offset, int length) throws IOException {
        ByteBuffer buffer = inputFile.readRange(offset, length);
        byte[] out = new byte[length];
        buffer.get(out);
        return out;
    }
}
