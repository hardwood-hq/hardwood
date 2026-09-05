/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The header decides how the decoder sizes its buffers and how far it reads, and it comes out of
/// the file, so every field of it is hostile input. These are the shapes a corrupt or malicious
/// page can take that a round trip against a well-behaved encoder never produces.
class DeltaBinaryPackedDecoderHeaderTest {

    @Test
    void refusesAZeroBlockSize() {
        assertThatThrownBy(() -> decode(header(0, 4, 5, 100), 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid block size: 0");
    }

    @Test
    void refusesANegativeTotalValueCount() {
        assertThatThrownBy(() -> decode(header(128, 4, -1, 100), 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid total value count: -1");
    }

    @Test
    void refusesAZeroMiniblockCount() {
        assertThatThrownBy(() -> decode(header(128, 0, 5, 100), 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid miniblock count: 0");
    }

    @Test
    void refusesANegativeMiniblockCount() {
        // A ULEB128 wide enough to overflow the int reaches the header as a negative count, and a
        // negative divisor divides evenly (128 % -1 == 0), so the divisibility check does not stop
        // it either. Unrefused it reaches new int[-1].
        assertThatThrownBy(() -> decode(header(128, -1, 5, 100), 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid miniblock count: -1");
    }

    @Test
    void refusesABlockSizeThatTheMiniblockCountDoesNotDivide() {
        assertThatThrownBy(() -> decode(header(128, 3, 5, 100), 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not divisible");
    }

    @Test
    void refusesABitWidthWiderThanALong() {
        byte[] page = concat(concat(header(128, 1, 5, 100), zigzag(3)), new byte[] { (byte) 200 });

        assertThatThrownBy(() -> decode(page, 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid bit width: 200");
    }

    @Test
    void sizesItsBufferByTheValueCountAndNotByTheBlockSize() throws IOException {
        // The block size says nothing about how many values the page holds, which is five. Sizing
        // the decode buffer from the block size instead follows a number the page never has to make
        // good on — here a 512 KiB long[] for a six-byte page.
        byte[] page = concat(concat(header(1 << 16, 1, 5, 100), zigzag(3)), new byte[] { 0 });

        assertThat(decode(page, 5)).containsExactly(100, 103, 106, 109, 112);
    }

    @Test
    void refusesABlockSizeBeyondWhatItWillBufferWhole() {
        // A block is decoded whole, so the block size is a memory bound, and no page bounds it: a
        // block of zero-width miniblocks yields blockSize values from two bytes. Left uncapped, this
        // six-byte page asks for an 8 GiB long[] and takes the reader out with an OutOfMemoryError.
        byte[] page = concat(concat(header(1 << 30, 1, 5, 100), zigzag(3)), new byte[] { 0 });

        assertThatThrownBy(() -> decode(page, 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Block size 1073741824 exceeds the maximum");
    }

    @Test
    void refusesABlockSizeBeyondTheCapEvenWhenTheValueCountIsLargeToo() {
        // The declared total is the other bound on the buffer, and it is file-controlled as well, so
        // a header that inflates both gets past a check that only bounds one by the other.
        byte[] page = concat(concat(header(1 << 30, 1, Integer.MAX_VALUE, 100), zigzag(3)),
                new byte[] { 0 });

        assertThatThrownBy(() -> decode(page, 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exceeds the maximum");
    }

    @Test
    void refusesToHandOutTheHeadersValueWhenTheStreamDeclaresNone() {
        // What a writer emits for a stream nothing was written to. The header still carries a first
        // value, but a count of zero says it is not one of the stream's, and the buffer sized from
        // that count has no room to hold it either.
        byte[] page = concat(header(128, 4, 0, 100), zigzag(0));

        assertThatThrownBy(() -> decode(page, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No more values to read");
        assertThatThrownBy(() -> new DeltaBinaryPackedDecoder(page, 0).readInt())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No more values to read");
    }

    @Test
    void readsAStreamThatIsNothingButItsHeaderValue() throws IOException {
        // The other end of the same boundary: a count of one is satisfied by the header alone, and
        // no block follows it to read.
        byte[] page = header(128, 4, 1, 100);

        assertThat(decode(page, 1)).containsExactly(100);
    }

    private static int[] decode(byte[] page, int count) throws IOException {
        int[] output = new int[count];
        new DeltaBinaryPackedDecoder(page, 0).readInts(output, null, 0);
        return output;
    }

    private static byte[] header(int blockSize, int miniblockCount, int totalValueCount, long firstValue) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUleb128(out, blockSize);
        writeUleb128(out, miniblockCount);
        writeUleb128(out, totalValueCount);
        out.writeBytes(zigzag(firstValue));
        return out.toByteArray();
    }

    private static byte[] zigzag(long value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long encoded = (value << 1) ^ (value >> 63);
        do {
            int b = (int) (encoded & 0x7F);
            encoded >>>= 7;
            out.write(encoded != 0 ? b | 0x80 : b);
        } while (encoded != 0);
        return out.toByteArray();
    }

    private static void writeUleb128(ByteArrayOutputStream out, int value) {
        long remaining = value & 0xFFFFFFFFL;
        do {
            int b = (int) (remaining & 0x7F);
            remaining >>>= 7;
            out.write(remaining != 0 ? b | 0x80 : b);
        } while (remaining != 0);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(first);
        out.writeBytes(second);
        return out.toByteArray();
    }
}
