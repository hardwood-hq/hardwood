/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A run header is a ULEB128 varint read as a `long`; a count it declares past the `int` range is
/// refused rather than narrowed, which would wrap it to a negative or a smaller count.
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class RleBitPackingHybridDecoderRunHeaderTest {

    @Test
    void repeatCountPastIntRangeIsRefused() {
        // 2^32 + 3 narrows to 3, which would read as three repeats of the value
        byte[] data = run((1L << 32) + 3, false, 1);

        assertThatThrownBy(() -> new RleBitPackingHybridDecoder(data, 1).readInts(new int[3], 0, 3))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid RLE/Bit-Packing run: repeat count 4294967299 exceeds 2147483647");
    }

    @Test
    void repeatCountOfIntMaxIsRead() {
        int[] values = new int[4];
        new RleBitPackingHybridDecoder(run(Integer.MAX_VALUE, false, 1), 1).readInts(values, 0, 4);

        assertThat(values).containsExactly(1, 1, 1, 1);
    }

    @Test
    void bitPackedGroupCountPastIntRangeIsRefused() {
        // 2^28 groups are 2^31 values, which narrow to Integer.MIN_VALUE
        byte[] data = run(1L << 28, true, 0);

        assertThatThrownBy(() -> new RleBitPackingHybridDecoder(data, 1).readInts(new int[8], 0, 8))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid RLE/Bit-Packing run: 268435456 bit-packed groups of 8 values exceed "
                        + "2147483647 values");
    }

    @Test
    void headerVarintPastSixtyFourBitsIsRefused() {
        // Eleven bytes: the bits of the last fall past a long
        byte[] data = { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x02, 0x01 };

        assertThatThrownBy(() -> new RleBitPackingHybridDecoder(data, 1).readInts(new int[1], 0, 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid RLE/Bit-Packing run: header varint longer than 64 bits");
    }

    @Test
    void headerVarintWhoseTenthByteFallsPastALongIsRefused() {
        // A tenth byte lands at bit 63, which holds its low bit only: 0x02 there is lost, leaving
        // a header of 6, an RLE run of 3
        byte[] data = { (byte) 0x86, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x02, 0x01 };

        assertThatThrownBy(() -> new RleBitPackingHybridDecoder(data, 1).readInts(new int[3], 0, 3))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid RLE/Bit-Packing run: header varint longer than 64 bits");
    }

    /// One run at bit width 1: its header, `count` shifted past the kind bit, then for an RLE run
    /// `value` in one byte, or for a bit-packed one a byte of `value` per group.
    private static byte[] run(long count, boolean bitPacked, int value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long header = (count << 1) | (bitPacked ? 1 : 0);
        while ((header & ~0x7FL) != 0) {
            out.write((int) ((header & 0x7F) | 0x80));
            header >>>= 7;
        }
        out.write((int) header);
        out.write(value);
        return out.toByteArray();
    }
}
