/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/// Decoder for DELTA_BINARY_PACKED encoding.
///
/// This encoding stores integers as deltas from consecutive values, organized in blocks
/// and miniblocks. Each block has a minimum delta, and values are stored as
/// (actual_delta - min_delta) to ensure non-negative values that can be efficiently bit-packed.
///
/// Format:
/// ```text
/// HEADER: block_size (ULEB128) | miniblock_count (ULEB128) | total_count (ULEB128) | first_value (zigzag)
/// BLOCK:  min_delta (zigzag) | bitwidths[miniblock_count] | miniblock_data...
/// ```
///
/// Supports INT32 and INT64 physical types.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md">Parquet Encodings</a>
public class DeltaBinaryPackedDecoder implements ValueDecoder {

    /// Reads eight packed bytes in one go, in the little-endian order the bit layout uses.
    /// [RleBitPackingHybridDecoder] loads the same layout the same way.
    private static final VarHandle LONG_LE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /// The stream header, which every bound in this class comes from.
    ///
    /// [#of] is where the header is checked, so a `Header` that exists is one the rest of the
    /// decoder can trust: the counts are positive, `blockSize` divides into whole miniblocks, and
    /// nothing further down has to re-establish that.
    ///
    /// @param blockSize values per block, a whole number of miniblocks
    /// @param miniblockCount miniblocks per block
    /// @param totalValueCount values the stream holds, padding aside
    /// @param firstValue the value the header carries outside any block
    /// @param valuesPerMiniblock `blockSize / miniblockCount`, carried rather than recomputed
    private record Header(int blockSize, int miniblockCount, int totalValueCount, long firstValue,
            int valuesPerMiniblock) {

        /// The largest block a header may declare.
        ///
        /// A block is decoded whole, so the block size is what the decode buffer is sized from — and
        /// nothing in the page bounds it. A block whose miniblocks are all declared at width 0
        /// occupies one byte of minimum delta plus one byte per miniblock and yields `blockSize`
        /// values, so however few bytes a page has, a header declaring 2^30 values per block asks
        /// for an 8 GiB `long[]`. Writers emit 128; this is three orders of magnitude of headroom
        /// over that and bounds the buffer at 512 KiB.
        private static final int MAX_BLOCK_SIZE = 1 << 16;

        /// Checks the four fields the stream carries and derives the fifth.
        static Header of(int blockSize, int miniblockCount, int totalValueCount, long firstValue) {
            if (blockSize <= 0) {
                throw new IllegalArgumentException("Invalid block size: " + blockSize);
            }
            if (blockSize > MAX_BLOCK_SIZE) {
                throw new IllegalArgumentException(
                        "Block size " + blockSize + " exceeds the maximum of " + MAX_BLOCK_SIZE);
            }
            // Negative counts reach here from a ULEB128 wide enough to overflow the int, and a
            // negative divisor still divides evenly (128 % -1 == 0), so the divisibility check below
            // does not catch them either. Left unchecked they surface as NegativeArraySizeException.
            if (miniblockCount <= 0) {
                throw new IllegalArgumentException("Invalid miniblock count: " + miniblockCount);
            }
            if (totalValueCount < 0) {
                throw new IllegalArgumentException("Invalid total value count: " + totalValueCount);
            }
            if (blockSize % miniblockCount != 0) {
                throw new IllegalArgumentException(
                        "Block size " + blockSize + " is not divisible by miniblock count " + miniblockCount);
            }
            return new Header(blockSize, miniblockCount, totalValueCount, firstValue, blockSize / miniblockCount);
        }
    }

    private final byte[] data;
    private int pos;

    private final Header header;

    // Reading state
    private long lastValue;
    /// Values decoded into the buffer so far, across every block. Bounds the last block, whose
    /// trailing miniblock the encoder pads out with values the caller never asked for.
    private int valuesProduced;

    // Current block state
    private long minDelta;
    private final int[] bitWidths;

    // One whole block of decoded values, plus the header's first value ahead of them
    private final long[] buffer;
    private int bufferPos;
    private int bufferFill;

    // Padded copy of a miniblock that ends too close to the end of the page to read wide from
    private byte[] tailBytes;

    /// Reads the stream header at construction. It is what the buffer's size and every bound come
    /// from, so nothing this class does is meaningful before it: deferring it only buys a flag to
    /// test on every entry point, and every caller already reports an [IOException].
    public DeltaBinaryPackedDecoder(byte[] data, int offset) throws IOException {
        this.data = data;
        this.pos = offset;
        this.header = readHeader();
        this.bitWidths = new int[header.miniblockCount()];
        // Both fields are file-controlled and neither bounds the buffer on its own, so both bound it:
        // the declared total can be Integer.MAX_VALUE on a five-byte page, and the block size is
        // capped by Header rather than by anything the page has to make good on. The cap is also
        // what keeps `blockSize + 1` from overflowing into a negative array size.
        this.buffer = new long[Math.min(header.blockSize() + 1, header.totalValueCount())];
        this.lastValue = header.firstValue();
    }

    /// Returns the current read position.
    /// Used by composite decoders (DeltaLengthByteArray, DeltaByteArray) that share the
    /// same byte[] and need to continue reading after this decoder has consumed its portion.
    public int getPos() {
        return pos;
    }

    /// Read a single INT32 value from the stream.
    public int readInt() throws IOException {
        return (int) readLongValue();
    }

    /// Read a single INT64 value from the stream.
    public long readLong() throws IOException {
        return readLongValue();
    }

    /// Read INT64 values directly into a primitive long array.
    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            int out = 0;
            while (out < output.length) {
                if (bufferPos < bufferFill) {
                    int taken = Math.min(bufferFill - bufferPos, output.length - out);
                    System.arraycopy(buffer, bufferPos, output, out, taken);
                    bufferPos += taken;
                    out += taken;
                }
                else if (output.length - out >= buffer.length) {
                    out += fillInto(output, out);
                }
                else {
                    bufferFill = fillInto(buffer, 0);
                    bufferPos = 0;
                }
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = readLongValue();
                }
            }
        }
    }

    /// Read INT32 values directly into a primitive int array.
    ///
    /// The prefix sum is carried in a `long` and narrowed here, which is the wrap-around modulo
    /// 2^32 the format calls for.
    @Override
    public void readInts(int[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            int out = 0;
            while (out < output.length) {
                if (bufferPos < bufferFill) {
                    int taken = Math.min(bufferFill - bufferPos, output.length - out);
                    for (int i = 0; i < taken; i++) {
                        output[out + i] = (int) buffer[bufferPos + i];
                    }
                    bufferPos += taken;
                    out += taken;
                }
                else if (output.length - out >= buffer.length) {
                    out += fillInto(output, out);
                }
                else {
                    bufferFill = fillInto(buffer, 0);
                    bufferPos = 0;
                }
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    output[i] = (int) readLongValue();
                }
            }
        }
    }

    /// Read a single value as a primitive long (no boxing).
    private long readLongValue() throws IOException {
        if (bufferPos >= bufferFill) {
            bufferFill = fillInto(buffer, 0);
            bufferPos = 0;
        }
        return buffer[bufferPos++];
    }

    /// Decode the next whole block, writing where the caller wants it.
    ///
    /// A block is the unit the format hands out — one minimum delta, one bit width per miniblock,
    /// then the miniblock bodies — so it is the unit to decode. Taking it whole is what keeps the
    /// readers above this method down to "drain what is buffered, else decode another block", with
    /// no boundary arithmetic and no first-value special case: the header's value is simply the
    /// first value of the first block.
    ///
    /// The destination is the caller's own array wherever a whole block fits in what is left of it,
    /// which for a page read in one go is every block but the last.
    ///
    /// @return how many values were written
    private int fillInto(long[] dest, int destOffset) throws IOException {
        checkNotExhausted();
        int produced = startBlock(dest, destOffset);
        if (valuesProduced >= header.totalValueCount()) {
            return produced;
        }
        readBlockHeader();
        for (int miniblock = 0; miniblock < header.miniblockCount() && valuesProduced < header.totalValueCount(); miniblock++) {
            int count = Math.min(header.valuesPerMiniblock(), header.totalValueCount() - valuesProduced);
            decodeMiniblock(miniblock, dest, destOffset + produced, count);
            produced += count;
            valuesProduced += count;
        }
        return produced;
    }

    /// The `int[]` form of [#fillInto(long[],int)], so an INT32 column is written once rather than
    /// staged through a `long[]` and narrowed by a second pass over it.
    private int fillInto(int[] dest, int destOffset) throws IOException {
        checkNotExhausted();
        int produced = startBlock(dest, destOffset);
        if (valuesProduced >= header.totalValueCount()) {
            return produced;
        }
        readBlockHeader();
        for (int miniblock = 0; miniblock < header.miniblockCount() && valuesProduced < header.totalValueCount(); miniblock++) {
            int count = Math.min(header.valuesPerMiniblock(), header.totalValueCount() - valuesProduced);
            decodeMiniblock(miniblock, dest, destOffset + produced, count);
            produced += count;
            valuesProduced += count;
        }
        return produced;
    }

    /// Refuses a read past the declared value count, before anything has been written.
    ///
    /// It runs ahead of [#startBlock(long[],int)] rather than after it, because the header's first
    /// value is not exempt from the count: a stream declaring no values at all — which is what
    /// writers emit for an empty one — has no first value to hand out either, and the buffer sized
    /// from that count has no room to hold one.
    private void checkNotExhausted() throws IOException {
        if (valuesProduced >= header.totalValueCount()) {
            throw new IOException("No more values to read");
        }
    }

    /// The header carries the first value outside any block. Emitting it as the first value of the
    /// first block is what spares every reader a special case for it.
    private int startBlock(long[] dest, int destOffset) {
        if (valuesProduced > 0) {
            return 0;
        }
        dest[destOffset] = header.firstValue();
        valuesProduced = 1;
        return 1;
    }

    private int startBlock(int[] dest, int destOffset) {
        if (valuesProduced > 0) {
            return 0;
        }
        dest[destOffset] = (int) header.firstValue();
        valuesProduced = 1;
        return 1;
    }

    /// Reads and checks the header the stream opens with.
    ///
    /// The checks live in [Header] itself and surface as [IllegalArgumentException]; they are
    /// translated here, because to this class a header that does not add up is a malformed file and
    /// not a programming error.
    private Header readHeader() throws IOException {
        int blockSize = readUleb128();
        int miniblockCount = readUleb128();
        int totalValueCount = readUleb128();
        long firstValue = readZigzagUleb128();
        try {
            return Header.of(blockSize, miniblockCount, totalValueCount, firstValue);
        }
        catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void readBlockHeader() throws IOException {
        minDelta = readZigzagUleb128();

        // Read bit widths for all miniblocks in this block
        for (int i = 0; i < header.miniblockCount(); i++) {
            if (pos >= data.length) {
                throw new IOException("Unexpected EOF reading bitwidths");
            }
            int bw = data[pos++] & 0xFF;
            if (bw > 64) {
                throw new IOException("Invalid bit width: " + bw);
            }
            bitWidths[i] = bw;
        }
    }

    /// Decode one miniblock onto the end of the buffer.
    ///
    /// `pos` advances by the bytes the miniblock occupies in full, which is not the same as the
    /// bytes `count` values need: the encoder pads a partly-filled miniblock and those bytes are
    /// still there to step over.
    private void decodeMiniblock(int miniblock, long[] dest, int destOffset, int count) throws IOException {
        int bitWidth = bitWidths[miniblock];
        if (bitWidth == 0) {
            long value = lastValue;
            for (int i = 0; i < count; i++) {
                value += minDelta;
                dest[destOffset + i] = value;
            }
            lastValue = value;
            return;
        }
        int bytes = miniblockBytes(bitWidth);
        unpack(dest, destOffset, bitWidth, count);
        pos += bytes;
    }

    private void decodeMiniblock(int miniblock, int[] dest, int destOffset, int count) throws IOException {
        int bitWidth = bitWidths[miniblock];
        if (bitWidth == 0) {
            long value = lastValue;
            for (int i = 0; i < count; i++) {
                value += minDelta;
                dest[destOffset + i] = (int) value;
            }
            lastValue = value;
            return;
        }
        int bytes = miniblockBytes(bitWidth);
        unpack(dest, destOffset, bitWidth, count);
        pos += bytes;
    }

    /// The bytes a whole miniblock occupies at this width, checked against what is left of the page.
    ///
    /// Taken as a long because a file-controlled block size overflows the product, and a negative
    /// byte count would pass the bounds check rather than fail it.
    private int miniblockBytes(int bitWidth) throws IOException {
        long bytes = ((long) header.valuesPerMiniblock() * bitWidth + 7) / 8;
        if (bytes > data.length - pos) {
            throw new IOException("Unexpected EOF reading miniblock data: expected " + bytes
                    + " bytes, got " + (data.length - pos));
        }
        return (int) bytes;
    }

    /// Unpack `count` residuals and run the prefix sum through them.
    ///
    /// Value `i` starts at bit `i * bitWidth` and is at most 64 bits long, so beginning at most 7
    /// bits into a byte it reaches at most 71 — two overlapping reads cover every width the format
    /// allows, and one shape of loop serves all of them. The doubled shift on the second read is
    /// what makes that branchless: at a bit offset of zero, where no second read is wanted, it
    /// shifts the byte out entirely.
    private void unpack(long[] dest, int destOffset, int bitWidth, int count) {
        byte[] src = sourceFor(bitWidth, count);
        int base = src == data ? pos : 0;
        long mask = bitWidth == Long.SIZE ? -1L : (1L << bitWidth) - 1;
        // Carried in locals across the loop and written back once. As fields they are stored on
        // every value, because nothing tells the compiler `dest` cannot alias `this`.
        long value = lastValue;
        long delta = minDelta;
        // A long: the page's own size bounds count * bitWidth only to about 2^34, so an int cursor
        // would wrap on a large enough page and read from the wrong byte.
        long bitPos = 0;
        for (int i = 0; i < count; i++) {
            int byteOffset = base + (int) (bitPos >>> 3);
            int shift = (int) (bitPos & 7);
            long low = (long) LONG_LE.get(src, byteOffset);
            long high = src[byteOffset + Long.BYTES] & 0xFFL;
            value += delta + (((low >>> shift) | ((high << (63 - shift)) << 1)) & mask);
            dest[destOffset + i] = value;
            bitPos += bitWidth;
        }
        lastValue = value;
    }

    /// The `int[]` form of [#unpack(long[],int,int,int)]. The two differ only in where they store.
    private void unpack(int[] dest, int destOffset, int bitWidth, int count) {
        byte[] src = sourceFor(bitWidth, count);
        int base = src == data ? pos : 0;
        long mask = bitWidth == Long.SIZE ? -1L : (1L << bitWidth) - 1;
        long value = lastValue;
        long delta = minDelta;
        // A long: the page's own size bounds count * bitWidth only to about 2^34, so an int cursor
        // would wrap on a large enough page and read from the wrong byte.
        long bitPos = 0;
        for (int i = 0; i < count; i++) {
            int byteOffset = base + (int) (bitPos >>> 3);
            int shift = (int) (bitPos & 7);
            long low = (long) LONG_LE.get(src, byteOffset);
            long high = src[byteOffset + Long.BYTES] & 0xFFL;
            value += delta + (((low >>> shift) | ((high << (63 - shift)) << 1)) & mask);
            dest[destOffset + i] = (int) value;
            bitPos += bitWidth;
        }
        lastValue = value;
    }

    /// The bytes to unpack from: the page itself, or a padded copy of the miniblock where the page
    /// ends inside the nine-byte window the last value's reads span.
    ///
    /// A page always ends that way on its final miniblock, so this is the ordinary case once per
    /// page rather than an error path — and paying one copy there keeps the unpack loop free of the
    /// bounds test that would otherwise run per value.
    private byte[] sourceFor(int bitWidth, int count) {
        long bytes = ((long) (count - 1) * bitWidth) / 8 + Long.BYTES + 1;
        if (pos + bytes <= data.length) {
            return data;
        }
        int needed = Math.toIntExact(bytes);
        int available = Math.min(data.length - pos, needed);
        if (tailBytes == null || tailBytes.length < needed) {
            tailBytes = new byte[needed];
        }
        System.arraycopy(data, pos, tailBytes, 0, available);
        Arrays.fill(tailBytes, available, tailBytes.length, (byte) 0);
        return tailBytes;
    }

    private int readUleb128() throws IOException {
        int result = 0;
        int shift = 0;
        int b;
        do {
            if (pos >= data.length) {
                throw new IOException("Unexpected EOF in ULEB128");
            }
            b = data[pos++] & 0xFF;
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    private long readUleb128Long() throws IOException {
        long result = 0;
        int shift = 0;
        int b;
        do {
            if (pos >= data.length) {
                throw new IOException("Unexpected EOF in ULEB128");
            }
            b = data[pos++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    private long readZigzagUleb128() throws IOException {
        long encoded = readUleb128Long();
        // Zigzag decode: (n >>> 1) ^ -(n & 1)
        return (encoded >>> 1) ^ -(encoded & 1);
    }
}
