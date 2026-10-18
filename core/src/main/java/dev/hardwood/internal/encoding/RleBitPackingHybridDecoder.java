/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.reader.ParquetReadException;

/// Decoder for RLE/Bit-Packing Hybrid encoding.
/// Used primarily for definition/repetition levels and dictionary indices.
public class RleBitPackingHybridDecoder {

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private final byte[] data;
    private final ByteBuffer dataBuffer;
    private final int dataEnd;
    private final int bitWidth;
    private final int bitMask;
    private int pos;

    // Run state
    private int currentValue;
    private int remainingInRun;
    private boolean isRleRun;

    // Bit buffer for packed values
    private long bitBuffer;
    private int bitsInBuffer;

    public RleBitPackingHybridDecoder(byte[] data, int bitWidth) {
        this(data, 0, data.length, bitWidth);
    }

    public RleBitPackingHybridDecoder(byte[] data, int offset, int length, int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Invalid RLE bit width: " + bitWidth
                    + ". Must be between 0 and 32");
        }
        this.data = data;
        this.dataBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.dataEnd = offset + length;
        this.pos = offset;
        this.bitWidth = bitWidth;
        // (1 << 32) - 1 wraps to 0 in Java (the shift count is taken mod 32), so a width of
        // 32 needs an explicit all-ones mask, matching the encoder's 32-bit handling.
        this.bitMask = (bitWidth == 0) ? 0 : (bitWidth == 32) ? -1 : (1 << bitWidth) - 1;
    }

    /// Decodes the next `count` values into `buffer` from `offset`.
    ///
    /// @throws ParquetReadException if the stream ends before `count` values
    public void readInts(int[] buffer, int offset, int count) {
        if (bitWidth == 0) {
            return;
        }

        int outPos = offset;
        int remaining = count;

        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRun();
                if (remainingInRun == 0) {
                    break;
                }
            }

            int toRead = Math.min(remaining, remainingInRun);
            int decoded;
            if (isRleRun) {
                Arrays.fill(buffer, outPos, outPos + toRead, currentValue);
                decoded = toRead;
            }
            else {
                decoded = decodeBitPacked(buffer, outPos, toRead);
            }

            outPos += decoded;
            remainingInRun -= decoded;
            remaining -= decoded;
            if (decoded < toRead) {
                break;
            }
        }

        if (remaining > 0) {
            throw new ParquetReadException("Insufficient RLE/Bit-Packing data: decoded "
                    + (count - remaining) + " of " + count + " requested values");
        }
    }

    /// Extracts up to `groups` groups of eight 9-bit values, starting at byte `pos`, into `out`
    /// from `o`, reading two little-endian words per group. Stops early when fewer than 16 bytes
    /// remain before `dataEnd`, and returns the number of groups extracted. `unpack10` to
    /// `unpack16` are the same for their own width.
    private static int unpack9(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 9, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0x1ff);
            out[o + 1] = (int) ((lo >>> 9) & 0x1ff);
            out[o + 2] = (int) ((lo >>> 18) & 0x1ff);
            out[o + 3] = (int) ((lo >>> 27) & 0x1ff);
            out[o + 4] = (int) ((lo >>> 36) & 0x1ff);
            out[o + 5] = (int) ((lo >>> 45) & 0x1ff);
            out[o + 6] = (int) ((lo >>> 54) & 0x1ff);
            out[o + 7] = (int) (((lo >>> 63) | (hi << 1)) & 0x1ff);
        }
        return g;
    }

    private static int unpack10(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 10, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0x3ff);
            out[o + 1] = (int) ((lo >>> 10) & 0x3ff);
            out[o + 2] = (int) ((lo >>> 20) & 0x3ff);
            out[o + 3] = (int) ((lo >>> 30) & 0x3ff);
            out[o + 4] = (int) ((lo >>> 40) & 0x3ff);
            out[o + 5] = (int) ((lo >>> 50) & 0x3ff);
            out[o + 6] = (int) (((lo >>> 60) | (hi << 4)) & 0x3ff);
            out[o + 7] = (int) ((hi >>> 6) & 0x3ff);
        }
        return g;
    }

    private static int unpack11(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 11, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0x7ff);
            out[o + 1] = (int) ((lo >>> 11) & 0x7ff);
            out[o + 2] = (int) ((lo >>> 22) & 0x7ff);
            out[o + 3] = (int) ((lo >>> 33) & 0x7ff);
            out[o + 4] = (int) ((lo >>> 44) & 0x7ff);
            out[o + 5] = (int) (((lo >>> 55) | (hi << 9)) & 0x7ff);
            out[o + 6] = (int) ((hi >>> 2) & 0x7ff);
            out[o + 7] = (int) ((hi >>> 13) & 0x7ff);
        }
        return g;
    }

    private static int unpack12(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 12, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0xfff);
            out[o + 1] = (int) ((lo >>> 12) & 0xfff);
            out[o + 2] = (int) ((lo >>> 24) & 0xfff);
            out[o + 3] = (int) ((lo >>> 36) & 0xfff);
            out[o + 4] = (int) ((lo >>> 48) & 0xfff);
            out[o + 5] = (int) (((lo >>> 60) | (hi << 4)) & 0xfff);
            out[o + 6] = (int) ((hi >>> 8) & 0xfff);
            out[o + 7] = (int) ((hi >>> 20) & 0xfff);
        }
        return g;
    }

    private static int unpack13(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 13, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0x1fff);
            out[o + 1] = (int) ((lo >>> 13) & 0x1fff);
            out[o + 2] = (int) ((lo >>> 26) & 0x1fff);
            out[o + 3] = (int) ((lo >>> 39) & 0x1fff);
            out[o + 4] = (int) (((lo >>> 52) | (hi << 12)) & 0x1fff);
            out[o + 5] = (int) ((hi >>> 1) & 0x1fff);
            out[o + 6] = (int) ((hi >>> 14) & 0x1fff);
            out[o + 7] = (int) ((hi >>> 27) & 0x1fff);
        }
        return g;
    }

    private static int unpack14(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 14, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0x3fff);
            out[o + 1] = (int) ((lo >>> 14) & 0x3fff);
            out[o + 2] = (int) ((lo >>> 28) & 0x3fff);
            out[o + 3] = (int) ((lo >>> 42) & 0x3fff);
            out[o + 4] = (int) (((lo >>> 56) | (hi << 8)) & 0x3fff);
            out[o + 5] = (int) ((hi >>> 6) & 0x3fff);
            out[o + 6] = (int) ((hi >>> 20) & 0x3fff);
            out[o + 7] = (int) ((hi >>> 34) & 0x3fff);
        }
        return g;
    }

    private static int unpack15(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 15, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0x7fff);
            out[o + 1] = (int) ((lo >>> 15) & 0x7fff);
            out[o + 2] = (int) ((lo >>> 30) & 0x7fff);
            out[o + 3] = (int) ((lo >>> 45) & 0x7fff);
            out[o + 4] = (int) (((lo >>> 60) | (hi << 4)) & 0x7fff);
            out[o + 5] = (int) ((hi >>> 11) & 0x7fff);
            out[o + 6] = (int) ((hi >>> 26) & 0x7fff);
            out[o + 7] = (int) ((hi >>> 41) & 0x7fff);
        }
        return g;
    }

    private static int unpack16(byte[] data, int pos, int dataEnd, int[] out, int o, int groups) {
        int g = 0;
        for (; g < groups && pos + 16 <= dataEnd; g++, pos += 16, o += 8) {
            long lo = (long) LONG_LE.get(data, pos);
            long hi = (long) LONG_LE.get(data, pos + 8);
            out[o + 0] = (int) (lo & 0xffff);
            out[o + 1] = (int) ((lo >>> 16) & 0xffff);
            out[o + 2] = (int) ((lo >>> 32) & 0xffff);
            out[o + 3] = (int) ((lo >>> 48) & 0xffff);
            out[o + 4] = (int) (hi & 0xffff);
            out[o + 5] = (int) ((hi >>> 16) & 0xffff);
            out[o + 6] = (int) ((hi >>> 32) & 0xffff);
            out[o + 7] = (int) ((hi >>> 48) & 0xffff);
        }
        return g;
    }

    private void readNextRun() {
        if (pos >= dataEnd) {
            remainingInRun = 0;
            return;
        }

        long header = readUnsignedVarInt();

        if ((header & 1) == 1) {
            // Bit-packed: header >> 1 = number of 8-value groups
            remainingInRun = (int) (header >> 1) * 8;
            isRleRun = false;
        }
        else {
            // RLE: header >> 1 = repeat count
            remainingInRun = (int) (header >> 1);
            currentValue = readRleValue();
            isRleRun = true;
        }
    }

    private int readRleValue() {
        int bytesNeeded = (bitWidth + 7) / 8;
        if (bytesNeeded > dataEnd - pos) {
            throw new ParquetReadException("Unexpected EOF reading RLE run value: expected " + bytesNeeded
                    + " bytes, got " + (dataEnd - pos));
        }
        int value = 0;
        for (int i = 0; i < bytesNeeded; i++) {
            value |= (data[pos++] & 0xFF) << (i * 8);
        }
        return value & bitMask;
    }

    /// Batch decode bit-packed values. Optimized paths for common bit widths.
    ///
    /// @return the number of values decoded, fewer than `count` only where the stream ends first
    private int decodeBitPacked(int[] output, int outPos, int count) {
        final int requested = count;
        final int width = bitWidth;
        final int mask = bitMask;

        // The bulk paths below read whole groups of eight straight from `pos`, so they run only
        // while the bit buffer is empty. A call that ends inside a group leaves the group's
        // remaining bits buffered, and the next call must take those first: reading from `pos`
        // instead would skip them and shift every value after. Finishing that group through the
        // buffer empties it again, at the latest at the group's end, where the bits taken reach
        // a byte boundary.
        int buffered = decodeBuffered(output, outPos, count, true);
        outPos += buffered;
        count -= buffered;

        // Fast path for bit width 1 (common for definition levels)
        if (width == 1) {
            while (count >= 8 && bitsInBuffer == 0 && pos < dataEnd) {
                int b = data[pos++] & 0xFF;
                output[outPos]     = b & 1;
                output[outPos + 1] = (b >> 1) & 1;
                output[outPos + 2] = (b >> 2) & 1;
                output[outPos + 3] = (b >> 3) & 1;
                output[outPos + 4] = (b >> 4) & 1;
                output[outPos + 5] = (b >> 5) & 1;
                output[outPos + 6] = (b >> 6) & 1;
                output[outPos + 7] = (b >> 7) & 1;
                outPos += 8;
                count -= 8;
            }
        }
        // For widths 2-8: read 8 bytes at once when possible, extract 8 values
        else if (width <= 8) {
            // Process 8 values at a time using bulk long reads when we have enough data
            while (count >= 8 && bitsInBuffer == 0 && pos + 8 <= dataEnd) {
                long bits = dataBuffer.getLong(pos);
                pos += width; // Only consume 'width' bytes for 8 values

                output[outPos]     = (int) (bits & mask); bits >>>= width;
                output[outPos + 1] = (int) (bits & mask); bits >>>= width;
                output[outPos + 2] = (int) (bits & mask); bits >>>= width;
                output[outPos + 3] = (int) (bits & mask); bits >>>= width;
                output[outPos + 4] = (int) (bits & mask); bits >>>= width;
                output[outPos + 5] = (int) (bits & mask); bits >>>= width;
                output[outPos + 6] = (int) (bits & mask); bits >>>= width;
                output[outPos + 7] = (int) (bits & mask);
                outPos += 8;
                count -= 8;
            }
            // Fallback when near end of buffer
            while (count >= 8 && bitsInBuffer == 0 && pos + width <= dataEnd) {
                long bits = 0;
                for (int i = 0; i < width; i++) {
                    bits |= ((long) (data[pos++] & 0xFF)) << (i * 8);
                }
                output[outPos]     = (int) (bits & mask); bits >>>= width;
                output[outPos + 1] = (int) (bits & mask); bits >>>= width;
                output[outPos + 2] = (int) (bits & mask); bits >>>= width;
                output[outPos + 3] = (int) (bits & mask); bits >>>= width;
                output[outPos + 4] = (int) (bits & mask); bits >>>= width;
                output[outPos + 5] = (int) (bits & mask); bits >>>= width;
                output[outPos + 6] = (int) (bits & mask); bits >>>= width;
                output[outPos + 7] = (int) (bits & mask);
                outPos += 8;
                count -= 8;
            }
        }

        // 9 to 16: a group of eight spans `width` bytes, at most 16, so two little-endian words
        // read from the group start hold all eight values. Each width has its own extractor whose
        // shifts and mask are constants; with the width a parameter the shifts become variable
        // and the extraction is no faster than the one-read-per-value path below.
        //
        // Only while the buffer is empty, because this bypasses it, and only while 16 bytes
        // remain: the second word runs up to 7 bytes past the group, though `pos` advances by just
        // the group's bytes, so over-reading would leave the cursor inside the next run's header.
        else if (width <= 16 && bitsInBuffer == 0) {
            int groups = count >>> 3;
            int done = switch (width) {
                case 9 -> unpack9(data, pos, dataEnd, output, outPos, groups);
                case 10 -> unpack10(data, pos, dataEnd, output, outPos, groups);
                case 11 -> unpack11(data, pos, dataEnd, output, outPos, groups);
                case 12 -> unpack12(data, pos, dataEnd, output, outPos, groups);
                case 13 -> unpack13(data, pos, dataEnd, output, outPos, groups);
                case 14 -> unpack14(data, pos, dataEnd, output, outPos, groups);
                case 15 -> unpack15(data, pos, dataEnd, output, outPos, groups);
                default -> unpack16(data, pos, dataEnd, output, outPos, groups);
            };
            pos += done * width;
            outPos += done * 8;
            count -= done * 8;
        }

        // Above 16 a group of eight no longer fits two 64-bit words, so it is read a value at a
        // time instead: the offset inside a byte is at most 7 and the constructor caps the width
        // at 32, so a value reaches 39 bits and one eight-byte read still covers it whole.
        // Nothing is carried between values, which is what the bit buffer below cannot do.
        //
        // Only while the buffer is empty, because this bypasses it, and only while the reads stay
        // inside the data: they run past the bytes the group owns, though `pos` advances by just
        // those, so over-reading would leave the cursor inside the next run's header.
        else if (width <= 32) {
            int groupSpan = ((7 * width) >>> 3) + Long.BYTES;
            while (count >= 8 && bitsInBuffer == 0 && pos + groupSpan <= dataEnd) {
                for (int i = 0; i < 8; i++) {
                    int bitOffset = i * width;
                    long word = dataBuffer.getLong(pos + (bitOffset >>> 3));
                    output[outPos + i] = (int) ((word >>> (bitOffset & 7)) & mask);
                }
                outPos += 8;
                count -= 8;
                pos += width;
            }
        }

        // Handle remaining values
        count -= decodeBuffered(output, outPos, count, false);
        return requested - count;
    }

    /// Decodes up to `count` values a value at a time through the bit buffer, stopping early
    /// where the stream ends or, if `untilEmpty`, where the buffer runs empty.
    ///
    /// @return the number of values decoded
    private int decodeBuffered(int[] output, int outPos, int count, boolean untilEmpty) {
        final int width = bitWidth;
        final int mask = bitMask;
        int decoded = 0;
        while (decoded < count && !(untilEmpty && bitsInBuffer == 0)) {
            while (bitsInBuffer < width && pos < dataEnd) {
                bitBuffer |= ((long) (data[pos++] & 0xFF)) << bitsInBuffer;
                bitsInBuffer += 8;
            }
            if (bitsInBuffer < width) {
                break;
            }
            output[outPos + decoded++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
        }
        return decoded;
    }

    /// Returns `true` if the first `count` values of this stream are all equal
    /// to `value`, as proven by a single leading RLE run that covers them.
    ///
    /// Used to detect all-present definition levels (one RLE run of `maxDef`)
    /// without materializing the level array. This is a read-only probe: it
    /// inspects the first run and then restores the decoder to its original
    /// state, so the same instance can afterwards be read from the beginning —
    /// via [#readInts] or otherwise — exactly as if the probe had never run.
    public boolean isSingleRleRunOf(int value, int count) {
        if (count == 0) {
            return true;
        }
        if (bitWidth == 0 || pos >= dataEnd) {
            return false;
        }
        // Snapshot the run state readNextRun is about to mutate, then restore it
        // before returning so the probe leaves no trace. bitBuffer/bitsInBuffer
        // are untouched by readNextRun, so they need no saving.
        int savedPos = pos;
        int savedRemainingInRun = remainingInRun;
        int savedCurrentValue = currentValue;
        boolean savedIsRleRun = isRleRun;

        readNextRun();
        boolean match = isRleRun && currentValue == value && remainingInRun >= count;

        pos = savedPos;
        remainingInRun = savedRemainingInRun;
        currentValue = savedCurrentValue;
        isRleRun = savedIsRleRun;
        return match;
    }

    private long readUnsignedVarInt() {
        long result = 0;
        int shift = 0;
        while (pos < dataEnd) {
            int b = data[pos++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }
}
