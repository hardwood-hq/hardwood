/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/// Pull-based cursor over an RLE/Bit-Packing Hybrid stream.
///
/// Instead of materializing the entire stream into an `int[]` via
/// [RleBitPackingHybridDecoder#readInts], this cursor exposes the run
/// structure so consumers can handle constant (RLE) runs in O(1) and
/// only unpack bit-packed runs into bounded chunks.
///
/// Usage:
/// ```
/// var cursor = new HybridStreamCursor(data, offset, length, bitWidth);
/// while (cursor.advance()) {
///     if (cursor.isRle()) {
///         // constant run: cursor.value() repeated cursor.remaining() times
///     } else {
///         // bit-packed run: unpack in chunks
///         while (cursor.remaining() > 0) {
///             int n = cursor.unpack(buf, 0, buf.length);
///             // process buf[0..n)
///         }
///     }
/// }
/// ```
///
/// The cursor is single-use and forward-only. After construction the cursor
/// is positioned *before* the first run; [#advance()] must be called to
/// load it.
public final class HybridStreamCursor {

    private final byte[] data;
    private final ByteBuffer dataBuffer;
    private final int dataEnd;
    private final int bitWidth;
    private final int bitMask;
    private int pos;

    // Current run state
    private int currentValue;
    private int remaining;
    private boolean rle;

    // Bit buffer for packed values (carried across unpack calls within a run)
    private long bitBuffer;
    private int bitsInBuffer;

    /// Creates a cursor over the given RLE/Bit-Packing Hybrid encoded data.
    ///
    /// The `[offset, offset + length)` slice is **copied** into a private array.
    /// A cursor outlives the page-decode call that creates it — it is consumed
    /// later, on the drain thread — while the source `data` is typically a
    /// pooled decompression buffer that the next page decode overwrites. Owning
    /// its bytes keeps the cursor valid across that hand-off. The copy is of the
    /// small encoded stream, not the materialized value array the fused path
    /// avoids building.
    ///
    /// @param data the encoded byte array
    /// @param offset start offset in `data`
    /// @param length number of bytes in the stream
    /// @param bitWidth bit width of each encoded value
    public HybridStreamCursor(byte[] data, int offset, int length, int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Invalid RLE bit width: " + bitWidth
                    + ". Must be between 0 and 32");
        }
        this.data = Arrays.copyOfRange(data, offset, offset + length);
        this.dataEnd = length;
        this.pos = 0;
        this.bitWidth = bitWidth;
        this.bitMask = (bitWidth == 0) ? 0 : (1 << bitWidth) - 1;
        this.dataBuffer = ByteBuffer.wrap(this.data).order(ByteOrder.LITTLE_ENDIAN);
    }

    public int bitWidth() {
        return bitWidth;
    }

    public int pos() {
        return pos;
    }

    public int dataEnd() {
        return dataEnd;
    }

    /// Advances to the next run in the stream. Returns `true` if a run was
    /// loaded, `false` at end-of-stream.
    ///
    /// After a successful advance, [#isRle()], [#value()], and [#remaining()]
    /// describe the new run.
    public boolean advance() {
        if (bitWidth == 0 || pos >= dataEnd) {
            remaining = 0;
            return false;
        }

        long header = readUnsignedVarInt();

        if ((header & 1) == 1) {
            // Bit-packed: header >> 1 = number of 8-value groups
            remaining = (int) (header >> 1) * 8;
            rle = false;
        }
        else {
            // RLE: header >> 1 = repeat count
            remaining = (int) (header >> 1);
            currentValue = readRleValue();
            rle = true;
        }
        return remaining > 0;
    }

    /// Returns `true` if the current run is an RLE (constant-value) run.
    /// Only valid after a successful [#advance()].
    public boolean isRle() {
        return rle;
    }

    /// Returns the constant value of the current RLE run.
    /// Only valid when [#isRle()] is `true`.
    public int value() {
        return currentValue;
    }

    /// Returns the number of values remaining in the current run.
    public int remaining() {
        return remaining;
    }

    /// Consumes `count` values from the current run without reading them.
    /// Only meaningful for RLE runs where the value is already known.
    /// For bit-packed runs, this also skips the underlying encoded bits.
    ///
    /// @param count number of values to skip; must be ≤ [#remaining()]
    public void skip(int count) {
        if (count > remaining) {
            throw new IllegalArgumentException("Cannot skip " + count
                    + " values; only " + remaining + " remaining in current run");
        }
        remaining -= count;
        if (!rle) {
            // For bit-packed runs, we must actually consume the bits
            skipBitPacked(count);
        }
    }

    /// Unpacks up to `max` bit-packed values from the current run into `dst`
    /// starting at `offset`. Returns the number of values actually unpacked.
    ///
    /// Only valid when [#isRle()] is `false`. Decrements [#remaining()] by
    /// the returned count. Multiple calls may be needed to drain a run.
    ///
    /// @param dst destination array
    /// @param offset start position in `dst`
    /// @param max maximum number of values to unpack
    /// @return number of values unpacked (0 if run is exhausted)
    public int unpack(int[] dst, int offset, int max) {
        int toRead = Math.min(max, remaining);
        if (toRead <= 0) {
            return 0;
        }
        decodeBitPacked(dst, offset, toRead);
        remaining -= toRead;
        return toRead;
    }

    // ==================== Internal Decode ====================

    private int readRleValue() {
        int bytesNeeded = (bitWidth + 7) / 8;
        int value = 0;
        for (int i = 0; i < bytesNeeded && pos < dataEnd; i++) {
            value |= (data[pos++] & 0xFF) << (i * 8);
        }
        return value & bitMask;
    }

    /// Batch decode bit-packed values. Optimized paths for common bit widths.
    ///
    /// NOTE: this intentionally duplicates the logic in
    /// [RleBitPackingHybridDecoder#decodeBitPacked]. The cursor owns its byte
    /// array so it remains valid after the page-decode call that built it (the
    /// drain thread consumes it later while the decompression buffer is reused).
    /// Coupling the two implementations would re-introduce that lifetime
    /// dependency. If you improve one, update the other and keep them in sync.
    private void decodeBitPacked(int[] output, int outPos, int count) {
        final int width = bitWidth;
        final int mask = bitMask;

        // Drain leftover bits first
        while (bitsInBuffer >= width && count > 0) {
            output[outPos++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }

        // Fast path for bit width 1 (common for definition levels)
        if (width == 1 && bitsInBuffer == 0) {
            while (count >= 8 && pos < dataEnd) {
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
        // For widths 2-8: read 8 bytes at once when possible, extract 8 values.
        // Each group of 8 values occupies exactly `width` bytes in the stream
        // (width bits × 8 values = width bytes). getLong loads a long for convenient
        // shifting but only `width` bytes belong to the current group, so pos advances
        // by width, not by 8.
        else if (width <= 8 && bitsInBuffer == 0) {
            while (count >= 8 && pos + 8 <= dataEnd) {
                long bits = dataBuffer.getLong(pos);
                pos += width;
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
            while (count >= 8 && pos + width <= dataEnd) {
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

        // Handle remaining values
        while (count > 0) {
            while (bitsInBuffer < width && pos < dataEnd) {
                bitBuffer |= ((long) (data[pos++] & 0xFF)) << bitsInBuffer;
                bitsInBuffer += 8;
            }
            if (bitsInBuffer < width) {
                break;
            }
            output[outPos++] = (int) (bitBuffer & mask);
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }
    }

    /// Skips `count` bit-packed values without storing them.
    private void skipBitPacked(int count) {
        final int width = bitWidth;

        // Drain leftover bits first
        while (bitsInBuffer >= width && count > 0) {
            bitBuffer >>>= width;
            bitsInBuffer -= width;
            count--;
        }

        if (count == 0) {
            return;
        }

        // Skip whole bytes for remaining values
        long totalBits = (long) count * width;

        // First consume any partial byte in the bit buffer
        if (bitsInBuffer > 0) {
            // The bits in buffer are already loaded; we can skip values from them
            // but that's already handled above. Just clear the buffer.
            totalBits -= bitsInBuffer;
            bitsInBuffer = 0;
            bitBuffer = 0;
        }

        if (totalBits > 0) {
            int bytesToSkip = (int) (totalBits / 8);
            int remainingBits = (int) (totalBits % 8);
            pos += bytesToSkip;

            // If there are remaining bits, load the next byte and skip them
            if (remainingBits > 0 && pos < dataEnd) {
                bitBuffer = (data[pos++] & 0xFF);
                bitsInBuffer = 8;
                bitBuffer >>>= remainingBits;
                bitsInBuffer -= remainingBits;
            }
        }
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
