/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/// Encoder for RLE/Bit-Packing Hybrid encoding, the inverse of
/// [RleBitPackingHybridDecoder]. Used for definition/repetition levels (via
/// [LevelEncoder]) and for a data page's dictionary index stream.
///
/// Values that repeat at least eight times in a row are emitted as an RLE run; shorter
/// stretches are bit-packed in groups of eight. Emitting a single RLE run for a constant
/// stream is what lets the reader take its all-present fast path on a fully-populated
/// optional column. The bit-packed byte layout is little-endian bit order, matching the
/// decoder: value `i` of a group occupies bits `[i·bitWidth, (i+1)·bitWidth)`.
///
/// A zero-bit stream — the dictionary indices of a chunk whose dictionary holds a single
/// entry — is one RLE run whose values occupy no bytes, so only the run header is written.
/// The header cannot be skipped even though it carries no value: a decoder reads the run
/// length from the stream rather than from the page's value count.
public final class RleBitPackingHybridEncoder {

    private int bitWidth;

    private byte[] buffer = new byte[64];
    private int length;

    // Values buffered for the current, not-yet-decided run (up to one group of eight).
    private final int[] bufferedValues = new int[8];
    private int numBufferedValues;

    // Run-length tracking for the value currently being repeated.
    private int previousValue;
    private int repeatCount;

    // Open bit-packed run: the index of its header byte (-1 when none is open) and the
    // number of eight-value groups written into it so far.
    private int bitPackedRunHeaderIndex = -1;
    private int bitPackedGroupCount;

    private boolean finished;

    /// @param bitWidth number of bits per value, 0–32
    public RleBitPackingHybridEncoder(int bitWidth) {
        requireValidBitWidth(bitWidth);
        this.bitWidth = bitWidth;
    }

    private static void requireValidBitWidth(int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Invalid RLE bit width: " + bitWidth + ". Must be between 0 and 32");
        }
    }

    /// Appends `count` values starting at `offset`.
    ///
    /// Produces the bytes [#writeInt] would for the same values, eight at a time where it can. A
    /// run of eight equal values only ever starts where the encoder holds nothing buffered —
    /// eight values arriving after a buffered one fill the group before they can count as a run —
    /// so from such a position the next eight values are either an RLE run, extended while the
    /// value repeats, or one bit-packed group taken straight from `values`. Values are passed to
    /// [#writeInt] one at a time only to reach such a position, and for a tail shorter than a
    /// group.
    public void writeInts(int[] values, int offset, int count) {
        requireUnfinished();
        if (bitWidth == 0) {
            repeatCount += count;
            return;
        }
        int i = offset;
        int end = offset + count;
        while (i < end) {
            if (repeatCount >= 8) {
                // An RLE run is open: it takes every further repeat of its value.
                int value = previousValue;
                while (i < end && values[i] == value) {
                    repeatCount++;
                    i++;
                }
                if (i < end) {
                    writeRleRun();
                }
            }
            else if (numBufferedValues > 0 || end - i < 8) {
                writeInt(values[i++]);
            }
            else if (isRun(values, i)) {
                // Opened with nothing buffered, which [#writeInt] treats alike: the run's values
                // are counted, not buffered, until a different value or the end closes it.
                previousValue = values[i];
                repeatCount = 8;
                i += 8;
            }
            else {
                writeOrAppendBitPackedRun(values, i);
                previousValue = values[i + 7];
                i += 8;
            }
        }
    }

    /// Appends `count` values starting at `offset`, each read as an unsigned byte, producing the
    /// bytes [#writeInts] would for the same values widened to `int`.
    public void writeUnsignedBytes(byte[] values, int offset, int count) {
        for (int from = 0; from < count; from += widened.length) {
            int length = Math.min(widened.length, count - from);
            for (int k = 0; k < length; k++) {
                widened[k] = values[offset + from + k] & 0xFF;
            }
            writeInts(widened, 0, length);
        }
    }

    /// Scratch for [#writeUnsignedBytes], which widens its bytes a slice at a time.
    private final int[] widened = new int[64];

    /// Whether the eight values from `from` are all the same.
    private static boolean isRun(int[] values, int from) {
        int value = values[from];
        for (int k = 1; k < 8; k++) {
            if (values[from + k] != value) {
                return false;
            }
        }
        return true;
    }

    private void requireUnfinished() {
        if (finished) {
            throw new IllegalStateException("Encoder already finished");
        }
    }

    /// Appends a single value.
    public void writeInt(int value) {
        requireUnfinished();
        if (bitWidth == 0) {
            // Zero bits leave only one representable value, so the whole stream is one RLE
            // run and the value itself needs no bytes. The run header is still required: a
            // decoder takes the run length from the stream rather than from the page's value
            // count, and reads past the end without it.
            repeatCount++;
            return;
        }
        if (value == previousValue) {
            repeatCount++;
            if (repeatCount >= 8) {
                // Certain to become an RLE run; keep counting and defer the emit.
                return;
            }
        }
        else {
            if (repeatCount >= 8) {
                writeRleRun();
            }
            repeatCount = 1;
            previousValue = value;
        }

        bufferedValues[numBufferedValues++] = value;
        if (numBufferedValues == 8) {
            writeOrAppendBitPackedRun(bufferedValues, 0);
        }
    }

    /// Finishes the stream and returns the encoded bytes. The encoder must not be written
    /// to afterwards.
    public byte[] toByteArray() {
        finish();
        return Arrays.copyOf(buffer, length);
    }

    /// Finishes the stream and returns how many bytes it occupies; [#buffer] then holds them,
    /// starting at zero.
    ///
    /// The bytes are lent rather than written into a sink of the caller's, so that the one sink
    /// on this path — the page body under construction — needs no particular type and can be a
    /// plain growable array rather than a synchronized [ByteArrayOutputStream]. The encoder must
    /// not be written to afterwards.
    public int finished() {
        finish();
        return length;
    }

    /// The backing array [#finished] reports the length of. Valid until the next reset.
    ///
    /// Call [#finished] **first and into a local**: finishing may replace this array, and
    /// `write(buffer(), 0, finished())` evaluates the array reference before the call that
    /// replaces it.
    public byte[] buffer() {
        return buffer;
    }

    /// Empties the encoder for another stream at `bitWidth`, keeping the buffer it grew. One
    /// encoder can then serve every page of a column chunk — each page's index stream is its
    /// own, at its own bit width — instead of one being allocated and regrown per page.
    public void reset(int bitWidth) {
        requireValidBitWidth(bitWidth);
        this.bitWidth = bitWidth;
        length = 0;
        numBufferedValues = 0;
        previousValue = 0;
        repeatCount = 0;
        bitPackedRunHeaderIndex = -1;
        bitPackedGroupCount = 0;
        finished = false;
    }

    /// Closes whichever run is open, after which [#length] bytes of [#buffer] are the stream.
    private void finish() {
        if (finished) {
            return;
        }
        if (bitWidth == 0) {
            if (repeatCount > 0) {
                writeRleRun();
            }
        }
        else if (repeatCount >= 8) {
            writeRleRun();
        }
        else if (numBufferedValues > 0) {
            Arrays.fill(bufferedValues, numBufferedValues, 8, 0);
            writeOrAppendBitPackedRun(bufferedValues, 0);
            endPreviousBitPackedRun();
        }
        else {
            endPreviousBitPackedRun();
        }
        finished = true;
    }

    /// Writes the eight values from `from` as one bit-packed group, appending it to the open
    /// bit-packed run or opening one.
    private void writeOrAppendBitPackedRun(int[] values, int from) {
        if (bitPackedGroupCount >= 63) {
            // A bit-packed run header counts groups in the upper bits of one byte, so a
            // run holds at most 63 groups; start a fresh one past that.
            endPreviousBitPackedRun();
        }
        if (bitPackedRunHeaderIndex == -1) {
            // Reserve the header byte; its final value is only known once the run ends.
            write(0);
            bitPackedRunHeaderIndex = length - 1;
        }
        packGroup(values, from);
        numBufferedValues = 0;
        // The buffered values are now written as a bit-packed group, so they must not also
        // be counted toward a later RLE run.
        repeatCount = 0;
        bitPackedGroupCount++;
    }

    private void endPreviousBitPackedRun() {
        if (bitPackedRunHeaderIndex == -1) {
            return;
        }
        buffer[bitPackedRunHeaderIndex] = (byte) ((bitPackedGroupCount << 1) | 1);
        bitPackedRunHeaderIndex = -1;
        bitPackedGroupCount = 0;
    }

    private void writeRleRun() {
        // Close any open bit-packed run before switching to an RLE run.
        endPreviousBitPackedRun();
        writeUnsignedVarInt(repeatCount << 1);
        int byteCount = (bitWidth + 7) / 8;
        for (int i = 0; i < byteCount; i++) {
            write((previousValue >>> (i * 8)) & 0xFF);
        }
        repeatCount = 0;
        numBufferedValues = 0;
    }

    /// Packs the eight values from `from` into `bitWidth` bytes, LSB-first, matching the
    /// decoder's little-endian bit order. The layout is [BitPacker]'s, shared with
    /// DELTA_BINARY_PACKED's 32-value miniblocks.
    private void packGroup(int[] values, int from) {
        int packed = BitPacker.packedLength(8, bitWidth);
        ensureCapacity(packed);
        length += BitPacker.pack(values, from, 8, bitWidth, buffer, length);
    }

    private void writeUnsignedVarInt(int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            write((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        write(v);
    }

    private void write(int b) {
        ensureCapacity(1);
        buffer[length++] = (byte) b;
    }

    private void ensureCapacity(int extra) {
        if (length + extra > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(length + extra, buffer.length * 2));
        }
    }
}
