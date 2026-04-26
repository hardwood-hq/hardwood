/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.metadata.PhysicalType;

/// BYTE_STREAM_SPLIT decoder that reads directly from a [MemorySegment] (native memory).
///
/// This is the Java 22+ counterpart to [ByteStreamSplitDecoder]. It is used when
/// the compressed page data was decompressed into a thread-local native buffer by
/// [dev.hardwood.internal.compression.libdeflate.LibdeflateDecompressor#decompressToSegment],
/// eliminating the intermediate native-to-heap copy.
///
/// ## BYTE_STREAM_SPLIT access pattern
///
/// For N values of K bytes each, the data is laid out as K contiguous byte streams:
/// ```
/// [byte0_of_val0][byte0_of_val1]...[byte0_of_valN-1]  ← stream 0  (N bytes)
/// [byte1_of_val0][byte1_of_val1]...[byte1_of_valN-1]  ← stream 1  (N bytes)
/// ...
/// [byteK-1_of_val0]...                                 ← stream K-1 (N bytes)
/// ```
///
/// Decoding value `i` requires gathering byte `k` from each stream at index
/// `baseOffset + k * numValues + i`. This is random-access by nature — the
/// performance win over [ByteStreamSplitDecoder] is exclusively the elimination
/// of the native-to-heap `byte[]` copy before decoding, not faster per-element
/// access.
public class NativeBssDecoder implements ValueDecoder {

    private final MemorySegment data;
    private final long baseOffset;
    private final int numValues;
    private final int byteWidth;
    private int currentIndex = 0;

    /// Creates a decoder over a native [MemorySegment] slice.
    ///
    /// @param data             the segment containing decompressed page data
    /// @param offset           byte offset within `data` where the BSS streams begin
    /// @param numNonNullValues number of encoded values (stream length)
    /// @param type             Parquet physical type
    /// @param typeLength       type length for FIXED_LEN_BYTE_ARRAY (may be null)
    public NativeBssDecoder(MemorySegment data, long offset, int numNonNullValues,
                            PhysicalType type, Integer typeLength) {
        this.data = data;
        this.baseOffset = offset;
        this.numValues = numNonNullValues;
        this.byteWidth = getByteWidth(type, typeLength);

        long expectedLength = (long) numNonNullValues * byteWidth;
        long available = data.byteSize() - offset;
        if (available < expectedLength) {
            throw new IllegalArgumentException(
                    "NativeBssDecoder: insufficient data — expected " + expectedLength
                    + " bytes for " + numNonNullValues + " values of " + byteWidth
                    + " bytes, got " + available);
        }
    }

    private static int getByteWidth(PhysicalType type, Integer typeLength) {
        return switch (type) {
            case FLOAT, INT32 -> 4;
            case DOUBLE, INT64 -> 8;
            case FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength == null) {
                    throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY requires typeLength");
                }
                yield typeLength;
            }
            default -> throw new UnsupportedOperationException(
                    "BYTE_STREAM_SPLIT not supported for type: " + type);
        };
    }

    // ── Typed read methods ──────────────────────────────────────────────────

    @Override
    public void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buf.rewind();
                output[i] = buf.getDouble();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buf.rewind();
                    output[i] = buf.getDouble();
                }
            }
        }
    }

    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buf.rewind();
                output[i] = buf.getLong();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buf.rewind();
                    output[i] = buf.getLong();
                }
            }
        }
    }

    @Override
    public void readInts(int[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[4];
        ByteBuffer buf = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buf.rewind();
                output[i] = buf.getInt();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buf.rewind();
                    output[i] = buf.getInt();
                }
            }
        }
    }

    @Override
    public void readFloats(float[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        byte[] valueBytes = new byte[4];
        ByteBuffer buf = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN);
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                gatherBytes(valueBytes);
                buf.rewind();
                output[i] = buf.getFloat();
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    gatherBytes(valueBytes);
                    buf.rewind();
                    output[i] = buf.getFloat();
                }
            }
        }
    }

    @Override
    public void readByteArrays(byte[][] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            for (int i = 0; i < output.length; i++) {
                byte[] valueBytes = new byte[byteWidth];
                gatherBytes(valueBytes);
                output[i] = valueBytes;
            }
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    byte[] valueBytes = new byte[byteWidth];
                    gatherBytes(valueBytes);
                    output[i] = valueBytes;
                }
            }
        }
    }

    // ── Stream gather ───────────────────────────────────────────────────────

    /// Gathers bytes for value `currentIndex` from each byte stream into `valueBytes`.
    /// Each stream k starts at `baseOffset + k * numValues`. Byte k of the current
    /// value lives at `baseOffset + k * numValues + currentIndex`.
    private void gatherBytes(byte[] valueBytes) throws IOException {
        if (currentIndex >= numValues) {
            throw new IOException("NativeBssDecoder: no more values (numValues=" + numValues + ")");
        }
        for (int k = 0; k < valueBytes.length; k++) {
            long streamOff = (long) k * numValues;
            valueBytes[k] = data.get(ValueLayout.JAVA_BYTE, baseOffset + streamOff + currentIndex);
        }
        currentIndex++;
    }
}
