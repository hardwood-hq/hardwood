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
import java.nio.ByteOrder;

import dev.hardwood.metadata.PhysicalType;

/// PLAIN-encoding decoder that reads directly from a [MemorySegment] (native memory).
///
/// This is the Java 22+ counterpart to [PlainDecoder]. It is used when the
/// compressed page data was decompressed into a thread-local native buffer by
/// [dev.hardwood.internal.compression.libdeflate.LibdeflateDecompressor#decompressToSegment],
/// eliminating the intermediate native-to-heap copy that the `byte[]`-based
/// [PlainDecoder] would otherwise require.
///
/// **Bulk numeric reads** (`readLongs`, `readDoubles`, `readInts`, `readFloats`)
/// use `MemorySegment.asByteBuffer()` to obtain a little-endian view and then
/// bulk-transfer into the caller's primitive array via the typed-buffer API.
/// The JIT compiles this to an efficient vectorised memory load — no element-by-element
/// iteration occurs for the dense (no-nulls) case.
///
/// **Non-numeric types** (BOOLEAN, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96) are
/// not implemented; callers fall back to the `byte[]` path via
/// [dev.hardwood.internal.compression.libdeflate.LibdeflateDecompressor#decompress].
public class NativePlainDecoder implements ValueDecoder {

    // Little-endian unaligned layout descriptors reused for every get() call.
    private static final ValueLayout.OfLong   LE_LONG   =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfDouble LE_DOUBLE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt    LE_INT    =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat  LE_FLOAT  =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final MemorySegment data;
    private final PhysicalType type;
    private final Integer typeLength;
    private long pos;

    /// Creates a decoder over a native [MemorySegment] slice.
    ///
    /// @param data   the segment containing decompressed page data
    /// @param offset byte offset within `data` where value encoding begins
    /// @param type   Parquet physical type
    /// @param typeLength type length for FIXED_LEN_BYTE_ARRAY (may be null)
    public NativePlainDecoder(MemorySegment data, long offset, PhysicalType type, Integer typeLength) {
        this.data = data;
        this.pos = offset;
        this.type = type;
        this.typeLength = typeLength;
    }

    // ── Bulk numeric reads ──────────────────────────────────────────────────

    /// Reads INT64 values directly into a primitive `long[]`.
    ///
    /// Dense path (no nulls): wraps the native segment slice as a little-endian
    /// `LongBuffer` and bulk-transfers into `output`. The JIT typically emits
    /// a vectorised load for this pattern.
    ///
    /// Sparse path (with nulls): reads one `long` at a time using
    /// `MemorySegment.get(LE_LONG, pos)`.
    @Override
    public void readLongs(long[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            long numBytes = (long) output.length * Long.BYTES;
            checkAvailable(numBytes);
            data.asSlice(pos, numBytes).asByteBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(output);
            pos += numBytes;
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    checkAvailable(Long.BYTES);
                    output[i] = data.get(LE_LONG, pos);
                    pos += Long.BYTES;
                }
            }
        }
    }

    /// Reads DOUBLE values directly into a primitive `double[]`.
    @Override
    public void readDoubles(double[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            long numBytes = (long) output.length * Double.BYTES;
            checkAvailable(numBytes);
            data.asSlice(pos, numBytes).asByteBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(output);
            pos += numBytes;
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    checkAvailable(Double.BYTES);
                    output[i] = data.get(LE_DOUBLE, pos);
                    pos += Double.BYTES;
                }
            }
        }
    }

    /// Reads INT32 values directly into a primitive `int[]`.
    @Override
    public void readInts(int[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            long numBytes = (long) output.length * Integer.BYTES;
            checkAvailable(numBytes);
            data.asSlice(pos, numBytes).asByteBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(output);
            pos += numBytes;
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    checkAvailable(Integer.BYTES);
                    output[i] = data.get(LE_INT, pos);
                    pos += Integer.BYTES;
                }
            }
        }
    }

    /// Reads FLOAT values directly into a primitive `float[]`.
    @Override
    public void readFloats(float[] output, int[] definitionLevels, int maxDefLevel) throws IOException {
        if (definitionLevels == null) {
            long numBytes = (long) output.length * Float.BYTES;
            checkAvailable(numBytes);
            data.asSlice(pos, numBytes).asByteBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(output);
            pos += numBytes;
        }
        else {
            for (int i = 0; i < output.length; i++) {
                if (definitionLevels[i] == maxDefLevel) {
                    checkAvailable(Float.BYTES);
                    output[i] = data.get(LE_FLOAT, pos);
                    pos += Float.BYTES;
                }
            }
        }
    }

    // ── Bounds check ────────────────────────────────────────────────────────

    private void checkAvailable(long needed) throws IOException {
        if (pos + needed > data.byteSize()) {
            throw new IOException(
                    "NativePlainDecoder: unexpected EOF reading " + type +
                    " at offset " + pos + " (need " + needed +
                    ", segment size " + data.byteSize() + ")");
        }
    }
}
