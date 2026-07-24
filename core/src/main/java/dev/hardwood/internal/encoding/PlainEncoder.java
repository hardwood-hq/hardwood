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

/// Encoder for the PLAIN encoding, the inverse of [PlainDecoder].
public final class PlainEncoder {

    private PlainEncoder() {
    }

    /// Encode INT32 values as little-endian 4-byte words, matching
    /// [PlainDecoder#readInts].
    ///
    /// @param values the values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    ///         (more than `Integer.MAX_VALUE / 4` values)
    public static byte[] encodeInts(int[] values) {
        return encodeInts(values, 0, values.length);
    }

    /// Encode `length` INT32 values starting at `offset` as little-endian 4-byte words,
    /// matching [PlainDecoder#readInts]. Encoding a sub-range avoids copying a page's
    /// values out of a larger backing array before encoding.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    ///         (more than `Integer.MAX_VALUE / 4` values)
    public static byte[] encodeInts(int[] values, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(Math.multiplyExact(length, Integer.BYTES))
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asIntBuffer().put(values, offset, length);
        return buffer.array();
    }

    /// Encode `length` INT64 values starting at `offset` as little-endian 8-byte words,
    /// matching [PlainDecoder#readLongs].
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static byte[] encodeLongs(long[] values, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(Math.multiplyExact(length, Long.BYTES))
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asLongBuffer().put(values, offset, length);
        return buffer.array();
    }

    /// Encode `length` FLOAT values starting at `offset` as little-endian IEEE-754 single
    /// words, matching [PlainDecoder#readFloats].
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static byte[] encodeFloats(float[] values, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(Math.multiplyExact(length, Float.BYTES))
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asFloatBuffer().put(values, offset, length);
        return buffer.array();
    }

    /// Encode `length` DOUBLE values starting at `offset` as little-endian IEEE-754 double
    /// words, matching [PlainDecoder#readDoubles].
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static byte[] encodeDoubles(double[] values, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(Math.multiplyExact(length, Double.BYTES))
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asDoubleBuffer().put(values, offset, length);
        return buffer.array();
    }

    /// Encode `length` BOOLEAN values starting at `offset` bit-packed, 8 values per byte,
    /// least-significant bit first, matching [PlainDecoder#readBooleans].
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    public static byte[] encodeBooleans(boolean[] values, int offset, int length) {
        byte[] packed = new byte[(length + Byte.SIZE - 1) / Byte.SIZE];
        for (int i = 0; i < length; i++) {
            if (values[offset + i]) {
                packed[i >> 3] |= (byte) (1 << (i & 7));
            }
        }
        return packed;
    }
}
