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

/// Encoder for the PLAIN encoding, the layout the reader's `PLAIN` value readers
/// (`FlatPageValues.plain`, `BinaryPageValues.plain`) read in place.
public final class PlainEncoder {

    private PlainEncoder() {
    }

    /// Encode INT32 values as little-endian 4-byte words.
    ///
    /// @param values the values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    ///         (more than `Integer.MAX_VALUE / 4` values)
    public static byte[] encodeInts(int[] values) {
        return encodeInts(values, 0, values.length);
    }

    /// Encode `length` INT32 values starting at `offset` as little-endian 4-byte words,
    /// Encoding a sub-range avoids copying a page's
    /// values out of a larger backing array before encoding.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    ///         (more than `Integer.MAX_VALUE / 4` values)
    public static byte[] encodeInts(int[] values, int offset, int length) {
        byte[] dest = new byte[fixedWidthLength(length, Integer.BYTES)];
        encodeInts(values, offset, length, dest, 0);
        return dest;
    }

    /// Encode `length` INT32 values starting at `offset` into `dest` at `destOffset`, which must
    /// have `length * 4` bytes free. The destination form is what the page path uses: the section
    /// is produced straight into the page body rather than into an array the caller then copies.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeInts(int[] values, int offset, int length, byte[] dest, int destOffset) {
        littleEndian(dest, destOffset, fixedWidthLength(length, Integer.BYTES))
                .asIntBuffer().put(values, offset, length);
    }

    /// Encode `length` INT64 values starting at `offset` as little-endian 8-byte words.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static byte[] encodeLongs(long[] values, int offset, int length) {
        byte[] dest = new byte[fixedWidthLength(length, Long.BYTES)];
        encodeLongs(values, offset, length, dest, 0);
        return dest;
    }

    /// Encode `length` INT64 values starting at `offset` into `dest` at `destOffset`, which must
    /// have `length * 8` bytes free.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeLongs(long[] values, int offset, int length, byte[] dest, int destOffset) {
        littleEndian(dest, destOffset, fixedWidthLength(length, Long.BYTES))
                .asLongBuffer().put(values, offset, length);
    }

    /// Encode `length` FLOAT values starting at `offset` as little-endian IEEE-754 single
    /// words.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static byte[] encodeFloats(float[] values, int offset, int length) {
        byte[] dest = new byte[fixedWidthLength(length, Float.BYTES)];
        encodeFloats(values, offset, length, dest, 0);
        return dest;
    }

    /// Encode `length` FLOAT values starting at `offset` into `dest` at `destOffset`, which must
    /// have `length * 4` bytes free.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeFloats(float[] values, int offset, int length, byte[] dest, int destOffset) {
        littleEndian(dest, destOffset, fixedWidthLength(length, Float.BYTES))
                .asFloatBuffer().put(values, offset, length);
    }

    /// Encode `length` DOUBLE values starting at `offset` as little-endian IEEE-754 double
    /// words.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static byte[] encodeDoubles(double[] values, int offset, int length) {
        byte[] dest = new byte[fixedWidthLength(length, Double.BYTES)];
        encodeDoubles(values, offset, length, dest, 0);
        return dest;
    }

    /// Encode `length` DOUBLE values starting at `offset` into `dest` at `destOffset`, which must
    /// have `length * 8` bytes free.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeDoubles(double[] values, int offset, int length, byte[] dest, int destOffset) {
        littleEndian(dest, destOffset, fixedWidthLength(length, Double.BYTES))
                .asDoubleBuffer().put(values, offset, length);
    }

    /// Encode `length` BOOLEAN values starting at `offset` bit-packed, 8 values per byte,
    /// least-significant bit first.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    public static byte[] encodeBooleans(boolean[] values, int offset, int length) {
        byte[] packed = new byte[booleansLength(length)];
        encodeBooleans(values, offset, length, packed, 0);
        return packed;
    }

    /// Encode `length` BOOLEAN values starting at `offset` into `dest` at `destOffset`, which must
    /// have [#booleansLength] bytes free. Those bytes are written whole rather than or-ed into,
    /// so a reserved region carrying earlier content still yields the values alone.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeBooleans(boolean[] values, int offset, int length, byte[] dest, int destOffset) {
        Arrays.fill(dest, destOffset, destOffset + booleansLength(length), (byte) 0);
        for (int i = 0; i < length; i++) {
            if (values[offset + i]) {
                dest[destOffset + (i >> 3)] |= (byte) (1 << (i & 7));
            }
        }
    }

    /// Encode `length` BOOLEAN values starting at `offset` from a packed bitset: value `i` is bit
    /// `i & 63` of `bits[i >>> 6]`. The output restarts the run at bit 0 of its first byte, as
    /// [#encodeBooleans(boolean[], int, int)] does for values held one `boolean` each, because a
    /// page's value section begins on a byte boundary wherever the page was cut.
    ///
    /// @param bits the packed value bits
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    public static byte[] encodeBooleans(long[] bits, int offset, int length) {
        byte[] packed = new byte[booleansLength(length)];
        encodeBooleans(bits, offset, length, packed, 0);
        return packed;
    }

    /// Encode `length` BOOLEAN values starting at `offset` from a packed bitset into `dest` at
    /// `destOffset`, which must have [#booleansLength] bytes free.
    ///
    /// @param bits the packed value bits
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeBooleans(long[] bits, int offset, int length, byte[] dest, int destOffset) {
        Arrays.fill(dest, destOffset, destOffset + booleansLength(length), (byte) 0);
        for (int i = 0; i < length; i++) {
            int index = offset + i;
            if ((bits[index >>> 6] & (1L << index)) != 0) {
                dest[destOffset + (i >> 3)] |= (byte) (1 << (i & 7));
            }
        }
    }

    /// The `PLAIN` length of `length` BOOLEAN values: one bit each, rounded up to a byte.
    public static int booleansLength(int length) {
        return (length + Byte.SIZE - 1) / Byte.SIZE;
    }

    /// Encode `length` `BYTE_ARRAY` values starting at `offset`, each as a 4-byte little-endian
    /// length prefix followed by its bytes.
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    public static byte[] encodeByteArrays(byte[][] values, int offset, int length) {
        long total = 0;
        for (int i = 0; i < length; i++) {
            total += Integer.BYTES + values[offset + i].length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(total)).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < length; i++) {
            byte[] value = values[offset + i];
            buffer.putInt(value.length);
            buffer.put(value);
        }
        return buffer.array();
    }

    /// Encode `length` `BYTE_ARRAY` values starting at `offset` from a packed buffer: value `i`
    /// occupies `data[offsets[i], offsets[i + 1])`. Each is written as a 4-byte little-endian
    /// length prefix followed by its bytes, as [#encodeByteArrays(byte[][], int, int)] does for
    /// values held one array each.
    ///
    /// @param data the packed value bytes
    /// @param offsets value boundaries, `length + offset + 1` entries at least
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @return the PLAIN-encoded bytes
    public static byte[] encodeByteArrays(byte[] data, int[] offsets, int offset, int length) {
        byte[] dest = new byte[byteArraysLength(offsets, offset, length)];
        encodeByteArrays(data, offsets, offset, length, dest, 0);
        return dest;
    }

    /// Encode `length` packed `BYTE_ARRAY` values starting at `offset` into `dest` at
    /// `destOffset`, which must have [#byteArraysLength] bytes free.
    ///
    /// @param data the packed value bytes
    /// @param offsets value boundaries, `length + offset + 1` entries at least
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encodeByteArrays(byte[] data, int[] offsets, int offset, int length,
                                        byte[] dest, int destOffset) {
        int at = destOffset;
        for (int i = 0; i < length; i++) {
            int from = offsets[offset + i];
            int valueLength = offsets[offset + i + 1] - from;
            writeIntLittleEndian(dest, at, valueLength);
            at += Integer.BYTES;
            System.arraycopy(data, from, dest, at, valueLength);
            at += valueLength;
        }
    }

    /// The `PLAIN` length of `length` packed `BYTE_ARRAY` values from `offset`: each value's bytes
    /// behind a 4-byte length prefix.
    ///
    /// @param offsets value boundaries, `length + offset + 1` entries at least
    /// @param offset the index of the first value
    /// @param length the number of values
    /// @return the encoded length
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static int byteArraysLength(int[] offsets, int offset, int length) {
        long total = (long) Integer.BYTES * length + offsets[offset + length] - offsets[offset];
        return Math.toIntExact(total);
    }

    /// Writes `value` little-endian at `at`, the length prefix form a `BYTE_ARRAY` value carries.
    private static void writeIntLittleEndian(byte[] dest, int at, int value) {
        dest[at] = (byte) value;
        dest[at + 1] = (byte) (value >>> 8);
        dest[at + 2] = (byte) (value >>> 16);
        dest[at + 3] = (byte) (value >>> 24);
    }

    /// Encode `length` `FIXED_LEN_BYTE_ARRAY` values starting at `offset` from a packed buffer as
    /// raw concatenated bytes, the packed counterpart of
    /// [#encodeFixedLenByteArrays(byte[][], int, int, int)].
    ///
    /// @param data the packed value bytes
    /// @param offsets value boundaries, `length + offset + 1` entries at least
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param typeLength the fixed byte length every value must have
    /// @return the PLAIN-encoded bytes
    /// @throws IllegalArgumentException if any value's length is not `typeLength`
    public static byte[] encodeFixedLenByteArrays(byte[] data, int[] offsets, int offset, int length,
                                                  int typeLength) {
        byte[] dest = new byte[fixedWidthLength(length, typeLength)];
        encodeFixedLenByteArrays(data, offsets, offset, length, typeLength, dest, 0);
        return dest;
    }

    /// Encode `length` packed `FIXED_LEN_BYTE_ARRAY` values starting at `offset` into `dest` at
    /// `destOffset`, which must have `length * typeLength` bytes free.
    ///
    /// @param data the packed value bytes
    /// @param offsets value boundaries, `length + offset + 1` entries at least
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param typeLength the fixed byte length every value must have
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    /// @throws IllegalArgumentException if any value's length is not `typeLength`
    public static void encodeFixedLenByteArrays(byte[] data, int[] offsets, int offset, int length,
                                                int typeLength, byte[] dest, int destOffset) {
        int at = destOffset;
        for (int i = 0; i < length; i++) {
            int from = offsets[offset + i];
            int valueLength = offsets[offset + i + 1] - from;
            if (valueLength != typeLength) {
                throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY value has length " + valueLength
                        + " but the column's type length is " + typeLength);
            }
            System.arraycopy(data, from, dest, at, valueLength);
            at += valueLength;
        }
    }

    /// Encode `length` `FIXED_LEN_BYTE_ARRAY` values starting at `offset` as raw concatenated
    /// bytes (no length prefix — the width is fixed by the schema).
    ///
    /// @param values the backing array
    /// @param offset the index of the first value to encode
    /// @param length the number of values to encode
    /// @param typeLength the fixed byte length every value must have
    /// @return the PLAIN-encoded bytes
    /// @throws IllegalArgumentException if any value's length is not `typeLength`
    public static byte[] encodeFixedLenByteArrays(byte[][] values, int offset, int length, int typeLength) {
        ByteBuffer buffer = ByteBuffer.allocate(Math.multiplyExact(length, typeLength));
        for (int i = 0; i < length; i++) {
            byte[] value = values[offset + i];
            if (value.length != typeLength) {
                throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY value has length " + value.length
                        + " but the column's type length is " + typeLength);
            }
            buffer.put(value);
        }
        return buffer.array();
    }

    /// The `PLAIN` length of `count` values of `width` bytes each.
    ///
    /// @param count the number of values
    /// @param width the bytes one value occupies
    /// @return the encoded length
    /// @throws ArithmeticException if the encoded size overflows an `int`
    public static int fixedWidthLength(int count, int width) {
        return Math.multiplyExact(count, width);
    }

    /// A little-endian view of `length` bytes of `dest` from `destOffset`.
    private static ByteBuffer littleEndian(byte[] dest, int destOffset, int length) {
        return ByteBuffer.wrap(dest, destOffset, length).order(ByteOrder.LITTLE_ENDIAN);
    }
}
