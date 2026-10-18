/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Encoder for BYTE_STREAM_SPLIT, the layout `FlatPageValues.byteStreamSplit` and
/// `BinaryPageValues.byteStreamSplit` read in place.
///
/// A value's K bytes are scattered across K streams, stream `k` holding byte `k` of every value
/// in turn, and the streams are written one after another. The encoded length is exactly the
/// plain length — this encoding changes no page's size at all.
///
/// What it changes is what the codec afterwards can do. Neighbouring floating-point values
/// typically agree in their exponent and high mantissa bits and differ in the low ones, so a
/// stream of like-positioned bytes is far more repetitive than the interleaved values are. Its
/// whole payoff is therefore a property of the compression that follows it, which is why it is
/// asked for rather than chosen by measurement.
public final class ByteStreamSplitEncoder {

    private ByteStreamSplitEncoder() {
    }

    /// Splits `count` fixed-width values held end to end in `data` starting at `from`.
    ///
    /// @param data the little-endian value bytes, `byteWidth` per value
    /// @param from index of the first byte to encode
    /// @param count how many values to encode
    /// @param byteWidth the type's byte width
    /// @return the encoded bytes, `count * byteWidth` of them
    public static byte[] encode(byte[] data, int from, int count, int byteWidth) {
        byte[] out = new byte[Math.multiplyExact(count, byteWidth)];
        encode(data, from, count, byteWidth, out, 0);
        return out;
    }

    /// Splits `count` fixed-width values held end to end in `data` starting at `from` into `dest`
    /// at `destOffset`, which must have `count * byteWidth` bytes free.
    ///
    /// @param data the little-endian value bytes, `byteWidth` per value
    /// @param from index of the first byte to encode
    /// @param count how many values to encode
    /// @param byteWidth the type's byte width
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void encode(byte[] data, int from, int count, int byteWidth, byte[] dest, int destOffset) {
        for (int k = 0; k < byteWidth; k++) {
            int stream = destOffset + k * count;
            int source = from + k;
            for (int i = 0; i < count; i++) {
                dest[stream + i] = data[source];
                source += byteWidth;
            }
        }
    }

    /// Splits `count` `INT32` values straight from the value store, without the `PLAIN` buffer an
    /// [#encode(byte[], int, int, int, byte[], int)] would need first. Stream `k` holds byte `k`
    /// of each value little-endian, which is byte `k` of what `PLAIN` would have written.
    ///
    /// @param values the backing array
    /// @param from the index of the first value to encode
    /// @param count how many values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void splitInts(int[] values, int from, int count, byte[] dest, int destOffset) {
        for (int i = 0; i < count; i++) {
            int value = values[from + i];
            for (int k = 0; k < Integer.BYTES; k++) {
                dest[destOffset + k * count + i] = (byte) (value >>> (Byte.SIZE * k));
            }
        }
    }

    /// Splits `count` `INT64` values straight from the value store.
    ///
    /// @param values the backing array
    /// @param from the index of the first value to encode
    /// @param count how many values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void splitLongs(long[] values, int from, int count, byte[] dest, int destOffset) {
        for (int i = 0; i < count; i++) {
            long value = values[from + i];
            for (int k = 0; k < Long.BYTES; k++) {
                dest[destOffset + k * count + i] = (byte) (value >>> (Byte.SIZE * k));
            }
        }
    }

    /// Splits `count` `FLOAT` values straight from the value store. The bits are taken raw, so a
    /// `NaN` keeps the payload it was written with rather than a canonical one.
    ///
    /// @param values the backing array
    /// @param from the index of the first value to encode
    /// @param count how many values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void splitFloats(float[] values, int from, int count, byte[] dest, int destOffset) {
        for (int i = 0; i < count; i++) {
            int value = Float.floatToRawIntBits(values[from + i]);
            for (int k = 0; k < Float.BYTES; k++) {
                dest[destOffset + k * count + i] = (byte) (value >>> (Byte.SIZE * k));
            }
        }
    }

    /// Splits `count` `DOUBLE` values straight from the value store, taking the bits raw as
    /// [#splitFloats] does.
    ///
    /// @param values the backing array
    /// @param from the index of the first value to encode
    /// @param count how many values to encode
    /// @param dest the buffer to encode into
    /// @param destOffset the offset in `dest` at which to start
    public static void splitDoubles(double[] values, int from, int count, byte[] dest, int destOffset) {
        for (int i = 0; i < count; i++) {
            long value = Double.doubleToRawLongBits(values[from + i]);
            for (int k = 0; k < Double.BYTES; k++) {
                dest[destOffset + k * count + i] = (byte) (value >>> (Byte.SIZE * k));
            }
        }
    }
}
