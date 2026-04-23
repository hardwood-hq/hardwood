/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/// Minimal Variant-binary encoder. Writes primitive/short-string/array/object
/// headers and payloads into a growable scratch buffer — used by
/// [VariantShredReassembler] to compose canonical `value` bytes from shredded
/// Parquet columns.
///
/// This is **not** a general-purpose Variant writer: it only emits what
/// reassembly needs, and it is intentionally allocation-light so the hot path
/// stays on a reusable buffer held by the row reader.
public final class VariantValueEncoder {

    private VariantValueEncoder() {}

    // ==================== Single-byte header primitives ====================

    public static int writeNull(byte[] buf, int pos) {
        return writeByte(buf, pos, primHeader(VariantBinary.PRIM_NULL));
    }

    public static int writeBoolean(byte[] buf, int pos, boolean v) {
        int tag = v ? VariantBinary.PRIM_BOOLEAN_TRUE : VariantBinary.PRIM_BOOLEAN_FALSE;
        return writeByte(buf, pos, primHeader(tag));
    }

    // ==================== Fixed-width primitives ====================

    public static int writeInt8(byte[] buf, int pos, int v) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_INT8));
        buf[pos++] = (byte) v;
        return pos;
    }

    public static int writeInt16(byte[] buf, int pos, int v) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_INT16));
        return writeLE(buf, pos, v, 2);
    }

    public static int writeInt32(byte[] buf, int pos, int v) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_INT32));
        return writeLE(buf, pos, v, 4);
    }

    public static int writeInt64(byte[] buf, int pos, long v) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_INT64));
        return writeLongLE(buf, pos, v, 8);
    }

    public static int writeFloat(byte[] buf, int pos, float v) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_FLOAT));
        return writeLE(buf, pos, Float.floatToRawIntBits(v), 4);
    }

    public static int writeDouble(byte[] buf, int pos, double v) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_DOUBLE));
        return writeLongLE(buf, pos, Double.doubleToRawLongBits(v), 8);
    }

    public static int writeDate(byte[] buf, int pos, int epochDays) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_DATE));
        return writeLE(buf, pos, epochDays, 4);
    }

    public static int writeTimestampMicros(byte[] buf, int pos, long micros, boolean adjustedToUTC) {
        int tag = adjustedToUTC ? VariantBinary.PRIM_TIMESTAMP : VariantBinary.PRIM_TIMESTAMP_NTZ;
        pos = writeByte(buf, pos, primHeader(tag));
        return writeLongLE(buf, pos, micros, 8);
    }

    public static int writeTimestampNanos(byte[] buf, int pos, long nanos, boolean adjustedToUTC) {
        int tag = adjustedToUTC ? VariantBinary.PRIM_TIMESTAMP_NANOS : VariantBinary.PRIM_TIMESTAMP_NTZ_NANOS;
        pos = writeByte(buf, pos, primHeader(tag));
        return writeLongLE(buf, pos, nanos, 8);
    }

    public static int writeTimeMicros(byte[] buf, int pos, long micros) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_TIME_NTZ));
        return writeLongLE(buf, pos, micros, 8);
    }

    public static int writeUuid(byte[] buf, int pos, UUID uuid) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_UUID));
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 7; i >= 0; i--) {
            buf[pos++] = (byte) (msb >>> (i * 8));
        }
        for (int i = 7; i >= 0; i--) {
            buf[pos++] = (byte) (lsb >>> (i * 8));
        }
        return pos;
    }

    // ==================== Decimals ====================

    public static int writeDecimal4(byte[] buf, int pos, int unscaled, int scale) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_DECIMAL4));
        buf[pos++] = (byte) scale;
        return writeLE(buf, pos, unscaled, 4);
    }

    public static int writeDecimal8(byte[] buf, int pos, long unscaled, int scale) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_DECIMAL8));
        buf[pos++] = (byte) scale;
        return writeLongLE(buf, pos, unscaled, 8);
    }

    /// Write a 16-byte little-endian signed decimal. `unscaled` is a two's
    /// complement integer. Accepts up to 16 bytes of big-endian magnitude (as
    /// returned by [BigInteger#toByteArray()]).
    public static int writeDecimal16(byte[] buf, int pos, BigInteger unscaled, int scale) {
        byte[] be = unscaled.toByteArray();
        if (be.length > 16) {
            throw new IllegalArgumentException("Decimal16 unscaled value exceeds 16 bytes: " + be.length);
        }
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_DECIMAL16));
        buf[pos++] = (byte) scale;
        // Sign-extend on the left (MSB in big-endian), then emit little-endian.
        byte sign = (byte) (be[0] < 0 ? 0xFF : 0x00);
        int padding = 16 - be.length;
        for (int i = 0; i < 16; i++) {
            int beIdx = 15 - i; // little-endian byte i = big-endian byte (16-1-i)
            byte b;
            if (beIdx < padding) {
                b = sign;
            }
            else {
                b = be[beIdx - padding];
            }
            buf[pos++] = b;
        }
        return pos;
    }

    // ==================== Strings & binary ====================

    /// Write a UTF-8 string. Uses the short-string encoding (`basic_type=1`)
    /// when `bytes.length < 64`, else the length-prefixed primitive STRING.
    public static int writeString(byte[] buf, int pos, byte[] utf8) {
        if (utf8.length < 64) {
            int header = (utf8.length << VariantBinary.VALUE_HEADER_SHIFT) | VariantBinary.BASIC_TYPE_SHORT_STRING;
            pos = writeByte(buf, pos, header);
            System.arraycopy(utf8, 0, buf, pos, utf8.length);
            return pos + utf8.length;
        }
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_STRING));
        pos = writeLE(buf, pos, utf8.length, 4);
        System.arraycopy(utf8, 0, buf, pos, utf8.length);
        return pos + utf8.length;
    }

    public static int writeString(byte[] buf, int pos, String value) {
        return writeString(buf, pos, value.getBytes(StandardCharsets.UTF_8));
    }

    public static int writeBinary(byte[] buf, int pos, byte[] bytes) {
        pos = writeByte(buf, pos, primHeader(VariantBinary.PRIM_BINARY));
        pos = writeLE(buf, pos, bytes.length, 4);
        System.arraycopy(bytes, 0, buf, pos, bytes.length);
        return pos + bytes.length;
    }

    /// Copy an already-encoded Variant sub-value at `[src, src+len)` verbatim
    /// into `buf` at `pos`. Used when the `value` column already contains the
    /// canonical bytes for a sub-variant.
    public static int writeRaw(byte[] buf, int pos, byte[] src, int srcOffset, int len) {
        System.arraycopy(src, srcOffset, buf, pos, len);
        return pos + len;
    }

    // ==================== Array & object framing ====================

    /// Writes an ARRAY header (basic_type=3) plus num_elements + offset table,
    /// given the element byte ranges already materialized in `elementBytes`.
    /// Returns the new `pos`.
    public static int writeArray(byte[] buf, int pos, byte[][] elements) {
        int n = elements.length;
        int totalValueBytes = 0;
        for (byte[] e : elements) {
            totalValueBytes += e.length;
        }
        boolean isLarge = n > 0xFF;
        int offsetSize = minUnsignedWidth(totalValueBytes);
        int header = VariantBinary.BASIC_TYPE_ARRAY
                | (((offsetSize - 1) & VariantBinary.ARRAY_FIELD_OFFSET_SIZE_MASK) << VariantBinary.VALUE_HEADER_SHIFT)
                | ((isLarge ? 1 : 0) << (VariantBinary.VALUE_HEADER_SHIFT + 2));
        pos = writeByte(buf, pos, header);
        pos = writeLE(buf, pos, n, isLarge ? 4 : 1);
        int rel = 0;
        for (int i = 0; i < n; i++) {
            pos = writeLE(buf, pos, rel, offsetSize);
            rel += elements[i].length;
        }
        pos = writeLE(buf, pos, rel, offsetSize);
        for (byte[] e : elements) {
            System.arraycopy(e, 0, buf, pos, e.length);
            pos += e.length;
        }
        return pos;
    }

    /// Writes an OBJECT header + id array + offset table + values. Caller
    /// supplies the field-id array (ascending by name order per spec) and the
    /// matching pre-encoded value byte-arrays.
    public static int writeObject(byte[] buf, int pos, int[] fieldIds, byte[][] values, int maxFieldId) {
        int n = fieldIds.length;
        int totalValueBytes = 0;
        for (byte[] v : values) {
            totalValueBytes += v.length;
        }
        boolean isLarge = n > 0xFF;
        int idSize = minUnsignedWidth(Math.max(maxFieldId, 0));
        int offsetSize = minUnsignedWidth(totalValueBytes);
        int vh = (offsetSize - 1)
                | ((idSize - 1) << 2)
                | ((isLarge ? 1 : 0) << 4);
        int header = VariantBinary.BASIC_TYPE_OBJECT | (vh << VariantBinary.VALUE_HEADER_SHIFT);
        pos = writeByte(buf, pos, header);
        pos = writeLE(buf, pos, n, isLarge ? 4 : 1);
        for (int id : fieldIds) {
            pos = writeLE(buf, pos, id, idSize);
        }
        int rel = 0;
        for (int i = 0; i < n; i++) {
            pos = writeLE(buf, pos, rel, offsetSize);
            rel += values[i].length;
        }
        pos = writeLE(buf, pos, rel, offsetSize);
        for (byte[] v : values) {
            System.arraycopy(v, 0, buf, pos, v.length);
            pos += v.length;
        }
        return pos;
    }

    // ==================== Helpers ====================

    private static int primHeader(int primTag) {
        return (primTag << VariantBinary.VALUE_HEADER_SHIFT) | VariantBinary.BASIC_TYPE_PRIMITIVE;
    }

    private static int writeByte(byte[] buf, int pos, int b) {
        buf[pos] = (byte) b;
        return pos + 1;
    }

    private static int writeLE(byte[] buf, int pos, int value, int width) {
        for (int i = 0; i < width; i++) {
            buf[pos + i] = (byte) (value >>> (i * 8));
        }
        return pos + width;
    }

    private static int writeLongLE(byte[] buf, int pos, long value, int width) {
        for (int i = 0; i < width; i++) {
            buf[pos + i] = (byte) (value >>> (i * 8));
        }
        return pos + width;
    }

    /// Minimum byte width needed to hold an unsigned value 0..n (always >= 1).
    static int minUnsignedWidth(int n) {
        if (n <= 0xFF) {
            return 1;
        }
        if (n <= 0xFFFF) {
            return 2;
        }
        if (n <= 0xFFFFFF) {
            return 3;
        }
        return 4;
    }
}
