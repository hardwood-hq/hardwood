/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.row.VariantType;
import dev.hardwood.row.VariantTypeException;

/// Stateless decoder for the Variant value buffer. Parses a value's header byte
/// and extracts the concrete primitive/short-string payload, or decodes OBJECT
/// and ARRAY header layouts into the record types [ObjectLayout] and
/// [ArrayLayout] for navigation.
public final class VariantValueDecoder {

    private VariantValueDecoder() {}

    // ==================== Type introspection ====================

    /// Returns the [VariantType] of the value starting at `buf[offset]`.
    ///
    /// @throws IllegalArgumentException if the header is malformed or the primitive
    ///     type tag is unrecognized
    public static VariantType type(byte[] buf, int offset) {
        checkBounds(buf, offset, 1);
        VariantType type = VariantBinary.typeOf(buf[offset]);
        if (type == null) {
            throw new IllegalArgumentException("Unrecognized Variant type tag at offset " + offset);
        }
        return type;
    }

    // ==================== Primitive extraction ====================

    public static boolean asBoolean(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t == VariantType.BOOLEAN_TRUE) {
            return true;
        }
        if (t == VariantType.BOOLEAN_FALSE) {
            return false;
        }
        throw VariantTypeException.expectedOneOf("BOOLEAN_TRUE/BOOLEAN_FALSE", t);
    }

    public static int asInt(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        return switch (t) {
            case INT8 -> buf[offset + 1];
            case INT16 -> readIntLE(buf, offset + 1, 2);
            case INT32 -> readIntLE(buf, offset + 1, 4);
            default -> throw VariantTypeException.expectedOneOf("INT8/INT16/INT32", t);
        };
    }

    public static long asLong(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        return switch (t) {
            case INT8 -> buf[offset + 1];
            case INT16 -> readIntLE(buf, offset + 1, 2);
            case INT32 -> readIntLE(buf, offset + 1, 4);
            case INT64 -> readLongLE(buf, offset + 1, 8);
            default -> throw VariantTypeException.expectedOneOf("INT8/INT16/INT32/INT64", t);
        };
    }

    public static float asFloat(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t != VariantType.FLOAT) {
            throw VariantTypeException.expected(VariantType.FLOAT, t);
        }
        return Float.intBitsToFloat(readIntLE(buf, offset + 1, 4));
    }

    public static double asDouble(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t != VariantType.DOUBLE) {
            throw VariantTypeException.expected(VariantType.DOUBLE, t);
        }
        return Double.longBitsToDouble(readLongLE(buf, offset + 1, 8));
    }

    public static String asString(byte[] buf, int offset) {
        int basic = VariantBinary.basicType(buf[offset]);
        if (basic == VariantBinary.BASIC_TYPE_SHORT_STRING) {
            int length = VariantBinary.valueHeader(buf[offset]);
            checkBounds(buf, offset + 1, length);
            return new String(buf, offset + 1, length, StandardCharsets.UTF_8);
        }
        if (basic == VariantBinary.BASIC_TYPE_PRIMITIVE) {
            int tag = VariantBinary.valueHeader(buf[offset]);
            if (tag == VariantBinary.PRIM_STRING) {
                int length = readIntLE(buf, offset + 1, 4);
                checkBounds(buf, offset + 5, length);
                return new String(buf, offset + 5, length, StandardCharsets.UTF_8);
            }
        }
        throw VariantTypeException.expected(VariantType.STRING, type(buf, offset));
    }

    public static byte[] asBinary(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t != VariantType.BINARY) {
            throw VariantTypeException.expected(VariantType.BINARY, t);
        }
        int length = readIntLE(buf, offset + 1, 4);
        checkBounds(buf, offset + 5, length);
        byte[] out = new byte[length];
        System.arraycopy(buf, offset + 5, out, 0, length);
        return out;
    }

    public static BigDecimal asDecimal(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        return switch (t) {
            case DECIMAL4 -> {
                int scale = buf[offset + 1] & 0xFF;
                int unscaled = readIntLE(buf, offset + 2, 4);
                yield BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL8 -> {
                int scale = buf[offset + 1] & 0xFF;
                long unscaled = readLongLE(buf, offset + 2, 8);
                yield BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL16 -> {
                int scale = buf[offset + 1] & 0xFF;
                // 16 bytes little-endian, signed two's complement — convert to big-endian for BigInteger
                byte[] be = new byte[16];
                for (int i = 0; i < 16; i++) {
                    be[i] = buf[offset + 2 + (15 - i)];
                }
                yield new BigDecimal(new BigInteger(be), scale);
            }
            default -> throw VariantTypeException.expectedOneOf("DECIMAL4/DECIMAL8/DECIMAL16", t);
        };
    }

    public static LocalDate asDate(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t != VariantType.DATE) {
            throw VariantTypeException.expected(VariantType.DATE, t);
        }
        return LocalDate.ofEpochDay(readIntLE(buf, offset + 1, 4));
    }

    public static Instant asTimestamp(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        return switch (t) {
            case TIMESTAMP, TIMESTAMP_NTZ -> {
                long micros = readLongLE(buf, offset + 1, 8);
                long secs = Math.floorDiv(micros, 1_000_000L);
                long nanoOfSec = Math.floorMod(micros, 1_000_000L) * 1_000L;
                yield Instant.ofEpochSecond(secs, nanoOfSec);
            }
            case TIMESTAMP_NANOS, TIMESTAMP_NTZ_NANOS -> {
                long nanos = readLongLE(buf, offset + 1, 8);
                long secs = Math.floorDiv(nanos, 1_000_000_000L);
                long nanoOfSec = Math.floorMod(nanos, 1_000_000_000L);
                yield Instant.ofEpochSecond(secs, nanoOfSec);
            }
            default -> throw VariantTypeException.expectedOneOf(
                    "TIMESTAMP/TIMESTAMP_NTZ/TIMESTAMP_NANOS/TIMESTAMP_NTZ_NANOS", t);
        };
    }

    public static LocalTime asTime(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t != VariantType.TIME_NTZ) {
            throw VariantTypeException.expected(VariantType.TIME_NTZ, t);
        }
        long micros = readLongLE(buf, offset + 1, 8);
        return LocalTime.ofNanoOfDay(Math.multiplyExact(micros, 1_000L));
    }

    public static UUID asUuid(byte[] buf, int offset) {
        VariantType t = type(buf, offset);
        if (t != VariantType.UUID) {
            throw VariantTypeException.expected(VariantType.UUID, t);
        }
        long msb = readLongBE(buf, offset + 1);
        long lsb = readLongBE(buf, offset + 9);
        return new UUID(msb, lsb);
    }

    // ==================== OBJECT / ARRAY layout parsing ====================

    /// Pre-parsed header of an OBJECT value, used to avoid re-decoding the header
    /// byte on every field lookup. All `*Start` fields are absolute offsets into
    /// the value buffer.
    ///
    /// @param numElements number of fields in the object
    /// @param idSize bytes per field id
    /// @param offsetSize bytes per field offset
    /// @param idsStart absolute offset of the field_id array
    /// @param offsetsStart absolute offset of the field_offset array
    /// @param valuesStart absolute offset of the concatenated values section
    public record ObjectLayout(int numElements, int idSize, int offsetSize,
                               int idsStart, int offsetsStart, int valuesStart) {}

    /// Pre-parsed header of an ARRAY value.
    ///
    /// @param numElements number of elements in the array
    /// @param offsetSize bytes per element offset
    /// @param offsetsStart absolute offset of the offset array
    /// @param valuesStart absolute offset of the concatenated values section
    public record ArrayLayout(int numElements, int offsetSize,
                              int offsetsStart, int valuesStart) {}

    public static ObjectLayout parseObject(byte[] buf, int offset) {
        int header = buf[offset] & 0xFF;
        int basic = header & VariantBinary.BASIC_TYPE_MASK;
        if (basic != VariantBinary.BASIC_TYPE_OBJECT) {
            throw VariantTypeException.expected(VariantType.OBJECT, VariantBinary.typeOf(buf[offset]));
        }
        int valueHeader = header >>> VariantBinary.VALUE_HEADER_SHIFT;
        int offsetSize = (valueHeader & VariantBinary.OBJECT_FIELD_OFFSET_SIZE_MASK) + 1;
        int idSize = ((valueHeader >>> VariantBinary.OBJECT_FIELD_ID_SIZE_SHIFT) & VariantBinary.OBJECT_FIELD_ID_SIZE_MASK) + 1;
        boolean isLarge = (valueHeader & VariantBinary.OBJECT_IS_LARGE_MASK) != 0;
        int numSize = isLarge ? 4 : 1;
        int pos = offset + 1;
        int numElements = VariantBinary.readUnsignedLE(buf, pos, numSize);
        pos += numSize;
        int idsStart = pos;
        pos += numElements * idSize;
        int offsetsStart = pos;
        pos += (numElements + 1) * offsetSize;
        int valuesStart = pos;
        return new ObjectLayout(numElements, idSize, offsetSize, idsStart, offsetsStart, valuesStart);
    }

    public static ArrayLayout parseArray(byte[] buf, int offset) {
        int header = buf[offset] & 0xFF;
        int basic = header & VariantBinary.BASIC_TYPE_MASK;
        if (basic != VariantBinary.BASIC_TYPE_ARRAY) {
            throw VariantTypeException.expected(VariantType.ARRAY, VariantBinary.typeOf(buf[offset]));
        }
        int valueHeader = header >>> VariantBinary.VALUE_HEADER_SHIFT;
        int offsetSize = (valueHeader & VariantBinary.ARRAY_FIELD_OFFSET_SIZE_MASK) + 1;
        boolean isLarge = (valueHeader & VariantBinary.ARRAY_IS_LARGE_MASK) != 0;
        int numSize = isLarge ? 4 : 1;
        int pos = offset + 1;
        int numElements = VariantBinary.readUnsignedLE(buf, pos, numSize);
        pos += numSize;
        int offsetsStart = pos;
        pos += (numElements + 1) * offsetSize;
        int valuesStart = pos;
        return new ArrayLayout(numElements, offsetSize, offsetsStart, valuesStart);
    }

    /// Reads the field id stored at index `i` in an object's id array.
    public static int objectFieldId(byte[] buf, ObjectLayout layout, int i) {
        return VariantBinary.readUnsignedLE(buf, layout.idsStart() + i * layout.idSize(), layout.idSize());
    }

    /// Absolute buffer offset of the i-th field's value in an object.
    public static int objectValueOffset(byte[] buf, ObjectLayout layout, int i) {
        int rel = VariantBinary.readUnsignedLE(buf, layout.offsetsStart() + i * layout.offsetSize(), layout.offsetSize());
        return layout.valuesStart() + rel;
    }

    /// Absolute buffer offset of the i-th element's value in an array.
    public static int arrayElementOffset(byte[] buf, ArrayLayout layout, int i) {
        int rel = VariantBinary.readUnsignedLE(buf, layout.offsetsStart() + i * layout.offsetSize(), layout.offsetSize());
        return layout.valuesStart() + rel;
    }

    // ==================== Helpers ====================

    private static int readIntLE(byte[] buf, int offset, int width) {
        int result = 0;
        for (int i = 0; i < width; i++) {
            result |= (buf[offset + i] & 0xFF) << (8 * i);
        }
        // Sign-extend for widths < 4
        int shift = 32 - width * 8;
        return (result << shift) >> shift;
    }

    private static long readLongLE(byte[] buf, int offset, int width) {
        long result = 0L;
        for (int i = 0; i < width; i++) {
            result |= ((long) (buf[offset + i] & 0xFF)) << (8 * i);
        }
        return result;
    }

    private static long readLongBE(byte[] buf, int offset) {
        long result = 0L;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (buf[offset + i] & 0xFFL);
        }
        return result;
    }

    private static void checkBounds(byte[] buf, int offset, int needed) {
        if (offset < 0 || offset + needed > buf.length) {
            throw new IllegalArgumentException(
                    "Variant value buffer truncated: need " + needed + " bytes at offset " + offset + ", buffer length " + buf.length);
        }
    }
}
