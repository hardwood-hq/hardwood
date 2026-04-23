/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import dev.hardwood.row.VariantType;

/// Bit-layout constants and header-byte decoding for the Parquet Variant binary
/// encoding. All multi-byte integers in the encoding are unsigned little-endian
/// unless otherwise noted.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant Encoding</a>
public final class VariantBinary {

    private VariantBinary() {}

    // ==================== Metadata header byte ====================
    //   bit 0-3: version (must be 1)
    //   bit 4:   sorted_strings flag
    //   bit 5-6: offset_size_minus_one (0..3 → 1..4 bytes)
    //   bit 7:   unused

    public static final int METADATA_VERSION = 1;
    public static final int METADATA_VERSION_MASK = 0x0F;
    public static final int METADATA_SORTED_MASK = 0x10;
    public static final int METADATA_OFFSET_SIZE_SHIFT = 5;
    public static final int METADATA_OFFSET_SIZE_MASK = 0x03;

    // ==================== Value header byte — basic_type ====================
    //   bit 0-1: basic_type
    //   bit 2-7: value_header (type-specific)

    public static final int BASIC_TYPE_MASK = 0x03;
    public static final int VALUE_HEADER_SHIFT = 2;

    public static final int BASIC_TYPE_PRIMITIVE = 0;
    public static final int BASIC_TYPE_SHORT_STRING = 1;
    public static final int BASIC_TYPE_OBJECT = 2;
    public static final int BASIC_TYPE_ARRAY = 3;

    // ==================== Primitive type tags (value_header when basic_type=0) ====================

    public static final int PRIM_NULL = 0;
    public static final int PRIM_BOOLEAN_TRUE = 1;
    public static final int PRIM_BOOLEAN_FALSE = 2;
    public static final int PRIM_INT8 = 3;
    public static final int PRIM_INT16 = 4;
    public static final int PRIM_INT32 = 5;
    public static final int PRIM_INT64 = 6;
    public static final int PRIM_DOUBLE = 7;
    public static final int PRIM_DECIMAL4 = 8;
    public static final int PRIM_DECIMAL8 = 9;
    public static final int PRIM_DECIMAL16 = 10;
    public static final int PRIM_DATE = 11;
    public static final int PRIM_TIMESTAMP = 12;
    public static final int PRIM_TIMESTAMP_NTZ = 13;
    public static final int PRIM_FLOAT = 14;
    public static final int PRIM_BINARY = 15;
    public static final int PRIM_STRING = 16;
    public static final int PRIM_TIME_NTZ = 17;
    public static final int PRIM_TIMESTAMP_NANOS = 18;
    public static final int PRIM_TIMESTAMP_NTZ_NANOS = 19;
    public static final int PRIM_UUID = 20;

    // ==================== Object value_header bit layout ====================
    //   bit 0-1 of value_header: field_offset_size_minus_one
    //   bit 2-3 of value_header: field_id_size_minus_one
    //   bit 4   of value_header: is_large
    //   bit 5   of value_header: unused

    public static final int OBJECT_FIELD_OFFSET_SIZE_MASK = 0x03;
    public static final int OBJECT_FIELD_ID_SIZE_SHIFT = 2;
    public static final int OBJECT_FIELD_ID_SIZE_MASK = 0x03;
    public static final int OBJECT_IS_LARGE_MASK = 0x10;

    // ==================== Array value_header bit layout ====================
    //   bit 0-1 of value_header: field_offset_size_minus_one
    //   bit 2   of value_header: is_large
    //   bit 3-5 of value_header: unused

    public static final int ARRAY_FIELD_OFFSET_SIZE_MASK = 0x03;
    public static final int ARRAY_IS_LARGE_MASK = 0x04;

    /// Extract the basic_type (bits 0-1) from a value header byte.
    public static int basicType(byte header) {
        return header & BASIC_TYPE_MASK;
    }

    /// Extract the value_header sub-field (bits 2-7) from a value header byte.
    public static int valueHeader(byte header) {
        return (header & 0xFF) >>> VALUE_HEADER_SHIFT;
    }

    /// Read `width` bytes starting at `offset` as an unsigned little-endian integer.
    /// Width must be in `[1, 4]`.
    static int readUnsignedLE(byte[] buf, int offset, int width) {
        int result = 0;
        for (int i = 0; i < width; i++) {
            result |= (buf[offset + i] & 0xFF) << (8 * i);
        }
        return result;
    }

    /// Map a Variant value-header byte to its [VariantType]. Returns `null` for
    /// unrecognized primitive type tags so callers can fail-early with context.
    public static VariantType typeOf(byte header) {
        int basic = basicType(header);
        int info = valueHeader(header);
        return switch (basic) {
            case BASIC_TYPE_PRIMITIVE -> primitiveType(info);
            case BASIC_TYPE_SHORT_STRING -> VariantType.STRING;
            case BASIC_TYPE_OBJECT -> VariantType.OBJECT;
            case BASIC_TYPE_ARRAY -> VariantType.ARRAY;
            default -> null;
        };
    }

    private static VariantType primitiveType(int tag) {
        return switch (tag) {
            case PRIM_NULL -> VariantType.NULL;
            case PRIM_BOOLEAN_TRUE -> VariantType.BOOLEAN_TRUE;
            case PRIM_BOOLEAN_FALSE -> VariantType.BOOLEAN_FALSE;
            case PRIM_INT8 -> VariantType.INT8;
            case PRIM_INT16 -> VariantType.INT16;
            case PRIM_INT32 -> VariantType.INT32;
            case PRIM_INT64 -> VariantType.INT64;
            case PRIM_DOUBLE -> VariantType.DOUBLE;
            case PRIM_DECIMAL4 -> VariantType.DECIMAL4;
            case PRIM_DECIMAL8 -> VariantType.DECIMAL8;
            case PRIM_DECIMAL16 -> VariantType.DECIMAL16;
            case PRIM_DATE -> VariantType.DATE;
            case PRIM_TIMESTAMP -> VariantType.TIMESTAMP;
            case PRIM_TIMESTAMP_NTZ -> VariantType.TIMESTAMP_NTZ;
            case PRIM_FLOAT -> VariantType.FLOAT;
            case PRIM_BINARY -> VariantType.BINARY;
            case PRIM_STRING -> VariantType.STRING;
            case PRIM_TIME_NTZ -> VariantType.TIME_NTZ;
            case PRIM_TIMESTAMP_NANOS -> VariantType.TIMESTAMP_NANOS;
            case PRIM_TIMESTAMP_NTZ_NANOS -> VariantType.TIMESTAMP_NTZ_NANOS;
            case PRIM_UUID -> VariantType.UUID;
            default -> null;
        };
    }
}
