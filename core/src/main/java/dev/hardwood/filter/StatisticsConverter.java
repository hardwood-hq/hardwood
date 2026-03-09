/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.filter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.metadata.PhysicalType;

/**
 * Converts raw Parquet statistics bytes to comparable {@code long} values.
 *
 * <p>Statistics in Parquet are stored as raw bytes in the column's physical type encoding.
 * This converter interprets those bytes into longs suitable for numeric comparisons.
 * For BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY types, lexicographic byte comparison is used instead.</p>
 */
public final class StatisticsConverter {

    private StatisticsConverter() {
    }

    /**
     * Interprets raw statistics bytes as a {@code long} for a given physical type.
     */
    public static long bytesToLong(byte[] bytes, PhysicalType physicalType) {
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return switch (physicalType) {
            case BOOLEAN -> (bytes[0] != 0) ? 1L : 0L;
            case INT32 -> bb.getInt();
            case INT64 -> bb.getLong();
            case FLOAT -> Float.floatToRawIntBits(bb.getFloat());
            case DOUBLE -> Double.doubleToRawLongBits(bb.getDouble());
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                    throw new UnsupportedOperationException(
                            "Cannot convert " + physicalType + " statistics to long; use byte comparison");
        };
    }

    /**
     * Interprets raw statistics bytes as a {@code double} for floating-point column filtering.
     */
    public static double bytesToDouble(byte[] bytes, PhysicalType physicalType) {
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return switch (physicalType) {
            case INT32 -> bb.getInt();
            case INT64 -> bb.getLong();
            case FLOAT -> bb.getFloat();
            case DOUBLE -> bb.getDouble();
            default -> throw new UnsupportedOperationException(
                    "Cannot convert " + physicalType + " statistics to double");
        };
    }

    /**
     * Compares two byte arrays lexicographically (unsigned), as used for BYTE_ARRAY statistics.
     *
     * @return negative if a < b, zero if equal, positive if a > b
     */
    public static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.length - b.length;
    }
}
