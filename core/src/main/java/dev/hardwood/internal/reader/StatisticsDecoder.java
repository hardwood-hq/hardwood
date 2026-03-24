/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/// Decodes raw statistics bytes from Parquet metadata to typed values.
///
/// Parquet statistics are stored as little-endian byte arrays. This utility
/// converts them to Java primitive types for comparison during predicate
/// push-down evaluation.
public class StatisticsDecoder {

    /// Decode a 4-byte little-endian value as an int.
    public static int decodeInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    /// Decode an 8-byte little-endian value as a long.
    public static long decodeLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /// Decode a 4-byte little-endian IEEE 754 value as a float.
    public static float decodeFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    /// Decode an 8-byte little-endian IEEE 754 value as a double.
    public static double decodeDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    /// Decode a single byte as a boolean (Parquet BOOLEAN type).
    /// A value of 0 means false, any other value means true.
    public static boolean decodeBoolean(byte[] bytes) {
        return bytes[0] != 0;
    }

    /// Compare two byte arrays lexicographically (unsigned).
    /// This matches Parquet's binary comparison semantics for BYTE_ARRAY statistics.
    ///
    /// @return negative if a < b, zero if equal, positive if a > b
    public static int compareBinary(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.length - b.length;
    }
}
