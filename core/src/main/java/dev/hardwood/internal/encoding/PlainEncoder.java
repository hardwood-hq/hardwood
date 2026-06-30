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
        ByteBuffer buffer = ByteBuffer.allocate(Math.multiplyExact(values.length, Integer.BYTES))
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.asIntBuffer().put(values);
        return buffer.array();
    }
}
