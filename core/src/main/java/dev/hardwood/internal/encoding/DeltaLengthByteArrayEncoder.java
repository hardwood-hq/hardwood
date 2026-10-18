/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Encoder for DELTA_LENGTH_BYTE_ARRAY, the layout `BinaryPageValues.deltaLength` reads in place.
///
/// The lengths are delta-encoded ahead of the values, then the values follow with nothing
/// between them:
///
/// ```text
/// <lengths, DELTA_BINARY_PACKED> <concatenated bytes>
/// ```
///
/// Against `PLAIN`, which prefixes every value with a four-byte length, this pays off wherever
/// the lengths are similar — a column of fixed-shape identifiers or codes spends a bit or two
/// per value on the length instead of 32 bits.
public final class DeltaLengthByteArrayEncoder {

    private DeltaLengthByteArrayEncoder() {
    }

    /// Encodes `count` byte arrays held end to end in `data`.
    ///
    /// The values are addressed the way [dev.hardwood.internal.writer.BinaryValueEncoder] stores
    /// them — one packed buffer, value `i` occupying `data[offsets[i], offsets[i + 1])` — so a
    /// page's range encodes without copying its values out first. Their bytes being already
    /// contiguous, the value section is one copy.
    ///
    /// @param data the buffer holding every value's bytes
    /// @param offsets value starts, one longer than the value count
    /// @param from index of the first value to encode
    /// @param count how many values to encode
    /// @return the encoded bytes
    public static byte[] encode(byte[] data, int[] offsets, int from, int count) {
        int[] lengths = new int[count];
        for (int i = 0; i < count; i++) {
            lengths[i] = offsets[from + i + 1] - offsets[from + i];
        }
        int valueBytes = offsets[from + count] - offsets[from];

        byte[] encodedLengths = DeltaBinaryPackedEncoder.encodeInts(lengths, 0, count);
        byte[] out = new byte[Math.addExact(encodedLengths.length, valueBytes)];
        System.arraycopy(encodedLengths, 0, out, 0, encodedLengths.length);
        System.arraycopy(data, offsets[from], out, encodedLengths.length, valueBytes);
        return out;
    }
}
