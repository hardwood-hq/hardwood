/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.util.Arrays;

/// Encoder for DELTA_BYTE_ARRAY, the inverse of [DeltaByteArrayDecoder].
///
/// Each value states how much of its start it shares with the value before it, and carries only
/// the rest:
///
/// ```text
/// <prefix lengths, DELTA_BINARY_PACKED> <suffixes, DELTA_LENGTH_BYTE_ARRAY>
/// ```
///
/// This is front compression, and it targets what a dictionary handles worst: sorted values that
/// are nearly all distinct and nearly all share a start — paths, URLs, keys.
///
/// A page begins afresh. Its first value has a prefix length of zero whatever preceded it in the
/// chunk, because a reader may seek to any page and must decode it without the one before.
public final class DeltaByteArrayEncoder {

    private DeltaByteArrayEncoder() {
    }

    /// Encodes `count` byte arrays held end to end in `data`, value `i` occupying
    /// `data[offsets[i], offsets[i + 1])` — the layout
    /// [dev.hardwood.internal.writer.BinaryValueEncoder] stores a chunk's values in.
    ///
    /// @param data the buffer holding every value's bytes
    /// @param offsets value starts, one longer than the value count
    /// @param from index of the first value to encode
    /// @param count how many values to encode
    /// @return the encoded bytes
    public static byte[] encode(byte[] data, int[] offsets, int from, int count) {
        int[] prefixLengths = new int[count];
        int[] suffixLengths = new int[count];
        int[] suffixOffsets = new int[count];
        long suffixBytes = 0;

        for (int i = 0; i < count; i++) {
            int offset = offsets[from + i];
            int length = offsets[from + i + 1] - offset;
            // The first value of the range shares nothing, whatever precedes it in the chunk:
            // a page must decode without the page before it.
            int prefix = i == 0 ? 0
                    : sharedPrefix(data, offsets[from + i - 1], offsets[from + i] - offsets[from + i - 1],
                            offset, length);
            prefixLengths[i] = prefix;
            suffixOffsets[i] = offset + prefix;
            suffixLengths[i] = length - prefix;
            suffixBytes += suffixLengths[i];
        }

        byte[] encodedPrefixes = DeltaBinaryPackedEncoder.encodeInts(prefixLengths, 0, count);
        byte[] encodedSuffixLengths = DeltaBinaryPackedEncoder.encodeInts(suffixLengths, 0, count);
        byte[] out = new byte[Math.toIntExact(
                encodedPrefixes.length + encodedSuffixLengths.length + suffixBytes)];

        int pos = 0;
        System.arraycopy(encodedPrefixes, 0, out, pos, encodedPrefixes.length);
        pos += encodedPrefixes.length;
        System.arraycopy(encodedSuffixLengths, 0, out, pos, encodedSuffixLengths.length);
        pos += encodedSuffixLengths.length;
        for (int i = 0; i < count; i++) {
            System.arraycopy(data, suffixOffsets[i], out, pos, suffixLengths[i]);
            pos += suffixLengths[i];
        }
        return out;
    }

    /// How many leading bytes two values share, capped at the shorter of them.
    private static int sharedPrefix(byte[] data, int previousOffset, int previousLength,
            int offset, int length) {
        int limit = Math.min(previousLength, length);
        int mismatch = Arrays.mismatch(data, previousOffset, previousOffset + limit, data, offset, offset + limit);
        return mismatch < 0 ? limit : mismatch;
    }
}
