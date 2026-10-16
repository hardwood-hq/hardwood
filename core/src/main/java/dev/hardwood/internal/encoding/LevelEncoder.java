/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

/// Encodes definition (or repetition) levels for a DataPage V1 body, the inverse of the
/// level decoding in the reader's page decoder. Levels are always RLE/bit-packed hybrid
/// in V1, with a bit width derived from the maximum level.
///
/// The stream written is the bare hybrid stream; the caller frames a page by prefixing
/// the 4-byte little-endian length, exactly as the reader expects it.
public final class LevelEncoder {

    private LevelEncoder() {
    }

    /// Writes `count` levels starting at `offset` from a byte-backed store into `encoder`, at the
    /// bit width `maxLevel` fixes. A level is bounded by the schema's nesting depth, so the writer
    /// retains a row group's levels one unsigned byte per entry rather than one `int`.
    ///
    /// The encoder is reset first and is left finished-on-demand: the caller decides where the
    /// stream goes, which is what lets a page's level stream be produced straight into the page
    /// body it belongs to rather than into an array to be copied in.
    ///
    /// @param encoder the encoder to write through, reset to this stream's bit width
    /// @param levels the level values, each read unsigned
    /// @param offset index of the first level to encode
    /// @param count number of levels to encode
    /// @param maxLevel the column's maximum level, which fixes the bit width
    public static void writeInto(RleBitPackingHybridEncoder encoder, byte[] levels, int offset, int count,
                                 int maxLevel) {
        encoder.reset(bitWidth(maxLevel));
        encoder.writeUnsignedBytes(levels, offset, count);
    }

    /// The largest level a byte-backed level store can hold, and so the deepest nesting the
    /// writer accepts.
    public static final int MAX_STORABLE_LEVEL = 0xFF;

    /// Minimum number of bits needed to represent levels in `[0, maxLevel]`, matching the
    /// reader's `getBitWidth`.
    public static int bitWidth(int maxLevel) {
        if (maxLevel == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }
}
