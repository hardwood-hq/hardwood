/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import dev.hardwood.metadata.PhysicalType;

/// The byte widths parquet-format fixes for a value: those of the physical types whose values
/// all take the same number of bytes, and those of the `FIXED_LEN_BYTE_ARRAY` annotations, which
/// `AnnotationPairings` checks a column's declared width against. Every encoder, decoder, check
/// and rendering of one of these values reads its width from here.
public final class FixedWidths {

    /// "`UUID` annotates a 16-byte `FIXED_LEN_BYTE_ARRAY` primitive type."
    public static final int UUID = 16;

    /// `INTERVAL` "must annotate a `fixed_len_byte_array` of length 12": months, days and
    /// milliseconds, each a little-endian unsigned 4-byte integer.
    public static final int INTERVAL = 12;

    /// `FLOAT16`: "The primitive type is a 2-byte `FIXED_LEN_BYTE_ARRAY`."
    public static final int FLOAT16 = 2;

    /// The `FIXED_LEN_BYTE_ARRAY` carrier of a `TIMESTAMP`, "with `type_length = 12`": a signed
    /// 96-bit little-endian count of the unit since the epoch.
    public static final int FLBA12_TIMESTAMP = 12;

    /// The legacy `INT96` timestamp: eight little-endian bytes of nanoseconds of the day, then
    /// four of the Julian day.
    public static final int INT96 = 12;

    private FixedWidths() {
    }

    /// The bytes one plain-encoded value of `type` occupies: 4 for an `INT32` or `FLOAT`, 8 for an
    /// `INT64` or `DOUBLE`, and [#INT96] for an `INT96`.
    ///
    /// @throws IllegalArgumentException for a type whose values have no width of their own: a
    ///         `BOOLEAN`, stored as one bit, a `BYTE_ARRAY`, whose values vary in length, and a
    ///         `FIXED_LEN_BYTE_ARRAY`, whose width each column declares
    public static int of(PhysicalType type) {
        return switch (type) {
            case INT32, FLOAT -> Integer.BYTES;
            case INT64, DOUBLE -> Long.BYTES;
            case INT96 -> INT96;
            case BOOLEAN, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                    throw new IllegalArgumentException(type + " has no fixed byte width");
        };
    }
}
