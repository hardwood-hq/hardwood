/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.writer.ColumnEncoding;

/// Which physical types each encoding policy may be applied to.
///
/// An encoding is defined over some physical types and not others — a delta of two binary
/// values means nothing, and there is no ordering to take differences of over a `BOOLEAN` — so
/// a policy naming a column whose type cannot carry it is rejected when the writer is created.
/// [ColumnEncoding#supports] is the enum's own view of this table; the table lives here so that
/// the writer's validation and anything enumerating the writer's capabilities read the same
/// one.
public final class EncodingSupport {

    private EncodingSupport() {
    }

    /// Whether a column of `type` can be dictionary-encoded at all.
    ///
    /// `BOOLEAN` cannot: `PLAIN` already stores a boolean in one bit, which no dictionary page
    /// plus index stream can beat, so the writer builds none and
    /// [ColumnEncoding#AUTO] resolves such a chunk to `PLAIN` whatever its values are. Every
    /// other writable type can, leaving the choice to the size comparison.
    ///
    /// @param type the column's physical type
    /// @return `true` when a dictionary is possible for the type
    public static boolean dictionaryCapable(PhysicalType type) {
        return type != PhysicalType.BOOLEAN;
    }

    /// Whether a column of `type` may be written under `encoding`.
    ///
    /// @param encoding the encoding policy
    /// @param type the column's physical type
    /// @return `true` when the combination is legal
    public static boolean supports(ColumnEncoding encoding, PhysicalType type) {
        return switch (encoding) {
            case AUTO, PLAIN -> true;
            case DELTA_BINARY_PACKED -> type == PhysicalType.INT32 || type == PhysicalType.INT64;
            case DELTA_LENGTH_BYTE_ARRAY -> type == PhysicalType.BYTE_ARRAY;
            case DELTA_BYTE_ARRAY -> type == PhysicalType.BYTE_ARRAY
                    || type == PhysicalType.FIXED_LEN_BYTE_ARRAY;
            case BYTE_STREAM_SPLIT -> type == PhysicalType.INT32 || type == PhysicalType.INT64
                    || type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE
                    || type == PhysicalType.FIXED_LEN_BYTE_ARRAY;
            case ALP -> type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE;
        };
    }
}
