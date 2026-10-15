/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import dev.hardwood.internal.writer.EncodingSupport;
import dev.hardwood.metadata.PhysicalType;

/// How a column's values are encoded, set file-wide or per leaf column through
/// [WriterConfig.Builder#encoding(ColumnEncoding)].
///
/// [#AUTO] leaves the choice to the writer, which measures it per column chunk. Every other
/// member names an encoding outright, and a column that names one builds no dictionary: its
/// chunks carry that encoding in every row group of the file.
///
/// An encoding is legal only for some physical types, and a policy naming a column whose type
/// cannot carry it fails when the writer is created rather than being quietly replaced:
///
/// | Policy | Physical types |
/// |---|---|
/// | `AUTO` | every writable type |
/// | `PLAIN` | every writable type |
/// | `DELTA_BINARY_PACKED` | `INT32`, `INT64` |
/// | `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` |
/// | `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` |
/// | `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `FIXED_LEN_BYTE_ARRAY` |
/// | `ALP` | `FLOAT`, `DOUBLE` |
///
/// No policy demands a dictionary: dictionary encoding is an outcome [#AUTO] may arrive at,
/// not something to ask for.
public enum ColumnEncoding {

    /// Let the writer choose, per column chunk, between a dictionary and `PLAIN`.
    ///
    /// The chunk is dictionary-encoded where the dictionary page plus an index stream is smaller
    /// than the values written `PLAIN`, and `PLAIN` otherwise. The comparison is made once the
    /// row group is buffered, from the values the chunk actually holds, so one column may be
    /// dictionary-encoded in one row group and `PLAIN` in the next.
    AUTO,

    /// Values as they are: fixed-width types little-endian, `BYTE_ARRAY` behind a 4-byte length
    /// prefix. Always available, never the smallest for data with structure to exploit.
    PLAIN,

    /// Differences between neighbouring values, bit-packed against the smallest difference in
    /// each block. Suits sorted or slow-moving integers — timestamps, identifiers, counters —
    /// and is larger than `PLAIN` on unordered data.
    DELTA_BINARY_PACKED,

    /// The lengths delta-encoded ahead of the values, which is cheaper than `PLAIN`'s
    /// per-value 4-byte length prefix wherever the lengths are similar.
    DELTA_LENGTH_BYTE_ARRAY,

    /// Each value carrying only what it does not share with the value before it. Suits sorted
    /// values with common starts — paths, URLs, keys — which are what a dictionary serves worst.
    DELTA_BYTE_ARRAY,

    /// A value's bytes scattered across one stream per byte position. This changes no page's
    /// size on its own; what it changes is how well the codec afterwards compresses floating
    /// point data, so it is worth choosing only alongside compression.
    BYTE_STREAM_SPLIT,

    /// Values scaled to integers by a power of ten, stored as their distance above each block's
    /// smallest and bit-packed. Suits values with few decimal digits, such as prices,
    /// measurements and coordinates; a value with more significant digits than its block keeps
    /// is stored as it is, at more than its `PLAIN` width. `AUTO` never chooses it: it is in
    /// Preview in the format specification, and only readers that implement it can read the
    /// column.
    ALP;

    /// Whether a column of `type` may be written with this policy.
    ///
    /// @param type the column's physical type
    /// @return `true` when the combination is legal
    boolean supports(PhysicalType type) {
        return EncodingSupport.supports(this, type);
    }
}
