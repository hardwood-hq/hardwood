/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

/// How a column's batch values reach the Arrow buffers of its vector. Resolved once per
/// output column while the schema is converted, so loading a batch never re-derives it
/// from the Arrow type.
///
/// A kind names the fill, not the Arrow type: a `DATE` column and a bare `INT32` column are
/// both [#INT32], a bulk copy of the reader's `int[]`, and differ only in the
/// [org.apache.arrow.vector.types.pojo.Field] the plan pairs them with.
public enum ScalarKind {
    /// `boolean[]` packed into bits.
    BOOLEAN,
    /// `int[]` copied as four-byte values: `INT32`, `INT(32, *)`, `DATE`, `TIME(MILLIS)`.
    INT32,
    /// `int[]` narrowed to signed bytes, each value checked against the annotation's range.
    INT8,
    /// `int[]` narrowed to signed shorts, each value checked against the annotation's range.
    INT16,
    /// `int[]` narrowed to unsigned bytes, each value checked against the annotation's range.
    UINT8,
    /// `int[]` narrowed to unsigned shorts, each value checked against the annotation's range.
    UINT16,
    /// `long[]` copied as eight-byte values: `INT64`, `INT(64, *)`, `TIME(MICROS|NANOS)`,
    /// `TIMESTAMP`.
    INT64,
    /// `float[]` copied as four-byte values.
    FLOAT,
    /// `double[]` copied as eight-byte values.
    DOUBLE,
    /// `int[]` holding a `DECIMAL`'s unscaled value, sign-extended to a 16-byte decimal.
    DECIMAL_INT32,
    /// `long[]` holding a `DECIMAL`'s unscaled value, sign-extended to a 16-byte decimal.
    DECIMAL_INT64,
    /// Big-endian two's-complement `DECIMAL` payloads, re-laid as little-endian 16- or 32-byte
    /// decimals.
    DECIMAL_BYTES,
    /// Twelve-byte legacy `INT96` timestamps converted to nanoseconds since the epoch.
    INT96_TIMESTAMP,
    /// Variable-length values: an offsets buffer and the bytes in value order.
    BINARY,
    /// Fixed-width values gathered by their views into a buffer of `width` bytes per value,
    /// which also serves `FLOAT16`, whose two little-endian bytes are Arrow's half-float layout.
    FIXED_BINARY,
    /// The `NULL` logical type: a length and no buffers.
    NULL
}
