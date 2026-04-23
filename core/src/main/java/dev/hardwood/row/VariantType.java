/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Logical type of a Variant value, as carried in the Variant binary encoding's
/// value header. Distinct from [dev.hardwood.metadata.PhysicalType] — Variant has
/// its own tagged type space that lives inside the `value` byte buffer.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet Variant Encoding</a>
public enum VariantType {

    /// SQL NULL.
    NULL,
    /// Boolean `true`.
    BOOLEAN_TRUE,
    /// Boolean `false`.
    BOOLEAN_FALSE,
    /// 8-bit signed integer.
    INT8,
    /// 16-bit signed integer.
    INT16,
    /// 32-bit signed integer.
    INT32,
    /// 64-bit signed integer.
    INT64,
    /// IEEE 64-bit floating point.
    DOUBLE,
    /// Decimal with up to 9 digits (stored as 32-bit unscaled + scale).
    DECIMAL4,
    /// Decimal with up to 18 digits (stored as 64-bit unscaled + scale).
    DECIMAL8,
    /// Decimal with up to 38 digits (stored as 128-bit unscaled + scale).
    DECIMAL16,
    /// Date (days since Unix epoch).
    DATE,
    /// Timestamp with microsecond precision, UTC-adjusted.
    TIMESTAMP,
    /// Timestamp with microsecond precision, no timezone.
    TIMESTAMP_NTZ,
    /// IEEE 32-bit floating point.
    FLOAT,
    /// Opaque byte sequence.
    BINARY,
    /// UTF-8 string.
    STRING,
    /// Time of day with microsecond precision, no timezone.
    TIME_NTZ,
    /// Timestamp with nanosecond precision, UTC-adjusted.
    TIMESTAMP_NANOS,
    /// Timestamp with nanosecond precision, no timezone.
    TIMESTAMP_NTZ_NANOS,
    /// UUID stored as 16 bytes (big-endian).
    UUID,
    /// Object — string-keyed collection of Variant values.
    OBJECT,
    /// Array — ordered collection of Variant values.
    ARRAY
}
