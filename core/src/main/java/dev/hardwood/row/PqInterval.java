/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Value type for the Parquet `INTERVAL` logical type.
///
/// The three components are stored independently — they are **not** normalized
/// (e.g. 90 days is not converted to ~3 months) because the calendar semantics
/// of months and days vary. The on-disk encoding is a 12-byte
/// FIXED_LEN_BYTE_ARRAY: three little-endian **unsigned** 32-bit integers in
/// the order `months`, `days`, `milliseconds`.
///
/// Each component is exposed as a `long` holding its unsigned 32-bit value,
/// so all values are in the range `[0, 4_294_967_295]`.
///
/// @param months       number of months (unsigned 32-bit, range 0–4,294,967,295)
/// @param days         number of days (unsigned 32-bit, range 0–4,294,967,295)
/// @param milliseconds number of milliseconds (unsigned 32-bit, range 0–4,294,967,295)
public record PqInterval(long months, long days, long milliseconds) {

    /// Creates an interval, such as a value for `setInterval` or a filter literal for an
    /// `INTERVAL` column.
    ///
    /// The components are taken as given. A component outside `[0, 4_294_967_295]` cannot be
    /// stored: the writer refuses it, and as a filter literal it equals no stored value.
    ///
    /// @param months       number of months
    /// @param days         number of days
    /// @param milliseconds number of milliseconds
    /// @return the interval
    public static PqInterval of(long months, long days, long milliseconds) {
        return new PqInterval(months, days, milliseconds);
    }
}
