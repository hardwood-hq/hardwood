/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// The ordering used for a leaf column's `min`/`max` statistics, decoded from the
/// `FileMetaData.column_orders` union.
///
/// When a file omits `column_orders`, the type-defined ordering applies implicitly to every
/// column and [FileMetaData#columnOrders] is empty.
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public enum ColumnOrder {
    /// Ordering defined by the column's physical or logical type (the `TYPE_ORDER` union member).
    /// For `FLOAT` and `DOUBLE` this is a signed comparison of the represented value, with the
    /// documented NaN and ±0 compatibility rules applied when reading statistics.
    TYPE_DEFINED_ORDER,
    /// The IEEE 754 total order (the `IEEE_754_TOTAL_ORDER` union member).
    IEEE754_TOTAL_ORDER,
    /// A `ColumnOrder` union member that this version of Hardwood does not recognize, e.g. an
    /// ordering added to the Parquet format after this release.
    UNKNOWN
}
