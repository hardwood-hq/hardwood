/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Legacy converted types in Parquet schema (used by PyArrow for LIST/MAP annotation).
///
/// @see <a href="https://parquet.apache.org/docs/file-format/types/logicaltypes/">File Format – Logical Types</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift – ConvertedType</a>
public enum ConvertedType {
    /// UTF-8 encoded character string.
    UTF8,
    /// Map of key-value pairs.
    MAP,
    /// Intermediate group inside a MAP; contains key and value fields.
    MAP_KEY_VALUE,
    /// List of elements.
    LIST,
    /// Enum stored as a UTF-8 string.
    ENUM,
    /// Decimal with scale and precision stored in the schema element.
    DECIMAL,
    /// Calendar date (days since Unix epoch), stored as INT32.
    DATE,
    /// Time of day in milliseconds, stored as INT32.
    TIME_MILLIS,
    /// Time of day in microseconds, stored as INT64.
    TIME_MICROS,
    /// Timestamp in milliseconds since Unix epoch, stored as INT64.
    TIMESTAMP_MILLIS,
    /// Timestamp in microseconds since Unix epoch, stored as INT64.
    TIMESTAMP_MICROS,
    /// Unsigned 8-bit integer, stored as INT32.
    UINT_8,
    /// Unsigned 16-bit integer, stored as INT32.
    UINT_16,
    /// Unsigned 32-bit integer, stored as INT32.
    UINT_32,
    /// Unsigned 64-bit integer, stored as INT64.
    UINT_64,
    /// Signed 8-bit integer, stored as INT32.
    INT_8,
    /// Signed 16-bit integer, stored as INT32.
    INT_16,
    /// Signed 32-bit integer, stored as INT32.
    INT_32,
    /// Signed 64-bit integer, stored as INT64.
    INT_64,
    /// JSON document stored as a UTF-8 string.
    JSON,
    /// BSON document stored as a byte array.
    BSON,
    /// Interval stored as a 12-byte fixed-length byte array.
    INTERVAL
}
