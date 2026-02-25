/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.schema;

/**
 * Legacy original types (converted types) for parquet-java compatibility.
 * <p>
 * These correspond to the ConvertedType enum in the Parquet format specification.
 * Modern Parquet files use LogicalType instead, but this is kept for backwards
 * compatibility with older parquet-java code.
 * </p>
 */
public enum OriginalType {
    /** UTF-8 encoded string */
    UTF8,
    /** Map container */
    MAP,
    /** Key-value pair within a map */
    MAP_KEY_VALUE,
    /** List container */
    LIST,
    /** Enum value (stored as string) */
    ENUM,
    /** Decimal with specified precision and scale */
    DECIMAL,
    /** Date (days since epoch) */
    DATE,
    /** Time in milliseconds */
    TIME_MILLIS,
    /** Time in microseconds */
    TIME_MICROS,
    /** Timestamp in milliseconds */
    TIMESTAMP_MILLIS,
    /** Timestamp in microseconds */
    TIMESTAMP_MICROS,
    /** Unsigned 8-bit integer */
    UINT_8,
    /** Unsigned 16-bit integer */
    UINT_16,
    /** Unsigned 32-bit integer */
    UINT_32,
    /** Unsigned 64-bit integer */
    UINT_64,
    /** Signed 8-bit integer */
    INT_8,
    /** Signed 16-bit integer */
    INT_16,
    /** Signed 32-bit integer */
    INT_32,
    /** Signed 64-bit integer */
    INT_64,
    /** JSON string */
    JSON,
    /** BSON binary */
    BSON,
    /** Time interval */
    INTERVAL
}
