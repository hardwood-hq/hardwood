/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Physical types supported by Parquet format.
/// These represent how data is stored on disk.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/types/">File Format – Types</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public enum PhysicalType {
    /// Single-bit boolean value.
    BOOLEAN,
    /// 32-bit signed integer.
    INT32,
    /// 64-bit signed integer.
    INT64,
    /// 96-bit value; deprecated, used for legacy timestamps.
    INT96,
    /// IEEE 32-bit floating point.
    FLOAT,
    /// IEEE 64-bit floating point.
    DOUBLE,
    /// Variable-length byte array (also used for strings).
    BYTE_ARRAY,
    /// Fixed-length byte array; length is specified by the schema element's type length.
    FIXED_LEN_BYTE_ARRAY
}
