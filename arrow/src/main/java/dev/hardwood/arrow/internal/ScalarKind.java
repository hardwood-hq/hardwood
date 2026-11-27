/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

/// How a leaf column's batch values are copied into its Arrow buffers, resolved once during schema
/// conversion.
public enum ScalarKind {
    BOOLEAN,
    INT32,
    INT8,
    INT16,
    UINT8,
    UINT16,
    INT64,
    FLOAT,
    DOUBLE,
    DECIMAL_INT32,
    DECIMAL_INT64,
    DECIMAL_BYTES,
    INT96_TIMESTAMP,
    BINARY,
    FIXED_BINARY,
    /// Parquet `UNKNOWN` logical type: length only, no buffers.
    NULL
}
