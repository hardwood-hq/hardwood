/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Field repetition types in Parquet schema.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/nestedencoding/">File Format – Nested Encoding</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public enum RepetitionType {
    /// Field must be present; nulls are not allowed.
    REQUIRED,
    /// Field may be null.
    OPTIONAL,
    /// Field can appear zero or more times (list semantics).
    REPEATED
}
