/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Low-level Parquet file metadata types that mirror the Thrift definitions.
///
/// These types provide direct access to the metadata structures stored in
/// a Parquet file footer. For a higher-level schema representation with
/// computed definition and repetition levels, see
/// [dev.hardwood.schema].
///
/// The record types in this package (e.g. [FileMetaData], [RowGroup],
/// [ColumnChunk], [Statistics]) are **read-only views** of the metadata in a
/// Parquet file: Hardwood constructs them while parsing, and callers consume
/// them through their accessors. Their canonical constructors and component
/// lists are not part of the supported API — do not instantiate these records
/// directly, and do not rely on record deconstruction patterns over them.
/// Because these types mirror the evolving Parquet Thrift definitions, future
/// releases may add components; such additions are treated as
/// backward-compatible under this policy even though they change the canonical
/// constructor.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/metadata/">File Format – Metadata</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
package dev.hardwood.metadata;
