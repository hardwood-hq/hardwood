/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// High-level Parquet schema types with computed definition and repetition levels.
///
/// [dev.hardwood.schema.FileSchema] is the main entry point, providing
/// both a flat list of [columns][dev.hardwood.schema.ColumnSchema] and a
/// hierarchical [tree][dev.hardwood.schema.SchemaNode] representation.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/nestedencoding/">File Format – Nested Encoding</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
package dev.hardwood.schema;
