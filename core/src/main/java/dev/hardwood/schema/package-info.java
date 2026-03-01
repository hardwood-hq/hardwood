/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * High-level Parquet schema types with computed definition and repetition levels.
 *
 * <p>{@link dev.hardwood.schema.FileSchema} is the main entry point, providing
 * both a flat list of {@link dev.hardwood.schema.ColumnSchema columns} and a
 * hierarchical {@link dev.hardwood.schema.SchemaNode tree} representation.
 *
 * @see <a href="https://parquet.apache.org/docs/file-format/nestedencoding/">File Format – Nested Encoding</a>
 * @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
 */
package dev.hardwood.schema;
