/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Parquet file readers with row-oriented and column-oriented APIs.
///
/// [ParquetFileReader] opens one or more files and provides access to metadata
/// and schema. From there, create a [RowReader] for row-at-a-time access with
/// typed getters, a [ColumnReader] for single-column batch-oriented access, or
/// a [ColumnReaders] for multi-column projection access. [FilterPredicate]
/// enables predicate pushdown at both the row-group and page level.
///
/// For reading multiple files as a single dataset with cross-file
/// prefetching, use [dev.hardwood.Hardwood] to share a thread pool across
/// readers.
package dev.hardwood.reader;
