/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Parquet file writer with a columnar API.
///
/// [ParquetFileWriter] writes a flat schema of `REQUIRED` and `OPTIONAL INT32` columns to
/// a [dev.hardwood.OutputFile], taking each column's values through [ColumnBatch] slices
/// and banding them into size-bounded pages and row groups. Build the target schema with
/// [dev.hardwood.schema.FileSchema#builder].
package dev.hardwood.writer;
