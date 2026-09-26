/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Parquet file writer, with a columnar and a row-oriented API over one core.
///
/// [ParquetFileWriter] writes to a [dev.hardwood.OutputFile], banding values into
/// size-bounded pages and row groups. Build the target schema with
/// [dev.hardwood.schema.FileSchema#builder] and pick the API that fits the caller:
///
/// - [ParquetFileWriter#columnWriter()] returns a [ColumnWriter], which takes an aligned slice
///   of typed arrays through [ColumnBatch], for a caller that already holds columns.
/// - [ParquetFileWriter#rowWriter()] returns a [RowWriter], which takes one record at a
///   time through [StructBuilder], [ListBuilder] and [MapBuilder], for a caller that holds
///   records. It stages records into batches and submits them through the same core.
///
/// A file is written through one of the two, not both. [WriterConfig] carries the page and
/// row-group targets, the codec and the encoding policies for either.
package dev.hardwood.writer;
