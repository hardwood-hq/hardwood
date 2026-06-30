/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Parquet file writer with a columnar API.
///
/// [ParquetFileWriter] writes a flat schema to a [dev.hardwood.OutputFile] as a
/// single row group, taking each column's values through a typed columnar method.
/// Build the target schema with [dev.hardwood.schema.FileSchema#builder].
package dev.hardwood.writer;
