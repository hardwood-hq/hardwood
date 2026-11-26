/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Apache Arrow integration for converting Parquet schemas and reading Parquet columns, nested
/// ones included, as Arrow [org.apache.arrow.vector.VectorSchemaRoot] batches.
///
/// [ArrowReaders] provides standalone schema conversion (`schema`) and factory methods for
/// building readers with optional column projections, record and row-group filters, batch sizes,
/// and dictionary-encoded output. [ArrowBatchReader] wraps a [dev.hardwood.reader.ColumnReaders]
/// and serves each batch (and any dictionaries) through Arrow Java's
/// [org.apache.arrow.vector.ipc.ArrowReader] contract.
package dev.hardwood.arrow;
