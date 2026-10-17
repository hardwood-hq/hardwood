/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Apache Arrow integration for reading Parquet columns as Arrow
/// [org.apache.arrow.vector.VectorSchemaRoot] batches.
///
/// [ArrowReaders] provides factory methods for creating readers with optional
/// filter predicates and column projections. [ArrowBatchReader] wraps a
/// [dev.hardwood.reader.ColumnReaders] and serves each of its batches through
/// Arrow Java's [org.apache.arrow.vector.ipc.ArrowReader] contract.
package dev.hardwood.arrow;
