/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.util.function.Consumer;

import dev.hardwood.Validity;

/// Writes a Parquet file one aligned batch of columns at a time.
///
/// This is the columnar API: the shape for a caller that already holds columns — a query
/// engine, an Arrow buffer, a bulk converter. Values are handed over as typed arrays addressed
/// by leaf index or leaf path, an `OPTIONAL` column carrying its nulls as a [Validity]
/// alongside them, and the writer shreds, pages, encodes and compresses them into the file.
///
/// ```java
/// try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
///     ColumnWriter columns = writer.columnWriter();
///     columns.writeBatch(batch -> batch
///             .longs("id", ids)
///             .doubles("price", prices, priceNulls));
/// }
/// ```
///
/// A `ColumnWriter` is not closeable: the [ParquetFileWriter] it came from owns the file, and
/// closing it flushes the row group still buffered here along with the footer.
public final class ColumnWriter {

    private final ParquetFileWriter writer;

    ColumnWriter(ParquetFileWriter writer) {
        this.writer = writer;
    }

    /// Writes one aligned batch of column values, flushing row groups as the buffered
    /// data crosses the row-group target. A batch that would overflow the current row
    /// group is split at the boundary.
    ///
    /// The writer creates the batch — bound to the schema — passes it to `filler` to be
    /// populated (columns addressed by index or name), then submits it. There is no
    /// separate build or submit step to forget.
    ///
    /// A failure anywhere in the batch — validation or I/O — marks the writer as failed.
    /// Under the default [WriteFailurePolicy#DISCARD], a failed writer refuses subsequent writes
    /// and discards the output on [ParquetFileWriter#close()] rather than publishing a
    /// truncated file.
    ///
    /// @param filler populates the batch's columns; must cover every column exactly once
    /// @throws IOException if the write fails
    /// @throws IllegalArgumentException if the batch does not cover every column, or its
    ///         per-layer inputs do not agree on a record count
    /// @throws UnsupportedOperationException if the schema has a shape the writer cannot
    ///         produce
    /// @throws IllegalStateException if the writer is closed, or a previous write has failed
    public void writeBatch(Consumer<ColumnBatch> filler) throws IOException {
        writer.ensureOpen();
        writer.writeStagedBatch(filler);
    }
}
