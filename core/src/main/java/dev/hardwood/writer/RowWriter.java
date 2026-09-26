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

import dev.hardwood.Experimental;
import dev.hardwood.internal.writer.RowPlan;
import dev.hardwood.schema.FileSchema;

/// Writes a Parquet file record by record, over the same columnar core [ColumnWriter]
/// exposes through [ColumnWriter#writeBatch].
///
/// This is the write-side mirror of the reader's `rowReader()`: the ergonomic API for a
/// caller that holds records rather than columns. Fields are addressed by the name they carry
/// in the schema, nesting is entered with a filler per level, and logical-type values are
/// written as the Java types the reader returns for them.
///
/// ```java
/// try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
///     RowWriter rows = writer.rowWriter();
///     for (Person person : people) {
///         rows.writeRow(row -> row
///                 .setLong("id", person.id())
///                 .setString("name", person.name())
///                 .setDate("hired", person.hired())
///                 .setStruct("address", address -> address.setString("city", person.city()))
///                 .setList("phones", phones -> person.phones().forEach(phones::addString)));
///     }
/// }
/// ```
///
/// Records are staged into a batch and submitted through the columnar path, so paging, row
/// group cadence, dictionary encoding, compression and statistics are exactly those of a file
/// written through [ColumnWriter#writeBatch]. A batch is submitted once it holds enough
/// records, or once its variable-width payload reaches the configured row-group size, so
/// staging never exceeds one row group regardless of how large the values are.
///
/// A `RowWriter` is not closeable: the [ParquetFileWriter] it came from owns the file, and
/// closing it writes the records still staged here along with the footer.
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public final class RowWriter {

    /// Records staged before a batch is submitted. Not a [WriterConfig] option: a batch is an
    /// arrival unit with no effect on the file produced, whose layout the page and row-group
    /// targets already govern.
    private static final int STAGED_RECORDS_PER_BATCH = 1024;

    private final ParquetFileWriter writer;
    private final RowPlan plan;

    /// Staged variable-width payload at which a batch is submitted early, so a record of
    /// large `BYTE_ARRAY` values cannot let [#STAGED_RECORDS_PER_BATCH] records hold
    /// arbitrarily much.
    private final long payloadLimitBytes;

    private int stagedRecords;

    RowWriter(ParquetFileWriter writer, FileSchema schema, WriterConfig config) {
        this.writer = writer;
        this.plan = RowPlan.build(schema, config.precisionLossPolicy());
        this.payloadLimitBytes = config.rowGroupBufferTargetBytes();
    }

    /// Writes one record.
    ///
    /// The writer creates the builder — bound to the schema — passes it to `filler` to be
    /// populated, then stages the record. A field the filler leaves unset is written as null
    /// if it is `OPTIONAL`, and fails the record if it is `REQUIRED`.
    ///
    /// Any exception fails the writer, whether the record is rejected, its `filler` throws, or a
    /// batch of staged records cannot be written: the writer accepts no more records, and
    /// [ParquetFileWriter#close()] discards the output.
    ///
    /// @param filler populates the record
    /// @throws IOException if writing a completed batch fails
    /// @throws IllegalArgumentException if the filler names a field the schema does not have,
    ///         sets one twice, uses a setter that does not fit a field's declared type, or
    ///         leaves a `REQUIRED` field unset; or if a record of the batch this call completes
    ///         has more values for a column than a column chunk can hold
    /// @throws IndexOutOfBoundsException if the filler addresses a field by an index the
    ///         struct it is setting does not have
    /// @throws IllegalStateException if the writer is closed, or a previous write has failed
    public void writeRow(Consumer<StructBuilder> filler) throws IOException {
        writer.ensureWritable();
        try {
            plan.writeRecord(filler);
        }
        catch (Throwable t) {
            writer.markFailed();
            throw t;
        }
        stagedRecords++;
        if (stagedRecords >= STAGED_RECORDS_PER_BATCH || plan.variableWidthBytes() >= payloadLimitBytes) {
            flush();
        }
    }

    /// Submits the records staged so far, called by [ParquetFileWriter#close()] before the
    /// final row group is flushed.
    void flushPending() throws IOException {
        flush();
    }

    private void flush() throws IOException {
        if (stagedRecords == 0) {
            return;
        }
        writer.writeStagedBatch(plan::fill);
        plan.reset();
        stagedRecords = 0;
    }
}
