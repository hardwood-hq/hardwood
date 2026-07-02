/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.internal.writer.IntColumnSource;
import dev.hardwood.internal.writer.RowGroupBuffer;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Writes a Parquet file through a columnar batch API.
///
/// This increment writes flat schemas of `REQUIRED` and `OPTIONAL INT32` columns. Data is
/// supplied as [ColumnBatch] slices; the writer packs each column into size-bounded,
/// uncompressed `PLAIN` data pages — an `OPTIONAL` column's pages carrying an RLE
/// definition-level stream ahead of the non-null values — and flushes a row group once
/// its buffered data reaches the configured target, so peak memory is bounded regardless
/// of how much is written. The row groups and footer are finalized on [#close()].
///
/// The file is produced front to back and is valid only after `close()` returns.
public final class ParquetFileWriter implements Closeable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FORMAT_VERSION = 1;

    private final OutputFile out;
    private final FileSchema schema;
    private final WriterConfig config;
    private final int pageValues;
    private final int maxRowsPerGroup;
    private final List<RowGroup> rowGroups = new ArrayList<>();

    private RowGroupBuffer current;
    private long numRows;
    private boolean closed;

    private ParquetFileWriter(OutputFile out, FileSchema schema, WriterConfig config) {
        this.out = out;
        this.schema = schema;
        this.config = config;
        this.pageValues = config.pageTargetBytes() / Integer.BYTES;
        this.maxRowsPerGroup = maxRowsPerGroup(config.rowGroupTargetBytes(), schema.getColumnCount());
        this.current = new RowGroupBuffer(schema, pageValues);
    }

    /// Opens a writer with the default [WriterConfig].
    ///
    /// @param out the destination
    /// @param schema the flat schema to write
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema is not a flat schema of `INT32`
    ///         columns
    public static ParquetFileWriter create(OutputFile out, FileSchema schema) throws IOException {
        return create(out, schema, WriterConfig.defaults());
    }

    /// Opens a writer, writing the leading magic bytes.
    ///
    /// @param out the destination
    /// @param schema the flat schema to write
    /// @param config the writer configuration
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema is not a flat schema of `INT32`
    ///         columns
    public static ParquetFileWriter create(OutputFile out, FileSchema schema, WriterConfig config)
            throws IOException {
        if (!schema.isFlatSchema()) {
            throw new UnsupportedOperationException("Only flat schemas are supported by the writer");
        }
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            if (column.type() != PhysicalType.INT32) {
                throw new UnsupportedOperationException(
                        "Only INT32 columns are supported; column " + column.name() + " is " + column.type());
            }
        }
        out.create();
        out.write(ByteBuffer.wrap(MAGIC));
        return new ParquetFileWriter(out, schema, config);
    }

    /// Writes one aligned batch of column values, flushing row groups as the buffered
    /// data crosses the row-group target. A batch that would overflow the current row
    /// group is split at the boundary.
    ///
    /// The writer creates the batch — bound to the schema — passes it to `filler` to be
    /// populated (columns addressed by index or name), then submits it. There is no
    /// separate build or submit step to forget.
    ///
    /// @param filler populates the batch's columns; must cover every column exactly once
    /// @throws IOException if the write fails
    /// @throws IllegalArgumentException if the batch does not cover every column
    public void writeBatch(Consumer<ColumnBatch> filler) throws IOException {
        ensureOpen();
        ColumnBatch batch = new ColumnBatch(schema);
        filler.accept(batch);
        IntColumnSource[] sources = batch.completedSources();
        Validity[] validities = batch.validities();
        batch.markConsumed();
        int rows = batch.rowCount();
        int pos = 0;
        while (pos < rows) {
            int space = maxRowsPerGroup - current.rowCount();
            int n = Math.min(space, rows - pos);
            current.appendRows(sources, validities, pos, n);
            pos += n;
            if (current.rowCount() >= maxRowsPerGroup) {
                flushRowGroup();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            flushRowGroup();
            writeFooter();
        }
        catch (IOException | RuntimeException e) {
            // The footer is incomplete, so the file is not valid. Discard it rather than
            // letting out.close() publish a truncated file.
            try {
                out.discard();
            }
            catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        out.close();
    }

    private void flushRowGroup() throws IOException {
        if (current.isEmpty()) {
            return;
        }
        RowGroup rowGroup = current.flushTo(out);
        rowGroups.add(rowGroup);
        numRows += rowGroup.numRows();
        current = new RowGroupBuffer(schema, pageValues);
    }

    private void writeFooter() throws IOException {
        FileMetaData metaData = new FileMetaData(
                FORMAT_VERSION,
                schema.toSchemaElements(),
                numRows,
                List.copyOf(rowGroups),
                Map.of(),
                config.createdBy(),
                List.of());

        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, metaData);
        byte[] footerBytes = footer.toByteArray();

        out.write(ByteBuffer.wrap(footerBytes));
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).flip());
        out.write(ByteBuffer.wrap(MAGIC));
    }

    /// Number of rows whose values fit within the row-group target, at least one so the
    /// writer always makes progress even with a tiny target or a wide schema.
    private static int maxRowsPerGroup(long rowGroupTargetBytes, int columnCount) {
        if (columnCount == 0) {
            return Integer.MAX_VALUE;
        }
        long bytesPerRow = (long) columnCount * Integer.BYTES;
        long rows = rowGroupTargetBytes / bytesPerRow;
        return (int) Math.max(1, Math.min(rows, Integer.MAX_VALUE));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }
    }
}
