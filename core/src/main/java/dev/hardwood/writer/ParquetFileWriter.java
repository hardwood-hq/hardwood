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
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.encoding.LevelEncoder;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.internal.writer.IntColumnSource;
import dev.hardwood.internal.writer.RecordShredder;
import dev.hardwood.internal.writer.RowGroupBuffer;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Writes a Parquet file through a columnar batch API.
///
/// This increment writes `INT32` columns — flat `REQUIRED` / `OPTIONAL`, nested inside
/// `REQUIRED` / `OPTIONAL` `struct` groups, and inside `LIST`s and `MAP`s (including lists
/// of lists, lists of structs, and maps of any in-scope value). Data is supplied as
/// [ColumnBatch] slices; the writer packs each column into size-bounded data pages — a
/// levelled column's pages carrying an RLE definition-level stream ahead of the values — and
/// flushes a row group once its buffered data reaches the configured target, so peak memory is
/// bounded regardless of how much is written. Columns are dictionary-encoded by default (a
/// dictionary page plus `RLE_DICTIONARY` index pages), falling back to `PLAIN` when the
/// dictionary grows past the configured limit. Each page body is compressed with the
/// configured codec (`ZSTD` by default, or `UNCOMPRESSED`). All of these are configurable
/// through [WriterConfig]. The row groups and footer are finalized on [#close()].
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
    private final RecordShredder shredder;
    private final Compressor compressor;
    private final List<RowGroup> rowGroups = new ArrayList<>();

    private RowGroupBuffer current;
    private long numRows;
    private boolean closed;

    private ParquetFileWriter(OutputFile out, FileSchema schema, WriterConfig config, Compressor compressor) {
        this.out = out;
        this.schema = schema;
        this.config = config;
        this.pageValues = pageRowCapacity(config.pageTargetBytes(), schema);
        this.maxRowsPerGroup = maxRowsPerGroup(config.rowGroupTargetBytes(), schema);
        this.shredder = new RecordShredder(schema, pageValues);
        this.compressor = compressor;
        this.current = newRowGroupBuffer();
    }

    private RowGroupBuffer newRowGroupBuffer() {
        return new RowGroupBuffer(schema, pageValues, config.enableDictionary(), config.dictionaryPageLimitBytes(),
                compressor, config.codec());
    }

    /// Opens a writer with the default [WriterConfig].
    ///
    /// @param out the destination
    /// @param schema the schema to write
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema has a non-`INT32` column
    public static ParquetFileWriter create(OutputFile out, FileSchema schema) throws IOException {
        return create(out, schema, WriterConfig.defaults());
    }

    /// Opens a writer, writing the leading magic bytes.
    ///
    /// @param out the destination
    /// @param schema the schema to write
    /// @param config the writer configuration
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema has a non-`INT32` column, or the
    ///         configured codec cannot be written
    public static ParquetFileWriter create(OutputFile out, FileSchema schema, WriterConfig config)
            throws IOException {
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            if (column.type() != PhysicalType.INT32) {
                throw new UnsupportedOperationException(
                        "Only INT32 columns are supported; column " + column.name() + " is " + column.type());
            }
        }
        // Resolve the codec before touching the output, so an unsupported codec or a missing
        // codec library fails before any bytes are written rather than leaving a partial file.
        Compressor compressor = new CompressorFactory().getCompressor(config.codec());
        out.create();
        out.write(ByteBuffer.wrap(MAGIC));
        return new ParquetFileWriter(out, schema, config, compressor);
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
        shredder.bind(sources, batch.validities(), batch.structValidities(),
                batch.listValidities(), batch.listOffsets());
        batch.markConsumed();
        int rows = shredder.recordCount();
        int pos = 0;
        while (pos < rows) {
            int space = maxRowsPerGroup - current.rowCount();
            int n = Math.min(space, rows - pos);
            current.appendRecords(shredder, pos, n);
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
        current = newRowGroupBuffer();
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

    /// Rows per data page whose encoded body fits the page target. A page costs
    /// `Integer.SIZE` bits per row for its `PLAIN` values plus, for an `OPTIONAL` column,
    /// its RLE definition-level stream; sizing to the widest column's per-row bit cost
    /// keeps every column's page within the target. At least one row so a tiny target
    /// still makes progress.
    private static int pageRowCapacity(long pageTargetBytes, FileSchema schema) {
        int maxColumnBitsPerRow = Integer.SIZE;
        for (int c = 0; c < schema.getColumnCount(); c++) {
            int defBits = LevelEncoder.bitWidth(schema.getColumn(c).maxDefinitionLevel());
            maxColumnBitsPerRow = Math.max(maxColumnBitsPerRow, Integer.SIZE + defBits);
        }
        long rows = pageTargetBytes * Byte.SIZE / maxColumnBitsPerRow;
        return (int) Math.max(1, Math.min(rows, Integer.MAX_VALUE));
    }

    /// Number of rows whose buffered row-group data fits the row-group target, counting
    /// every column's `PLAIN` values plus the RLE definition-level stream of each
    /// `OPTIONAL` column. At least one so the writer always makes progress even with a
    /// tiny target or a wide schema.
    private static int maxRowsPerGroup(long rowGroupTargetBytes, FileSchema schema) {
        int columnCount = schema.getColumnCount();
        if (columnCount == 0) {
            return Integer.MAX_VALUE;
        }
        long bitsPerRow = (long) columnCount * Integer.SIZE;
        for (int c = 0; c < columnCount; c++) {
            bitsPerRow += LevelEncoder.bitWidth(schema.getColumn(c).maxDefinitionLevel());
        }
        long rows = rowGroupTargetBytes * Byte.SIZE / bitsPerRow;
        return (int) Math.max(1, Math.min(rows, Integer.MAX_VALUE));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }
    }
}
