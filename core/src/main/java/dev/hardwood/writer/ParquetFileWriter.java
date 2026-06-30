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

import dev.hardwood.OutputFile;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Writes a Parquet file through a columnar API.
///
/// This first increment writes flat schemas of `REQUIRED INT32` columns as a
/// single row group with one uncompressed, PLAIN-encoded data page per column.
/// Each column's values are supplied once via [#writeInts]; the row group and
/// footer are finalized on [#close()].
///
/// The file is produced front to back and is valid only after `close()` returns.
public final class ParquetFileWriter implements Closeable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FORMAT_VERSION = 1;
    private static final String CREATED_BY = "hardwood";

    /// A data page's body length and value count are i32 on the wire, so a single
    /// page cannot exceed this many bytes. Until multi-page writing lands a column
    /// is written as one page, so this also bounds a single column's values.
    private static final long MAX_DATA_PAGE_BYTES = Integer.MAX_VALUE;

    private final OutputFile out;
    private final FileSchema schema;
    private final ColumnMetaData[] columnMeta;
    private long numRows = -1;
    private boolean closed;

    private ParquetFileWriter(OutputFile out, FileSchema schema) {
        this.out = out;
        this.schema = schema;
        this.columnMeta = new ColumnMetaData[schema.getColumnCount()];
    }

    /// Opens a writer, writing the leading magic bytes.
    ///
    /// @param out the destination
    /// @param schema the flat schema to write
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema is not flat
    public static ParquetFileWriter create(OutputFile out, FileSchema schema) throws IOException {
        if (!schema.isFlatSchema()) {
            throw new UnsupportedOperationException("Only flat schemas are supported by the writer");
        }
        out.create();
        out.write(ByteBuffer.wrap(MAGIC));
        return new ParquetFileWriter(out, schema);
    }

    /// Writes the full contents of a `REQUIRED INT32` column.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column values; its length must match every other column
    /// @throws IOException if the write fails
    public void writeInts(int columnIndex, int[] values) throws IOException {
        ensureOpen();
        ColumnSchema column = column(columnIndex);
        if (column.type() != PhysicalType.INT32) {
            throw new UnsupportedOperationException(
                    "Only INT32 columns are supported; column " + column.name() + " is " + column.type());
        }
        if (column.repetitionType() != RepetitionType.REQUIRED) {
            throw new UnsupportedOperationException(
                    "Only REQUIRED columns are supported; column " + column.name() + " is " + column.repetitionType());
        }
        if (columnMeta[columnIndex] != null) {
            throw new IllegalStateException("Column already written: " + column.name());
        }
        checkRowCount(values.length, column);
        checkPageFits(values.length, Integer.BYTES, column.name());

        byte[] valueBytes = PlainEncoder.encodeInts(values);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, values.length, valueBytes.length, valueBytes.length, Encoding.PLAIN);
        byte[] headerBytes = header.toByteArray();

        long dataPageOffset = out.position();
        out.write(ByteBuffer.wrap(headerBytes));
        out.write(ByteBuffer.wrap(valueBytes));

        // total_*_size cover the whole column chunk including page headers; the
        // page is stored uncompressed so the two sizes are equal.
        long chunkSize = (long) headerBytes.length + valueBytes.length;
        columnMeta[columnIndex] = new ColumnMetaData(
                PhysicalType.INT32,
                List.of(Encoding.PLAIN),
                column.fieldPath(),
                CompressionCodec.UNCOMPRESSED,
                values.length,
                chunkSize,
                chunkSize,
                Map.of(),
                dataPageOffset,
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writeFooter();
        }
        catch (IOException | RuntimeException e) {
            // The footer is incomplete, so the file is not valid. Discard it
            // rather than letting out.close() publish a truncated file.
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

    private void writeFooter() throws IOException {
        List<ColumnChunk> chunks = new ArrayList<>(columnMeta.length);
        long totalByteSize = 0;
        for (int i = 0; i < columnMeta.length; i++) {
            if (columnMeta[i] == null) {
                throw new IllegalStateException("Column not written: " + column(i).name());
            }
            chunks.add(new ColumnChunk(columnMeta[i], null, null, null, null));
            totalByteSize += columnMeta[i].totalUncompressedSize();
        }
        long rows = numRows < 0 ? 0 : numRows;
        RowGroup rowGroup = new RowGroup(chunks, totalByteSize, rows);

        FileMetaData metaData = new FileMetaData(
                FORMAT_VERSION,
                schema.toSchemaElements(),
                rows,
                List.of(rowGroup),
                Map.of(),
                CREATED_BY,
                List.of());

        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, metaData);
        byte[] footerBytes = footer.toByteArray();

        out.write(ByteBuffer.wrap(footerBytes));
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).flip());
        out.write(ByteBuffer.wrap(MAGIC));
    }

    private void checkRowCount(int length, ColumnSchema column) {
        if (numRows < 0) {
            numRows = length;
        }
        else if (numRows != length) {
            throw new IllegalArgumentException("Column " + column.name() + " has " + length
                    + " values but the row group already has " + numRows + " rows");
        }
    }

    /// Fail fast when a column's values would not fit in a single data page. The
    /// writer emits one page per column today, so an oversized column must be
    /// rejected loudly rather than silently producing a page with overflowed i32
    /// size fields. Multi-page writing removes this limit.
    static void checkPageFits(int numValues, int bytesPerValue, String columnName) {
        long pageBytes = (long) numValues * bytesPerValue;
        if (pageBytes > MAX_DATA_PAGE_BYTES) {
            throw new IllegalArgumentException("Column '" + columnName + "' has " + numValues
                    + " values (" + pageBytes + " bytes), exceeding the single data-page limit of "
                    + MAX_DATA_PAGE_BYTES + " bytes. Writing a column as multiple pages is not yet implemented.");
        }
    }

    private ColumnSchema column(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnMeta.length) {
            throw new IndexOutOfBoundsException(
                    "Column index " + columnIndex + " out of range [0, " + columnMeta.length + ")");
        }
        return schema.getColumn(columnIndex);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }
    }
}
