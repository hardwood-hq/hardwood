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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

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
/// This increment writes flat schemas of `REQUIRED INT32` columns as a single row
/// group of uncompressed, PLAIN-encoded data pages. A column larger than the target
/// page size is split across multiple size-bounded data pages. Each column's values
/// are supplied once via [#writeInts]; the row group and footer are finalized on
/// [#close()].
///
/// The file is produced front to back and is valid only after `close()` returns.
public final class ParquetFileWriter implements Closeable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FORMAT_VERSION = 1;
    private static final String CREATED_BY = "hardwood";

    /// Target uncompressed size of a single data page; a column larger than this is
    /// split across multiple pages. Keeping a page well under the i32 wire limit also
    /// means a page's size and value-count fields can never overflow. Becomes a
    /// `WriterConfig` knob in a later increment.
    private static final int TARGET_PAGE_BYTES = 1 << 20; // 1 MiB

    /// Number of `INT32` values that fit in one page at [#TARGET_PAGE_BYTES].
    private static final int INT32_VALUES_PER_PAGE = TARGET_PAGE_BYTES / Integer.BYTES;

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

        // data_page_offset points at the first page; the chunk's pages follow
        // contiguously. A column larger than one page is split into several.
        long dataPageOffset = out.position();
        long chunkSize = 0;
        int pos = 0;
        do {
            int count = Math.min(INT32_VALUES_PER_PAGE, values.length - pos);
            chunkSize += writeIntDataPage(values, pos, count);
            pos += count;
        }
        while (pos < values.length);

        // total_*_size cover the whole column chunk including page headers; pages
        // are stored uncompressed so the two sizes are equal.
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

    /// Encodes `count` values starting at `from` as one uncompressed PLAIN V1 data
    /// page and writes it. Returns the bytes written (page header + body).
    private long writeIntDataPage(int[] values, int from, int count) throws IOException {
        int[] pageValues = (from == 0 && count == values.length)
                ? values
                : Arrays.copyOfRange(values, from, from + count);
        byte[] valueBytes = PlainEncoder.encodeInts(pageValues);
        // CRC-32 over the page body as stored on disk (uncompressed here), which is
        // what the reader validates against.
        CRC32 crc = new CRC32();
        crc.update(valueBytes);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, count, valueBytes.length, valueBytes.length,
                (int) crc.getValue(), Encoding.PLAIN);
        byte[] headerBytes = header.toByteArray();
        out.write(ByteBuffer.wrap(headerBytes));
        out.write(ByteBuffer.wrap(valueBytes));
        return (long) headerBytes.length + valueBytes.length;
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
