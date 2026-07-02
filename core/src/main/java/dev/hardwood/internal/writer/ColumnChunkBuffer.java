/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Accumulates one `REQUIRED INT32` column's values for the current row group, packing
/// them into uncompressed `PLAIN` data pages of at most the page-value capacity. Values
/// arrive across one or more batches; a page is sealed each time the pending buffer
/// fills, and the tail is sealed at flush. The encoded pages are held until the row
/// group is flushed, since a column chunk's `data_page_offset` is only known when its
/// bytes are written.
final class ColumnChunkBuffer {

    private final int[] pending;
    private final ByteArrayOutputStream pages = new ByteArrayOutputStream();
    private int pendingCount;
    private long numValues;

    /// @param pageValues maximum number of `INT32` values per data page
    ColumnChunkBuffer(int pageValues) {
        this.pending = new int[pageValues];
    }

    /// Appends `count` values starting at `srcPos` in `source`, sealing pages as the
    /// pending buffer fills.
    void append(IntColumnSource source, int srcPos, int count) {
        int remaining = count;
        int from = srcPos;
        while (remaining > 0) {
            int space = pending.length - pendingCount;
            int n = Math.min(space, remaining);
            source.copyInto(from, pending, pendingCount, n);
            pendingCount += n;
            from += n;
            remaining -= n;
            if (pendingCount == pending.length) {
                sealPage();
            }
        }
    }

    /// Seals the trailing page, writes the whole column chunk to `out` at the current
    /// position, and returns its metadata. The caller captures `dataPageOffset` before
    /// invoking this, as it is the offset of the first page.
    ColumnMetaData flushTo(OutputFile out, ColumnSchema column, long dataPageOffset) throws IOException {
        sealPage();
        long totalSize = pages.size();
        out.write(ByteBuffer.wrap(pages.toByteArray()));
        // Pages are stored uncompressed, so compressed and uncompressed sizes are equal.
        return new ColumnMetaData(
                PhysicalType.INT32,
                List.of(Encoding.PLAIN),
                column.fieldPath(),
                CompressionCodec.UNCOMPRESSED,
                numValues,
                totalSize,
                totalSize,
                Map.of(),
                dataPageOffset,
                null,
                null,
                null,
                null,
                null);
    }

    private void sealPage() {
        if (pendingCount == 0) {
            return;
        }
        byte[] valueBytes = PlainEncoder.encodeInts(pending, 0, pendingCount);
        // CRC-32 over the page body as stored on disk (uncompressed here), matching what
        // the reader validates.
        CRC32 crc = new CRC32();
        crc.update(valueBytes);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, pendingCount, valueBytes.length, valueBytes.length,
                (int) crc.getValue(), Encoding.PLAIN);
        pages.writeBytes(header.toByteArray());
        pages.writeBytes(valueBytes);
        numValues += pendingCount;
        pendingCount = 0;
    }
}
