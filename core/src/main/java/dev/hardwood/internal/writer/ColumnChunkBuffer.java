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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.encoding.LevelEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Accumulates one flat `INT32` column's values for the current row group, packing them
/// into uncompressed `PLAIN` data pages of at most the page-value capacity. Values arrive
/// across one or more batches; a page is sealed each time the pending buffer fills, and
/// the tail is sealed at flush. The encoded pages are held until the row group is flushed,
/// since a column chunk's `data_page_offset` is only known when its bytes are written.
///
/// For an `OPTIONAL` column each page carries a definition-level stream ahead of its
/// values: `[4-byte LE def-level length][RLE def-levels][PLAIN of the non-null values]`.
/// A `REQUIRED` column has no levels and writes its values directly.
final class ColumnChunkBuffer {

    private final int[] pending;
    private final boolean[] pendingNulls;
    private final int maxDefLevel;
    private final int[] defLevels;
    private final int[] compactValues;
    private final ByteArrayOutputStream pages = new ByteArrayOutputStream();
    private int pendingCount;
    private long numValues;

    /// @param pageValues maximum number of rows per data page
    /// @param maxDefLevel the column's maximum definition level (0 for `REQUIRED`, 1 for a
    ///        flat `OPTIONAL` column), which selects whether pages carry def levels
    ColumnChunkBuffer(int pageValues, int maxDefLevel) {
        this.pending = new int[pageValues];
        this.maxDefLevel = maxDefLevel;
        if (maxDefLevel > 0) {
            this.pendingNulls = new boolean[pageValues];
            this.defLevels = new int[pageValues];
            this.compactValues = new int[pageValues];
        }
        else {
            this.pendingNulls = null;
            this.defLevels = null;
            this.compactValues = null;
        }
    }

    /// Appends `count` rows starting at `srcPos` in `source`, sealing pages as the pending
    /// buffer fills. `validity` carries the rows' nulls (indexed absolutely into the batch)
    /// or is `null` when every appended row is present.
    void append(IntColumnSource source, Validity validity, int srcPos, int count) {
        int remaining = count;
        int from = srcPos;
        while (remaining > 0) {
            int space = pending.length - pendingCount;
            int n = Math.min(space, remaining);
            source.copyInto(from, pending, pendingCount, n);
            if (pendingNulls != null) {
                // Start all-present (also clearing the reused slots from a prior page), then
                // punch the nulls the validity reports over this range — representation
                // agnostic, so a dense or a future sparse validity feed the same code.
                Arrays.fill(pendingNulls, pendingCount, pendingCount + n, false);
                if (validity != null) {
                    int end = from + n;
                    for (int i = validity.nextNull(from, end); i != -1; i = validity.nextNull(i + 1, end)) {
                        pendingNulls[pendingCount + (i - from)] = true;
                    }
                }
            }
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
        List<Encoding> encodings = pendingNulls == null
                ? List.of(Encoding.PLAIN)
                : List.of(Encoding.RLE, Encoding.PLAIN);
        // Pages are stored uncompressed, so compressed and uncompressed sizes are equal.
        return new ColumnMetaData(
                PhysicalType.INT32,
                encodings,
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
        byte[] body = maxDefLevel > 0 ? optionalBody() : PlainEncoder.encodeInts(pending, 0, pendingCount);
        // CRC-32 over the page body as stored on disk (uncompressed here), matching what
        // the reader validates.
        CRC32 crc = new CRC32();
        crc.update(body);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, pendingCount, body.length, body.length,
                (int) crc.getValue(), Encoding.PLAIN);
        pages.writeBytes(header.toByteArray());
        pages.writeBytes(body);
        numValues += pendingCount;
        pendingCount = 0;
    }

    /// Builds an `OPTIONAL` page body: the def-level length prefix, the RLE def-level
    /// stream, then the `PLAIN` values of the non-null rows only.
    private byte[] optionalBody() {
        int nonNull = 0;
        for (int i = 0; i < pendingCount; i++) {
            if (pendingNulls[i]) {
                defLevels[i] = 0;
            }
            else {
                defLevels[i] = maxDefLevel;
                compactValues[nonNull++] = pending[i];
            }
        }
        byte[] defBytes = LevelEncoder.encode(defLevels, 0, pendingCount, maxDefLevel);
        byte[] valueBytes = PlainEncoder.encodeInts(compactValues, 0, nonNull);

        ByteArrayOutputStream body = new ByteArrayOutputStream(4 + defBytes.length + valueBytes.length);
        body.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(defBytes.length).array());
        body.writeBytes(defBytes);
        body.writeBytes(valueBytes);
        return body.toByteArray();
    }
}
