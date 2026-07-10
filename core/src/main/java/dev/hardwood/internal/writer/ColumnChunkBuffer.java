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
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.encoding.LevelEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Accumulates one `INT32` column's level entries for the current row group, packing them
/// into uncompressed `PLAIN` data pages of at most the page capacity. The [RecordShredder]
/// streams a record range's entries into this buffer through [#accept] (as a
/// [RecordShredder.LevelSink]); a page is sealed each time the pending buffer fills — even
/// part-way through a record — and the tail is sealed at flush. The encoded pages are held
/// until the row group is flushed, since a column chunk's `data_page_offset` is only known
/// when its bytes are written.
///
/// A page body is `[rep levels?][def levels?][PLAIN present values]`, each level stream
/// prefixed by its 4-byte little-endian length. The page capacity bounds the number of
/// level entries (`num_values`), which for a repeated column exceeds the record count.
final class ColumnChunkBuffer implements RecordShredder.LevelSink {

    private final int maxDefLevel;
    private final int maxRepLevel;
    private final int[] pendingRep;
    private final int[] pendingDef;
    private final int[] pendingValues;
    private final ByteArrayOutputStream pages = new ByteArrayOutputStream();
    private int pendingCount;      // level entries buffered for the current page
    private int pendingValueCount; // present values buffered for the current page
    private long numValues;        // total level entries across sealed pages

    /// @param pageValues maximum number of level entries per data page
    /// @param maxDefLevel the column's maximum definition level (0 selects no def stream)
    /// @param maxRepLevel the column's maximum repetition level (0 selects no rep stream)
    ColumnChunkBuffer(int pageValues, int maxDefLevel, int maxRepLevel) {
        this.maxDefLevel = maxDefLevel;
        this.maxRepLevel = maxRepLevel;
        this.pendingValues = new int[pageValues];
        this.pendingDef = maxDefLevel > 0 ? new int[pageValues] : null;
        this.pendingRep = maxRepLevel > 0 ? new int[pageValues] : null;
    }

    /// Shreds records `[fromRecord, fromRecord + count)` of this column straight into the
    /// page buffers, sealing pages as they fill.
    void append(RecordShredder shredder, int columnIndex, int fromRecord, int count) {
        shredder.shred(columnIndex, fromRecord, count, this);
    }

    @Override
    public void accept(int repetitionLevel, int definitionLevel, boolean present, int value) {
        if (maxRepLevel > 0) {
            pendingRep[pendingCount] = repetitionLevel;
        }
        if (maxDefLevel > 0) {
            pendingDef[pendingCount] = definitionLevel;
        }
        if (present) {
            pendingValues[pendingValueCount++] = value;
        }
        pendingCount++;
        if (pendingCount == pendingValues.length) {
            sealPage();
        }
    }

    /// Seals the trailing page, writes the whole column chunk to `out` at the current
    /// position, and returns its metadata. The caller captures `dataPageOffset` before
    /// invoking this, as it is the offset of the first page.
    ColumnMetaData flushTo(OutputFile out, ColumnSchema column, long dataPageOffset) throws IOException {
        sealPage();
        long totalSize = pages.size();
        out.write(ByteBuffer.wrap(pages.toByteArray()));
        List<Encoding> encodings = maxDefLevel > 0 || maxRepLevel > 0
                ? List.of(Encoding.RLE, Encoding.PLAIN)
                : List.of(Encoding.PLAIN);
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
        byte[] body = buildBody();
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
        pendingValueCount = 0;
    }

    /// Frames the page body: the repetition and definition level streams (each RLE, each
    /// prefixed by a 4-byte little-endian length) ahead of the `PLAIN` present values. A
    /// stream is omitted when its max level is 0, exactly the layout the reader parses.
    private byte[] buildBody() {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (maxRepLevel > 0) {
            writeLevels(body, pendingRep, maxRepLevel);
        }
        if (maxDefLevel > 0) {
            writeLevels(body, pendingDef, maxDefLevel);
        }
        body.writeBytes(PlainEncoder.encodeInts(pendingValues, 0, pendingValueCount));
        return body.toByteArray();
    }

    private void writeLevels(ByteArrayOutputStream body, int[] levels, int maxLevel) {
        byte[] encoded = LevelEncoder.encode(levels, 0, pendingCount, maxLevel);
        body.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.length).array());
        body.writeBytes(encoded);
    }
}
