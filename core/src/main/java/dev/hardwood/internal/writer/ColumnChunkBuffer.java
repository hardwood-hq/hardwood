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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.OutputFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.encoding.DictionaryEncoder;
import dev.hardwood.internal.encoding.LevelEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Accumulates one `INT32` column's level entries for the current row group, packing them
/// into data pages of at most the page capacity. The [RecordShredder] streams a
/// record range's entries into this buffer through [#accept] (as a [RecordShredder.LevelSink]);
/// a page is sealed each time the pending buffer fills — even part-way through a record — and
/// the tail is sealed at flush. The encoded pages are held until the row group is flushed,
/// since a column chunk's page offsets are only known when its bytes are written.
///
/// A page body is `[rep levels?][def levels?][value section]`, each level stream prefixed by
/// its 4-byte little-endian length. When dictionary encoding is enabled the value section is
/// `[1-byte index bit width][RLE/bit-packed indices]` (`RLE_DICTIONARY`); otherwise, and after
/// a fallback, it is `[PLAIN present values]`. The assembled body (levels and values together)
/// is compressed with the chunk's [Compressor] before framing, and the page header records
/// both the uncompressed and the stored compressed size; the dictionary page body is
/// compressed the same way. The CRC-32 is taken over the stored bytes, matching what the
/// reader validates.
///
/// Dictionary encoding is column-chunk scoped: present values are interned in first-seen order
/// through a [DictionaryEncoder], and each page's indices reference the dictionary page written
/// ahead of the data pages at flush. When the dictionary would exceed `dictionaryLimitBytes`
/// the chunk **falls back** — the pending page is sealed as `RLE_DICTIONARY` and every
/// subsequent page is `PLAIN`. Encoding is per-page (each page's header declares its own), so a
/// chunk may hold any mix of the two; a page sealed while the dictionary is still empty (a
/// leading run of nulls filling a page) is `PLAIN` even ahead of later `RLE_DICTIONARY` pages.
final class ColumnChunkBuffer implements RecordShredder.LevelSink {

    private final int maxDefLevel;
    private final int maxRepLevel;
    private final int[] pendingRep;
    private final int[] pendingDef;
    private final int[] pendingValues; // indices while dictionary-encoding, raw values once plain
    private final ByteArrayOutputStream pages = new ByteArrayOutputStream(); // stored (compressed) data pages
    private int pendingCount;      // level entries buffered for the current page
    private int pendingValueCount; // present values buffered for the current page
    private long numValues;        // total level entries across sealed pages
    private long dataPagesUncompressedSize; // sum of header + uncompressed body across data pages
    private long dictionaryPageUncompressedSize; // header + uncompressed body of the dictionary page

    private final Compressor compressor;
    private final CompressionCodec codec;

    private final DictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private final int dictionaryLimitBytes;
    private boolean dictionaryActive;    // true while pages are still dictionary-encoded

    /// @param pageValues maximum number of level entries per data page
    /// @param maxDefLevel the column's maximum definition level (0 selects no def stream)
    /// @param maxRepLevel the column's maximum repetition level (0 selects no rep stream)
    /// @param enableDictionary whether to dictionary-encode this chunk (with `PLAIN` fallback)
    /// @param dictionaryLimitBytes the dictionary size past which the chunk falls back to `PLAIN`
    /// @param compressor compresses each page body before framing
    /// @param codec the codec `compressor` applies, recorded in the chunk metadata
    ColumnChunkBuffer(int pageValues, int maxDefLevel, int maxRepLevel,
                      boolean enableDictionary, int dictionaryLimitBytes,
                      Compressor compressor, CompressionCodec codec) {
        this.maxDefLevel = maxDefLevel;
        this.maxRepLevel = maxRepLevel;
        this.pendingValues = new int[pageValues];
        this.pendingDef = maxDefLevel > 0 ? new int[pageValues] : null;
        this.pendingRep = maxRepLevel > 0 ? new int[pageValues] : null;
        this.dictionary = enableDictionary ? new DictionaryEncoder() : null;
        this.dictionaryLimitBytes = dictionaryLimitBytes;
        this.dictionaryActive = enableDictionary;
        this.compressor = compressor;
        this.codec = codec;
    }

    /// Shreds records `[fromRecord, fromRecord + count)` of this column straight into the
    /// page buffers, sealing pages as they fill.
    void append(RecordShredder shredder, int columnIndex, int fromRecord, int count) {
        shredder.shred(columnIndex, fromRecord, count, this);
    }

    @Override
    public void accept(int repetitionLevel, int definitionLevel, boolean present, int value) {
        // Decide fallback before recording the entry: falling back seals the buffered page and
        // resets the pending buffers, so the current entry must land in the fresh page.
        int index = 0;
        boolean useDictionary = false;
        if (present && dictionaryActive) {
            index = dictionary.indexOf(value);
            if (index < 0 && dictionary.byteSize() + Integer.BYTES > dictionaryLimitBytes) {
                fallBack();
            }
            useDictionary = dictionaryActive;
        }
        if (maxRepLevel > 0) {
            pendingRep[pendingCount] = repetitionLevel;
        }
        if (maxDefLevel > 0) {
            pendingDef[pendingCount] = definitionLevel;
        }
        if (present) {
            pendingValues[pendingValueCount++] = useDictionary
                    ? (index < 0 ? dictionary.add(value) : index)
                    : value;
        }
        pendingCount++;
        if (pendingCount == pendingValues.length) {
            sealPage();
        }
    }

    /// Seals the trailing page, writes the whole column chunk — dictionary page (when present)
    /// then data pages — to `out` starting at `chunkStartOffset`, and returns its metadata.
    /// The caller captures `chunkStartOffset` before invoking this.
    ColumnMetaData flushTo(OutputFile out, ColumnSchema column, long chunkStartOffset) throws IOException {
        sealPage();
        boolean hasDictionary = dictionary != null && dictionary.size() > 0;
        long dataPageOffset = chunkStartOffset;
        Long dictionaryPageOffset = null;
        long dictionaryCompressedSize = 0;
        long dictionaryUncompressedSize = 0;
        if (hasDictionary) {
            byte[] dictionaryPage = buildDictionaryPage();
            dictionaryPageOffset = chunkStartOffset;
            dataPageOffset = chunkStartOffset + dictionaryPage.length;
            dictionaryCompressedSize = dictionaryPage.length;
            dictionaryUncompressedSize = dictionaryPageUncompressedSize;
            out.write(ByteBuffer.wrap(dictionaryPage));
        }
        // Page headers are stored uncompressed either way; only the bodies differ, so the
        // compressed total is what was actually written and the uncompressed total restores
        // each body to its pre-compression size.
        long totalCompressed = dictionaryCompressedSize + pages.size();
        long totalUncompressed = dictionaryUncompressedSize + dataPagesUncompressedSize;
        out.write(ByteBuffer.wrap(pages.toByteArray()));
        return new ColumnMetaData(
                PhysicalType.INT32,
                encodings(hasDictionary),
                column.fieldPath(),
                codec,
                numValues,
                totalUncompressed,
                totalCompressed,
                Map.of(),
                dataPageOffset,
                dictionaryPageOffset,
                null,
                null,
                null,
                null);
    }

    /// Seals the current page as a dictionary-indexed page and switches the rest of the chunk
    /// to `PLAIN`. The already-buffered indices are valid against the dictionary as it stands,
    /// and the dictionary page (written at flush) still holds those values.
    private void fallBack() {
        sealPage();
        dictionaryActive = false;
    }

    private void sealPage() {
        if (pendingCount == 0) {
            return;
        }
        boolean dictionaryPage = dictionaryActive && dictionary != null && dictionary.size() > 0;
        byte[] body = buildBody(dictionaryPage);
        byte[] stored = compress(body);
        // CRC-32 over the page body as stored on disk (compressed), matching what the reader
        // validates.
        CRC32 crc = new CRC32();
        crc.update(stored);
        ThriftCompactWriter header = new ThriftCompactWriter();
        Encoding valuesEncoding = dictionaryPage ? Encoding.RLE_DICTIONARY : Encoding.PLAIN;
        PageHeaderWriter.writeDataPageV1(header, pendingCount, body.length, stored.length,
                (int) crc.getValue(), valuesEncoding);
        byte[] headerBytes = header.toByteArray();
        pages.writeBytes(headerBytes);
        pages.writeBytes(stored);
        dataPagesUncompressedSize += headerBytes.length + body.length;
        numValues += pendingCount;
        pendingCount = 0;
        pendingValueCount = 0;
    }

    /// Frames the page body: the repetition and definition level streams (each RLE, each
    /// prefixed by a 4-byte little-endian length) ahead of the value section. For a dictionary
    /// page the value section is a 1-byte index bit width followed by the RLE/bit-packed
    /// indices (running to the page end, not length-prefixed); otherwise it is the `PLAIN`
    /// present values.
    private byte[] buildBody(boolean dictionaryPage) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (maxRepLevel > 0) {
            writeLevels(body, pendingRep, maxRepLevel);
        }
        if (maxDefLevel > 0) {
            writeLevels(body, pendingDef, maxDefLevel);
        }
        if (dictionaryPage) {
            int bitWidth = LevelEncoder.bitWidth(dictionary.size() - 1);
            body.write(bitWidth);
            RleBitPackingHybridEncoder indices = new RleBitPackingHybridEncoder(bitWidth);
            indices.writeInts(pendingValues, 0, pendingValueCount);
            body.writeBytes(indices.toByteArray());
        }
        else {
            body.writeBytes(PlainEncoder.encodeInts(pendingValues, 0, pendingValueCount));
        }
        return body.toByteArray();
    }

    /// Builds the dictionary page: a `DICTIONARY_PAGE` header over the distinct values,
    /// `PLAIN`-encoded in index order and compressed with the chunk's codec. Records the
    /// page's uncompressed size (header plus uncompressed body) for the chunk metadata.
    private byte[] buildDictionaryPage() {
        byte[] body = PlainEncoder.encodeInts(dictionary.values(), 0, dictionary.size());
        byte[] stored = compress(body);
        CRC32 crc = new CRC32();
        crc.update(stored);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDictionaryPageV1(header, dictionary.size(), body.length, stored.length,
                (int) crc.getValue(), Encoding.PLAIN);
        byte[] headerBytes = header.toByteArray();
        dictionaryPageUncompressedSize = headerBytes.length + body.length;
        ByteArrayOutputStream page = new ByteArrayOutputStream();
        page.writeBytes(headerBytes);
        page.writeBytes(stored);
        return page.toByteArray();
    }

    /// Compresses a page body with the chunk's codec. Compressing an in-memory buffer that
    /// fails is unrecoverable, so a codec error surfaces as an unchecked exception rather than
    /// forcing a checked-exception path through the [RecordShredder.LevelSink] callback.
    private byte[] compress(byte[] body) {
        try {
            return compressor.compress(body, 0, body.length);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to " + codec + "-compress a page body", e);
        }
    }

    /// The deduplicated set of encodings the chunk actually uses: `RLE` for the level streams
    /// (when the column is levelled), `PLAIN` for the dictionary body and any plain/all-null
    /// data pages, and `RLE_DICTIONARY` when a dictionary page is present.
    private List<Encoding> encodings(boolean hasDictionary) {
        List<Encoding> encodings = new ArrayList<>(3);
        if (maxDefLevel > 0 || maxRepLevel > 0) {
            encodings.add(Encoding.RLE);
        }
        encodings.add(Encoding.PLAIN);
        if (hasDictionary) {
            encodings.add(Encoding.RLE_DICTIONARY);
        }
        return encodings;
    }

    private void writeLevels(ByteArrayOutputStream body, int[] levels, int maxLevel) {
        byte[] encoded = LevelEncoder.encode(levels, 0, pendingCount, maxLevel);
        body.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.length).array());
        body.writeBytes(encoded);
    }
}
