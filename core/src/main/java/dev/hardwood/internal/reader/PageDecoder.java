/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.encoding.ByteStreamSplitDecoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.internal.encoding.DeltaByteArrayDecoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayDecoder;
import dev.hardwood.internal.encoding.PlainDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageDecodedEvent;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Decoder for individual Parquet data pages.
///
/// This class provides page decoding via [#decodePage].
/// Page location is handled by the [FetchPlan] implementations, dictionary
/// parsing by [DictionaryParser].
public class PageDecoder {

    /// Reusable level-decoding buffers owned by one in-flight page slot.
    static final class LevelScratch {
        private int[] repetitionLevels;
        private int[] definitionLevels;

        int[] repetitionLevels(int size) {
            if (repetitionLevels == null || repetitionLevels.length < size) {
                repetitionLevels = new int[size];
            }
            return repetitionLevels;
        }

        int[] definitionLevels(int size) {
            if (definitionLevels == null || definitionLevels.length < size) {
                definitionLevels = new int[size];
            }
            return definitionLevels;
        }
    }

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final DecompressorFactory decompressorFactory;

    /// Whether the fixed-size-list read fast path may engage for this column,
    /// resolved from the reader's [dev.hardwood.reader.ReaderConfig] option
    /// (default disabled).
    private final boolean fixedListFastPathEnabled;

    /// Constructor for page decoding, with the fixed-size-list fast path enabled.
    public PageDecoder(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory) {
        this(columnMetaData, column, decompressorFactory, true);
    }

    /// Constructor for page decoding.
    ///
    /// @param columnMetaData metadata for the column
    /// @param column column schema
    /// @param decompressorFactory factory for creating decompressors
    /// @param fixedListFastPathEnabled whether the fixed-size-list fast path may engage
    public PageDecoder(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory,
                       boolean fixedListFastPathEnabled) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
    }

    /// Checks if this PageDecoder is compatible with the given column metadata.
    /// Used for cross-file prefetching to determine if PageDecoder can be reused.
    ///
    /// @param otherMetaData the column metadata to check against
    /// @return true if compatible (same codec), false otherwise
    public boolean isCompatibleWith(ColumnMetaData otherMetaData) {
        return columnMetaData.codec() == otherMetaData.codec();
    }

    /// Gets the decompressor factory used by this PageDecoder.
    ///
    /// @return the decompressor factory
    public DecompressorFactory getDecompressorFactory() {
        return decompressorFactory;
    }

    /// Produces an all-null typed [Page] of the given size, without reading or
    /// decompressing any data. Used when inline page statistics have proven that
    /// no row in the page can match the active filter predicate — row alignment
    /// with sibling columns is preserved while decompression and value decoding
    /// are skipped entirely. The row-level filter drops the synthetic nulls via
    /// SQL three-valued logic (`null <op> x → unknown → non-match`).
    ///
    /// Only valid for columns where `maxDefinitionLevel > 0`; for required
    /// columns the caller must not skip the page.
    public Page nullPage(int numValues) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (maxDefLevel == 0) {
            throw new IllegalStateException("Cannot create null placeholder page for required column '"
                    + column.name() + "' — maxDefinitionLevel is 0");
        }
        int[] definitionLevels = new int[numValues]; // zero-initialised → all null
        int[] repetitionLevels = column.maxRepetitionLevel() > 0 ? new int[numValues] : null;
        PhysicalType type = column.type();
        return switch (type) {
            case INT64 -> new Page.LongPage(new long[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case DOUBLE -> new Page.DoublePage(new double[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case INT32 -> new Page.IntPage(new int[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case FLOAT -> new Page.FloatPage(new float[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case BOOLEAN -> new Page.BooleanPage(new boolean[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    new Page.ByteArrayPage(new byte[numValues][], definitionLevels, repetitionLevels, maxDefLevel, numValues);
        };
    }

    /// Decode a single data page from a buffer.
    ///
    /// The buffer should contain the complete page including header.
    ///
    /// @param pageBuffer buffer containing just this page (header + data)
    /// @param dictionary dictionary for this page, or null if not dictionary-encoded
    /// @return decoded page
    public Page decodePage(ByteBuffer pageBuffer, Dictionary dictionary) {
        // Standalone callers have no reorder slot to reuse across, so a throwaway
        // scratch (allocated fresh here, buffers grown lazily) matches the old
        // per-call allocation while keeping the decode path free of null handling.
        return decodePage(pageBuffer, dictionary, new LevelScratch());
    }

    /// Decode a single data page, reusing the supplied slot-owned level scratch.
    /// `scratch` must not be null; standalone callers pass a throwaway instance via
    /// the two-argument overload.
    Page decodePage(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch) {
        return decodeOpened(open(pageBuffer), dictionary, scratch);
    }

    /// A page whose header has been read and whose body CRC, when present, has
    /// been checked. Both decode paths start from one of these so the header is
    /// parsed once whichever path the page takes.
    record OpenedPage(PageHeader header, ByteBuffer pageData) {}

    /// Reads the page header and checks the body CRC.
    OpenedPage open(ByteBuffer pageBuffer) {
        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        ByteBuffer pageData = pageBuffer.slice(headerReader.getBytesRead(), pageHeader.compressedPageSize());
        if (pageHeader.crc() != null) {
            CrcValidator.assertCorrectCrc(pageHeader.crc(), pageData);
        }
        return new OpenedPage(pageHeader, pageData);
    }

    /// Materializes a page whose header [#open] already read.
    Page decodeOpened(OpenedPage opened, Dictionary dictionary, LevelScratch scratch) {
        PageDecodedEvent event = new PageDecodedEvent();
        event.begin();

        PageHeader pageHeader = opened.header();
        ByteBuffer pageData = opened.pageData();
        Page result = switch (pageHeader.type()) {
            case DATA_PAGE -> {
                Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
                int uncompressedSize = pageHeader.uncompressedPageSize();
                byte[] uncompressedData = decompressor.decompress(pageData, uncompressedSize);
                yield parseDataPage(pageHeader.dataPageHeader(), uncompressedData, uncompressedSize, dictionary,
                        scratch);
            }
            case DATA_PAGE_V2 -> {
                yield parseDataPageV2(pageHeader.dataPageHeaderV2(), pageData,
                        pageHeader.uncompressedPageSize(), dictionary, scratch);
            }
            default -> throw new ParquetReadException(
                    "Unexpected page type for single-page decode: " + pageHeader.type());
        };

        event.column = column.name();
        event.compressedSize = pageHeader.compressedPageSize();
        event.uncompressedSize = pageHeader.uncompressedPageSize();
        event.commit();
        return result;
    }

    /// Whether `type` can be written straight into a batch array. Boolean and
    /// byte-array columns stay on the materialized-page path.
    static boolean isDirectlyDecodableType(PhysicalType type) {
        return switch (type) {
            case INT32, INT64, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }

    /// Whether this page's encoding is one the cursor path reads. The column
    /// type and the row mask are the caller's to have already decided; this is
    /// only the fact that lives in the header.
    boolean directEncoding(OpenedPage opened) {
        Encoding encoding = switch (opened.header().type()) {
            case DATA_PAGE -> opened.header().dataPageHeader().encoding();
            case DATA_PAGE_V2 -> opened.header().dataPageHeaderV2().encoding();
            default -> null;
        };
        return encoding == Encoding.PLAIN || encoding == Encoding.BYTE_STREAM_SPLIT;
    }

    /// Fills `cursor` from a page [#directEncoding] accepted. Decompresses the
    /// value bytes and records definition levels; does not build a [Page].
    void fillCursor(OpenedPage opened, PageValueCursor cursor) {
        PageDecodedEvent event = new PageDecodedEvent();
        event.begin();
        switch (opened.header().type()) {
            case DATA_PAGE -> fillV1Cursor(opened.header(), opened.pageData(), cursor);
            case DATA_PAGE_V2 -> fillV2Cursor(opened.header(), opened.pageData(), cursor);
            default -> throw new IllegalStateException(
                    "fillCursor called for page type " + opened.header().type());
        }
        commitDirectEvent(event, opened.header().compressedPageSize(), opened.header().uncompressedPageSize());
    }

    /// V1 DATA_PAGE direct-into-cursor path.
    ///
    /// V1 layout: the entire body (rep-levels + def-levels + values) is compressed
    /// as a single blob. Level sections are length-prefixed with inline 4-byte
    /// integers. The caller has already accepted the encoding and checked the CRC.
    private void fillV1Cursor(PageHeader pageHeader, ByteBuffer pageData, PageValueCursor cursor) {
        DataPageHeader dataHeader = pageHeader.dataPageHeader();
        Encoding enc = dataHeader.encoding();
        int uncompressedSize = pageHeader.uncompressedPageSize();

        Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
        byte[] uncompressed = decompressor.decompress(pageData, uncompressedSize);

        int numValues = dataHeader.numValues();
        int offset = 0;

        // Skip repetition level stream
        if (column.maxRepetitionLevel() > 0) {
            int repLen = readSectionLength(uncompressed, offset, uncompressedSize, "repetition level");
            offset += 4 + repLen;
        }

        // Probe definition level stream
        if (column.maxDefinitionLevel() > 0) {
            int defLen = readSectionLength(uncompressed, offset, uncompressedSize, "definition level");
            offset += 4;
            // Check for all-present fast path (single RLE run of maxDef)
            int maxDef = column.maxDefinitionLevel();
            int bitWidth = getBitWidth(maxDef);
            RleBitPackingHybridDecoder probe =
                    new RleBitPackingHybridDecoder(
                            uncompressed, offset, defLen, bitWidth);
            if (probe.isSingleRleRunOf(maxDef, numValues)) {
                // All-present: decoders see a null level array. The slot buffer stays.
                cursor.definitionLevelsActive = false;
                cursor.nonNullsLeft = numValues;
            } else {
                // Mixed nulls: decode the full def-level stream into the cursor.
                // isSingleRleRunOf restores the probe, but a second decoder keeps
                // that restoration from being load-bearing here.
                cursor.ensureDefLevels(numValues);
                cursor.definitionLevelsActive = true;
                RleBitPackingHybridDecoder fullDecoder =
                        new RleBitPackingHybridDecoder(
                                uncompressed, offset, defLen, bitWidth);
                fullDecoder.readInts(cursor.definitionLevels, 0, numValues);
                cursor.defLevelPos = 0;
                cursor.nonNullsLeft = countNonNull(cursor.definitionLevels, numValues, maxDef);
            }
            offset += defLen;
        } else {
            cursor.definitionLevelsActive = false;
            cursor.nonNullsLeft = numValues;
        }
        // offset now points at the first value byte

        // Fill cursor
        cursor.copyData(uncompressed, uncompressedSize);
        cursor.srcPos = offset;
        cursor.srcLimit = uncompressedSize;
        cursor.valuesLeft = numValues;
        cursor.encoding = enc;
        // BSS straddle tracking: record the stream base and total non-null values
        if (enc == Encoding.BYTE_STREAM_SPLIT) {
            cursor.bssBaseOffset = offset;
            cursor.bssTotalValues = cursor.nonNullsLeft;
            cursor.bssCurrentIndex = 0;
        }

    }

    /// V2 DATA_PAGE_V2 direct-into-cursor path.
    ///
    /// V2 layout: rep-levels and def-levels are stored raw (uncompressed) at the
    /// start of the body, with their byte lengths declared in the header. Only
    /// the value region is optionally compressed. The header's `numNulls` field
    /// provides a free all-present check — no need to probe the RLE stream.
    /// The caller has already accepted the encoding and checked the CRC.
    private void fillV2Cursor(PageHeader pageHeader, ByteBuffer pageData, PageValueCursor cursor) {
        DataPageHeaderV2 v2Header = pageHeader.dataPageHeaderV2();
        Encoding enc = v2Header.encoding();
        int uncompressedSize = pageHeader.uncompressedPageSize();

        int numValues = v2Header.numValues();
        int repLevelLen = v2Header.repetitionLevelsByteLength();
        int defLevelLen = v2Header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.remaining() - valuesOffset;

        // Decode definition levels: V2 levels are raw (uncompressed) in pageData
        if (v2Header.numNulls() != 0) {
            // Nullable page: decode the def-level stream into the cursor
            byte[] defBytes = new byte[defLevelLen];
            pageData.slice(repLevelLen, defLevelLen).get(defBytes);
            int maxDef = column.maxDefinitionLevel();
            int bitWidth = getBitWidth(maxDef);
            cursor.ensureDefLevels(numValues);
            cursor.definitionLevelsActive = true;
            RleBitPackingHybridDecoder decoder =
                    new RleBitPackingHybridDecoder(defBytes, 0, defLevelLen, bitWidth);
            decoder.readInts(cursor.definitionLevels, 0, numValues);
            cursor.defLevelPos = 0;
            cursor.nonNullsLeft = numValues - v2Header.numNulls();
        } else {
            // All-present: decoders see a null level array. The slot buffer stays.
            cursor.definitionLevelsActive = false;
            cursor.nonNullsLeft = numValues;
        }

        // Decompress only the value region (levels are stored raw in V2)
        byte[] valueBytes;
        int valuesLen;
        if (isValueRegionCompressed(v2Header, compressedValuesLen)) {
            ByteBuffer compressedValues = pageData.slice(valuesOffset, compressedValuesLen);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            valuesLen = uncompressedSize - repLevelLen - defLevelLen;
            valueBytes = decompressor.decompress(compressedValues, valuesLen);
        } else {
            valuesLen = compressedValuesLen;
            valueBytes = new byte[compressedValuesLen];
            pageData.slice(valuesOffset, compressedValuesLen).get(valueBytes);
        }

        // Fill cursor — values start at offset 0 because the buffer contains
        // only the value region (no level prefix as in V1)
        cursor.copyData(valueBytes, valuesLen);
        cursor.srcPos = 0;
        cursor.srcLimit = valuesLen;
        cursor.valuesLeft = numValues;
        cursor.encoding = enc;
        // BSS straddle tracking: record the stream base and total non-null values
        if (enc == Encoding.BYTE_STREAM_SPLIT) {
            cursor.bssBaseOffset = 0;
            cursor.bssTotalValues = cursor.nonNullsLeft;
            cursor.bssCurrentIndex = 0;
        }

    }

    /// Emit a JFR event for a successful direct-into-cursor decode.
    /// `event` was begun before decompression, so its duration covers the
    /// header work already done by [#open] plus decompression and level decode.
    /// Value decode into the batch happens later, on the drain.
    private void commitDirectEvent(PageDecodedEvent event, int compressedSize, int uncompressedSize) {
        event.column = column.name();
        event.compressedSize = compressedSize;
        event.uncompressedSize = uncompressedSize;
        event.commit();
    }

    /// Count values where {@code defLevels[i] == maxDef}.  Returns
    /// {@code numValues} when {@code defLevels} is {@code null}
    /// (all-present convention).
    private static int countNonNull(int[] defLevels, int numValues, int maxDef) {
        if (defLevels == null) {
            return numValues;
        }
        int count = 0;
        for (int i = 0; i < numValues; i++) {
            if (defLevels[i] == maxDef) {
                count++;
            }
        }
        return count;
    }

    /// Decode levels using RLE/Bit-Packing Hybrid encoding.
    private int[] decodeRepetitionLevels(byte[] levelData, int offset, int length, int numValues, int maxLevel,
            LevelScratch scratch) {
        int[] levels = scratch.repetitionLevels(numValues);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(levelData, offset, length, getBitWidth(maxLevel));
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /// Decode definition levels, applying the all-present fast path: when the
    /// stream is a single RLE run of `maxDef` (the common case for an optional
    /// but fully-populated column), skip materializing the per-value level array
    /// and represent "all present" as a `null` level array — the same
    /// representation used for required columns throughout the reader.
    ///
    /// Applies to flat and nested columns alike: nested assembly treats a `null`
    /// definition-level array as all-present (every leaf at `maxDef`) and takes a
    /// whole-page bulk-copy path; the repetition levels are still materialised, so
    /// record boundaries are preserved.
    private int[] decodeDefinitionLevels(byte[] levelData, int offset, int length, int numValues,
            LevelScratch scratch) {
        int maxDef = column.maxDefinitionLevel();
        int bitWidth = getBitWidth(maxDef);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(levelData, offset, length, bitWidth);
        // The probe loads the first run; if it is not the all-present fast path,
        // readInts below resumes from that same loaded run on the same instance.
        if (decoder.isSingleRleRunOf(maxDef, numValues)) {
            return null;
        }
        int[] levels = scratch.definitionLevels(numValues);
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /// Count non-null values based on definition levels.
    private int countNonNullValues(int numValues, int[] definitionLevels) {
        if (definitionLevels == null) {
            return numValues;
        }
        int maxDefLevel = column.maxDefinitionLevel();
        int count = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels[i] == maxDefLevel) {
                count++;
            }
        }
        return count;
    }

    private int getBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }

    /// The fixed-size-list fast path is restricted to primitive numeric element
    /// types, which decode into contiguous primitive arrays the fixed-width
    /// assembly can bulk-copy. Byte-array-backed types (`BYTE_ARRAY`,
    /// `FIXED_LEN_BYTE_ARRAY`, `INT96`) take the regular path.
    private boolean isFixedListElementSupported() {
        return switch (column.type()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }

    /// Whether the column's level geometry is a single-level `LIST` the fast path
    /// can target: `maxRep == 1` with either an optional list (`maxDef == 2`) or a
    /// required list of required elements (`maxDef == 1`).
    ///
    /// The required case is admitted where the leaf sits under a repeated group, so
    /// its own repetition type is not `REPEATED`: an annotated `LIST` group, or a
    /// bare repeated group with a required primitive child. A bare unannotated
    /// `repeated <primitive>` shares the same `maxRep == 1` / `maxDef == 1` levels
    /// but is left to the regular path.
    private boolean hasFixedListLevelShape() {
        if (column.maxRepetitionLevel() != 1) {
            return false;
        }
        return switch (column.maxDefinitionLevel()) {
            case 2 -> true;
            case 1 -> column.repetitionType() != RepetitionType.REPEATED;
            default -> false;
        };
    }

    /// @param data the decompressed page body, valid up to `limit`; the buffer may run on past it
    ///        with bytes of an earlier page
    /// @param limit the length of the page body
    private Page parseDataPage(DataPageHeader header, byte[] data, int limit, Dictionary dictionary,
            LevelScratch scratch) {
        int numValues = header.numValues();
        int offset = 0;

        int repLevelLength = 0;
        int repLevelOffset = 0;
        if (column.maxRepetitionLevel() > 0) {
            repLevelLength = readSectionLength(data, offset, limit, "repetition level");
            offset += 4;
            repLevelOffset = offset;
            offset += repLevelLength;
        }

        int defLevelLength = 0;
        int defLevelOffset = 0;
        if (column.maxDefinitionLevel() > 0) {
            defLevelLength = readSectionLength(data, offset, limit, "definition level");
            offset += 4;
            defLevelOffset = offset;
            offset += defLevelLength;
        }
        int valuesOffset = offset;

        // Fixed-size-list fast path: the V1 header carries no num_rows, so the
        // detector derives it. The levels are inline and (unlike V2) always
        // present here in decompressed form; the detector only understands the
        // RLE hybrid, so legacy BIT_PACKED levels are left to the regular path.
        if (fixedListFastPathEnabled && isFixedListElementSupported()
                && hasFixedListLevelShape()
                && header.repetitionLevelEncoding() == Encoding.RLE
                && header.definitionLevelEncoding() == Encoding.RLE
                && repLevelLength > 0 && defLevelLength > 0) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    data, repLevelOffset, repLevelLength,
                    data, defLevelOffset, defLevelLength,
                    numValues, FixedSizeListDetector.ROWS_UNKNOWN,
                    column.maxRepetitionLevel(), column.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.FixedWidth(int k)) {
                Page page = decodeTypedValues(
                        header.encoding(), header.encodingValue(),
                        data, valuesOffset, limit, numValues, null, null, dictionary);
                return Page.withFixedListK(page, k);
            }
        }

        int[] repetitionLevels = column.maxRepetitionLevel() > 0
                ? decodeRepetitionLevels(data, repLevelOffset, repLevelLength, numValues,
                        column.maxRepetitionLevel(), scratch)
                : null;
        int[] definitionLevels = column.maxDefinitionLevel() > 0
                ? decodeDefinitionLevels(data, defLevelOffset, defLevelLength, numValues, scratch)
                : null;

        return decodeTypedValues(
                header.encoding(), header.encodingValue(), data, valuesOffset, limit, numValues,
                definitionLevels, repetitionLevels, dictionary);
    }

    /// Reads the 4-byte little-endian length prefix of a section of `data` at `offset`, and checks
    /// that the section it announces ends within the page.
    private static int readSectionLength(byte[] data, int offset, int limit, String section) {
        if (Integer.BYTES > limit - offset) {
            throw new ParquetReadException("Unexpected EOF reading the " + section + " length");
        }
        int length = ByteBuffer.wrap(data, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int remaining = limit - offset - Integer.BYTES;
        if (length < 0 || length > remaining) {
            throw new ParquetReadException("Invalid " + section + " length " + length + ": "
                    + remaining + " bytes remain in the page");
        }
        return length;
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            Dictionary dictionary, LevelScratch scratch) {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.remaining() - valuesOffset;
        int numValues = header.numValues();
        int valuesLimit = isValueRegionCompressed(header, compressedValuesLen)
                ? uncompressedPageSize - repLevelLen - defLevelLen
                : compressedValuesLen;

        byte[] repLevelData = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            repLevelData = new byte[repLevelLen];
            pageData.slice(0, repLevelLen).get(repLevelData);
        }

        byte[] defLevelData = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            defLevelData = new byte[defLevelLen];
            pageData.slice(repLevelLen, defLevelLen).get(defLevelData);
        }

        // Fixed-size-list fast path: when the level streams prove every row is a
        // present list of exactly k elements, skip level materialization and
        // decode only the values, stamping the shape onto the page. The regular
        // value decoders already read densely from a null definition-level array
        // (the all-present convention), so no value-decode change is needed.
        if (fixedListFastPathEnabled && isFixedListElementSupported()
                && repLevelData != null && defLevelData != null
                && hasFixedListLevelShape()) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    repLevelData, 0, repLevelLen, defLevelData, 0, defLevelLen,
                    numValues, header.numRows(),
                    column.maxRepetitionLevel(), column.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.FixedWidth(int k)) {
                byte[] valuesData = readValueRegion(header, pageData, uncompressedPageSize,
                        repLevelLen, defLevelLen, valuesOffset, compressedValuesLen);
                Page page = decodeTypedValues(
                        header.encoding(), header.encodingValue(),
                        valuesData, 0, valuesLimit, numValues, null, null, dictionary);
                return Page.withFixedListK(page, k);
            }
        }

        int[] repetitionLevels = repLevelData != null
                ? decodeRepetitionLevels(repLevelData, 0, repLevelLen, numValues,
                        column.maxRepetitionLevel(), scratch)
                : null;
        int[] definitionLevels = defLevelData != null
                ? decodeDefinitionLevels(defLevelData, 0, defLevelLen, numValues, scratch)
                : null;

        byte[] valuesData = readValueRegion(header, pageData, uncompressedPageSize,
                repLevelLen, defLevelLen, valuesOffset, compressedValuesLen);
        return decodeTypedValues(
                header.encoding(), header.encodingValue(), valuesData, 0, valuesLimit, numValues,
                definitionLevels, repetitionLevels, dictionary);
    }

    /// Whether the value region of a `DataPageV2` body is stored compressed.
    private static boolean isValueRegionCompressed(DataPageHeaderV2 header, int compressedValuesLen) {
        return header.isCompressed() && compressedValuesLen > 0;
    }

    /// Extracts the value region of a `DataPageV2` body, decompressing it when
    /// the page marks its values compressed. The level regions precede the
    /// values and are never compressed.
    ///
    /// A decompressed region is valid up to `uncompressedPageSize - repLevelLen - defLevelLen`
    /// only, the buffer it comes in running on past that with bytes of an earlier page; a stored
    /// one is exactly as long as the region.
    private byte[] readValueRegion(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            int repLevelLen, int defLevelLen, int valuesOffset, int compressedValuesLen) {
        if (isValueRegionCompressed(header, compressedValuesLen)) {
            ByteBuffer compressedValues = pageData.slice(valuesOffset, compressedValuesLen);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;
            return decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        byte[] valuesData = new byte[compressedValuesLen];
        pageData.slice(valuesOffset, compressedValuesLen).get(valuesData);
        return valuesData;
    }

    /// Decode values into Page using primitive arrays where possible.
    ///
    /// @param data the bytes holding the values, which may run on past `limit` with bytes of an
    ///        earlier page
    /// @param offset the position in `data` the values start at
    /// @param limit the position in `data` the values end at, exclusive
    private Page decodeTypedValues(Encoding encoding, int encodingValue, byte[] data, int offset, int limit,
                                   int numValues,
                                   int[] definitionLevels, int[] repetitionLevels,
                                   Dictionary dictionary) {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

        // Try to decode into primitive arrays for supported type/encoding combinations
        return switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(data, offset, limit, type, column.typeLength());
                yield switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FLOAT -> {
                        float[] values = new float[numValues];
                        decoder.readFloats(values, definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case BOOLEAN -> {
                        boolean[] values = new boolean[numValues];
                        decoder.readBooleans(values, definitionLevels, maxDefLevel);
                        yield new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                        byte[][] values = new byte[numValues][];
                        decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            case DELTA_BINARY_PACKED -> {
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(data, offset, limit);
                yield switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    default -> throw undefinedOverType(Encoding.DELTA_BINARY_PACKED, type,
                            PhysicalType.INT32, PhysicalType.INT64);
                };
            }
            case BYTE_STREAM_SPLIT -> {
                // The decoder derives its byte width from the physical type, so an undefined pair
                // has to be refused before it is constructed — otherwise the width lookup, and not
                // the format check, is what the file fails on.
                if (type == PhysicalType.BOOLEAN || type == PhysicalType.BYTE_ARRAY
                        || type == PhysicalType.INT96) {
                    throw byteStreamSplitUndefinedOverType(type);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        data, offset, limit, numNonNullValues, type, column.typeLength());
                yield switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FLOAT -> {
                        float[] values = new float[numValues];
                        decoder.readFloats(values, definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FIXED_LEN_BYTE_ARRAY -> {
                        byte[][] values = new byte[numValues][];
                        decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    default -> throw byteStreamSplitUndefinedOverType(type);
                };
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new ParquetReadException("Dictionary page not found for " + encoding + " encoding");
                }
                if (offset >= limit) {
                    throw new ParquetReadException("Unexpected EOF reading the dictionary index bit width");
                }
                int bitWidth = data[offset++] & 0xFF;
                if (bitWidth > 32) {
                    throw new ParquetReadException("Invalid dictionary index bit width: " + bitWidth
                            + ". Must be between 0 and 32");
                }
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(data, offset, limit - offset, bitWidth);

                yield dictionary.decodePage(indexDecoder, numValues, definitionLevels, repetitionLevels, maxDefLevel);
            }
            case RLE -> {
                // In the value position RLE carries booleans and nothing else; the format also
                // gives it the level streams and dictionary indices, which do not come through
                // here. Refusing another type is the format's position, not a decoder this
                // release has yet to write.
                if (type != PhysicalType.BOOLEAN) {
                    throw new ParquetReadException(
                            "RLE encodes only boolean values in a data page, not " + type);
                }

                // Read 4-byte length prefix (little-endian)
                int rleLength = readSectionLength(data, offset, limit, "RLE boolean values");
                offset += 4;

                RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(data, offset, rleLength, 1);
                boolean[] values = new boolean[numValues];
                decoder.readBooleans(values, definitionLevels, maxDefLevel);
                yield new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                // The lengths are delta-encoded ahead of the bytes, so the values are byte arrays
                // whatever the column claims; decoding an integer column's page this way would
                // build a page of the wrong shape rather than fail.
                if (type != PhysicalType.BYTE_ARRAY) {
                    throw undefinedOverType(Encoding.DELTA_LENGTH_BYTE_ARRAY, type,
                            PhysicalType.BYTE_ARRAY);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(data, offset, limit);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_BYTE_ARRAY -> {
                if (type != PhysicalType.BYTE_ARRAY && type != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    throw undefinedOverType(Encoding.DELTA_BYTE_ARRAY, type,
                            PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(data, offset, limit);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            // BIT_PACKED encodes levels, never a page's values, so refusing it is the format's
            // position rather than a decoder this release has yet to write.
            case BIT_PACKED -> throw new ParquetReadException(
                    "BIT_PACKED encodes levels and is not valid for a data page's values");
            // UNKNOWN stands for every Thrift value this release cannot name, so on its own it
            // does not say which encoding was met; the raw value does. This is the one refusal
            // here that is a gap in this release rather than a malformed file, so it is the one
            // that keeps UnsupportedOperationException.
            case UNKNOWN -> throw new UnsupportedOperationException(
                    "Encoding not yet supported: UNKNOWN (Thrift encoding value " + encodingValue + ")");
        };
    }

    /// Refusal for a page whose declared encoding is not defined over its column's physical type.
    ///
    /// The format names the physical types each encoding may carry, so such a page is a malformed
    /// file rather than a gap in this release — which is why this is a [ParquetReadException] and
    /// not the [UnsupportedOperationException] an unrecognized encoding raises. Decoding it anyway would
    /// build a page of the wrong shape rather than fail: wrong in the values, and wrong in the
    /// [Page] variant handed to the column reader.
    ///
    /// @param encoding the encoding the page declares
    /// @param type the column's physical type
    /// @param definedOver the physical types the format defines `encoding` over
    /// @return the exception to throw
    private static ParquetReadException undefinedOverType(Encoding encoding, PhysicalType type,
            PhysicalType... definedOver) {
        StringBuilder legalTypes = new StringBuilder();
        for (PhysicalType candidate : definedOver) {
            if (!legalTypes.isEmpty()) {
                legalTypes.append(", ");
            }
            legalTypes.append(candidate);
        }
        return new ParquetReadException(encoding + " is not defined over " + type
                + "; the format defines it over " + legalTypes + " only");
    }

    /// [#undefinedOverType] for `BYTE_STREAM_SPLIT`, whose legal types are named both before the
    /// decoder is constructed and by the value switch that follows it.
    private static ParquetReadException byteStreamSplitUndefinedOverType(PhysicalType type) {
        return undefinedOverType(Encoding.BYTE_STREAM_SPLIT, type, PhysicalType.INT32,
                PhysicalType.INT64, PhysicalType.FLOAT, PhysicalType.DOUBLE,
                PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }
}
