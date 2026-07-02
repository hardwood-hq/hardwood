/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
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
import dev.hardwood.schema.ColumnSchema;

/// Decoder for individual Parquet data pages.
///
/// This class provides page decoding via [#decodePage].
/// Page scanning and dictionary parsing are handled by [PageScanner].
public class PageDecoder {

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final DecompressorFactory decompressorFactory;

    /// Whether the fixed-size-list read fast path may engage for this column,
    /// resolved from the reader's [dev.hardwood.HardwoodContext] option (default
    /// enabled).
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
    public Page decodePage(ByteBuffer pageBuffer, Dictionary dictionary) throws IOException {
        PageDecodedEvent event = new PageDecodedEvent();
        event.begin();

        // Parse page header directly from buffer
        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerReader.getBytesRead();

        // Slice the page data (avoids copying)
        int compressedSize = pageHeader.compressedPageSize();
        ByteBuffer pageData = pageBuffer.slice(headerSize, compressedSize);

        if (pageHeader.crc() != null) {
            CrcValidator.assertCorrectCrc(pageHeader.crc(), pageData, column.name());
        }

        Page result = switch (pageHeader.type()) {
            case DATA_PAGE -> {
                Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                yield parseDataPage(pageHeader.dataPageHeader(), uncompressedData, dictionary);
            }
            case DATA_PAGE_V2 -> {
                yield parseDataPageV2(pageHeader.dataPageHeaderV2(), pageData, pageHeader.uncompressedPageSize(), dictionary);
            }
            default -> throw new IOException("Unexpected page type for single-page decode: " + pageHeader.type());
        };

        event.column = column.name();
        event.compressedSize = compressedSize;
        event.uncompressedSize = pageHeader.uncompressedPageSize();
        event.commit();

        return result;
    }

    /// Decode levels using RLE/Bit-Packing Hybrid encoding.
    private int[] decodeRepetitionLevels(byte[] levelData, int offset, int length, int numValues, int maxLevel) {
        int[] levels = new int[numValues];
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
    /// Restricted to flat columns; nested record assembly consumes the def-level
    /// array directly and stays on the materializing path.
    private int[] decodeDefinitionLevels(byte[] levelData, int offset, int length, int numValues) {
        int maxDef = column.maxDefinitionLevel();
        int bitWidth = getBitWidth(maxDef);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(levelData, offset, length, bitWidth);
        // The probe loads the first run; if it is not the all-present fast path,
        // readInts below resumes from that same loaded run on the same instance.
        if (column.maxRepetitionLevel() == 0 && decoder.isSingleRleRunOf(maxDef, numValues)) {
            return null;
        }
        int[] levels = new int[numValues];
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
    /// types, which decode into contiguous primitive arrays the clean-page
    /// assembly can bulk-copy. Byte-array-backed types (`BYTE_ARRAY`,
    /// `FIXED_LEN_BYTE_ARRAY`, `INT96`) take the regular path.
    private boolean isFixedListElementSupported() {
        return switch (column.type()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }

    private Page parseDataPage(DataPageHeader header, byte[] data, Dictionary dictionary) throws IOException {
        int numValues = header.numValues();
        int offset = 0;

        int repLevelLength = 0;
        int repLevelOffset = 0;
        if (column.maxRepetitionLevel() > 0) {
            repLevelLength = ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            offset += 4;
            repLevelOffset = offset;
            offset += repLevelLength;
        }

        int defLevelLength = 0;
        int defLevelOffset = 0;
        if (column.maxDefinitionLevel() > 0) {
            defLevelLength = ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
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
                && column.maxRepetitionLevel() == 1 && column.maxDefinitionLevel() == 2
                && header.repetitionLevelEncoding() == Encoding.RLE
                && header.definitionLevelEncoding() == Encoding.RLE
                && repLevelLength > 0 && defLevelLength > 0) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    data, repLevelOffset, repLevelLength,
                    data, defLevelOffset, defLevelLength,
                    numValues, FixedSizeListDetector.ROWS_UNKNOWN,
                    column.maxRepetitionLevel(), column.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.CleanFixedK(int k)) {
                Page page = decodeTypedValues(
                        header.encoding(), data, valuesOffset, numValues, null, null, dictionary);
                return Page.withFixedListK(page, k);
            }
        }

        int[] repetitionLevels = column.maxRepetitionLevel() > 0
                ? decodeRepetitionLevels(data, repLevelOffset, repLevelLength, numValues, column.maxRepetitionLevel())
                : null;
        int[] definitionLevels = column.maxDefinitionLevel() > 0
                ? decodeDefinitionLevels(data, defLevelOffset, defLevelLength, numValues)
                : null;

        return decodeTypedValues(
                header.encoding(), data, valuesOffset, numValues,
                definitionLevels, repetitionLevels, dictionary);
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            Dictionary dictionary) throws IOException {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.remaining() - valuesOffset;
        int numValues = header.numValues();

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
                && column.maxRepetitionLevel() == 1 && column.maxDefinitionLevel() == 2) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    repLevelData, 0, repLevelLen, defLevelData, 0, defLevelLen,
                    numValues, header.numRows(),
                    column.maxRepetitionLevel(), column.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.CleanFixedK(int k)) {
                byte[] valuesData = readValueRegion(header, pageData, uncompressedPageSize,
                        repLevelLen, defLevelLen, valuesOffset, compressedValuesLen);
                Page page = decodeTypedValues(
                        header.encoding(), valuesData, 0, numValues, null, null, dictionary);
                return Page.withFixedListK(page, k);
            }
        }

        int[] repetitionLevels = repLevelData != null
                ? decodeRepetitionLevels(repLevelData, 0, repLevelLen, numValues, column.maxRepetitionLevel())
                : null;
        int[] definitionLevels = defLevelData != null
                ? decodeDefinitionLevels(defLevelData, 0, defLevelLen, numValues)
                : null;

        byte[] valuesData = readValueRegion(header, pageData, uncompressedPageSize,
                repLevelLen, defLevelLen, valuesOffset, compressedValuesLen);
        return decodeTypedValues(
                header.encoding(), valuesData, 0, numValues,
                definitionLevels, repetitionLevels, dictionary);
    }

    /// Extracts the value region of a `DataPageV2` body, decompressing it when
    /// the page marks its values compressed. The level regions precede the
    /// values and are never compressed.
    private byte[] readValueRegion(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            int repLevelLen, int defLevelLen, int valuesOffset, int compressedValuesLen) throws IOException {
        if (header.isCompressed() && compressedValuesLen > 0) {
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
    private Page decodeTypedValues(Encoding encoding, byte[] data, int offset,
                                   int numValues,
                                   int[] definitionLevels, int[] repetitionLevels,
                                   Dictionary dictionary) throws IOException {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

        // Try to decode into primitive arrays for supported type/encoding combinations
        switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(data, offset, type, column.typeLength());
                return switch (type) {
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
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(data, offset);
                return switch (type) {
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
                    default -> throw new UnsupportedOperationException(
                            "DELTA_BINARY_PACKED not supported for type: " + type);
                };
            }
            case BYTE_STREAM_SPLIT -> {
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        data, offset, numNonNullValues, type, column.typeLength());
                return switch (type) {
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
                    default -> throw new UnsupportedOperationException(
                            "BYTE_STREAM_SPLIT not supported for type: " + type);
                };
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new IOException("Dictionary page not found for " + encoding + " encoding");
                }
                int bitWidth = data[offset++] & 0xFF;
                if (bitWidth > 32) {
                    throw new IOException("Invalid dictionary index bit width: " + bitWidth
                            + " for column '" + column.name() + "'. Must be between 0 and 32");
                }
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(data, offset, data.length - offset, bitWidth);

                return dictionary.decodePage(indexDecoder, numValues, definitionLevels, repetitionLevels, maxDefLevel);
            }
            case RLE -> {
                // RLE encoding for boolean values uses bit-width of 1
                if (type != PhysicalType.BOOLEAN) {
                    throw new UnsupportedOperationException(
                            "RLE encoding for non-boolean types not yet supported: " + type);
                }

                // Read 4-byte length prefix (little-endian)
                int rleLength = ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
                offset += 4;

                RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(data, offset, rleLength, 1);
                boolean[] values = new boolean[numValues];
                decoder.readBooleans(values, definitionLevels, maxDefLevel);
                return new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(data, offset);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                return new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_BYTE_ARRAY -> {
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(data, offset);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                return new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            default -> throw new UnsupportedOperationException("Encoding not yet supported: " + encoding);
        }
    }
}
