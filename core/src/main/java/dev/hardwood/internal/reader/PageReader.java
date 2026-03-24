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
public class PageReader {

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final DecompressorFactory decompressorFactory;

    /// Constructor for page decoding.
    ///
    /// @param columnMetaData metadata for the column
    /// @param column column schema
    /// @param decompressorFactory factory for creating decompressors
    public PageReader(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
    }

    /// Checks if this PageReader is compatible with the given column metadata.
    /// Used for cross-file prefetching to determine if PageReader can be reused.
    ///
    /// @param otherMetaData the column metadata to check against
    /// @return true if compatible (same codec), false otherwise
    public boolean isCompatibleWith(ColumnMetaData otherMetaData) {
        return columnMetaData.codec() == otherMetaData.codec();
    }

    /// Gets the decompressor factory used by this PageReader.
    ///
    /// @return the decompressor factory
    public DecompressorFactory getDecompressorFactory() {
        return decompressorFactory;
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
    private int[] decodeLevels(byte[] levelData, int offset, int length, int numValues, int maxLevel) {
        int[] levels = new int[numValues];
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(levelData, offset, length, getBitWidth(maxLevel));
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

    private Page parseDataPage(DataPageHeader header, byte[] data, Dictionary dictionary) throws IOException {
        int offset = 0;

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            int repLevelLength = ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            offset += 4;
            repetitionLevels = decodeLevels(data, offset, repLevelLength, header.numValues(), column.maxRepetitionLevel());
            offset += repLevelLength;
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0) {
            int defLevelLength = ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            offset += 4;
            definitionLevels = decodeLevels(data, offset, defLevelLength, header.numValues(), column.maxDefinitionLevel());
            offset += defLevelLength;
        }

        return decodeTypedValues(
                header.encoding(), data, offset, header.numValues(),
                definitionLevels, repetitionLevels, dictionary);
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            Dictionary dictionary) throws IOException {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.remaining() - valuesOffset;

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            byte[] repLevelData = new byte[repLevelLen];
            pageData.slice(0, repLevelLen).get(repLevelData);
            repetitionLevels = decodeLevels(repLevelData, 0, repLevelLen, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            byte[] defLevelData = new byte[defLevelLen];
            pageData.slice(repLevelLen, defLevelLen).get(defLevelData);
            definitionLevels = decodeLevels(defLevelData, 0, defLevelLen, header.numValues(), column.maxDefinitionLevel());
        }

        byte[] valuesData;
        int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;

        if (header.isCompressed() && compressedValuesLen > 0) {
            ByteBuffer compressedValues = pageData.slice(valuesOffset, compressedValuesLen);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            valuesData = decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        else {
            valuesData = new byte[compressedValuesLen];
            pageData.slice(valuesOffset, compressedValuesLen).get(valuesData);
        }

        return decodeTypedValues(
                header.encoding(), valuesData, 0, header.numValues(),
                definitionLevels, repetitionLevels, dictionary);
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
