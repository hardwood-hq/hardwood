/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

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
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/**
 * Decoder for individual Parquet data pages.
 * <p>
 * This class provides page decoding via {@link #decodePage}.
 * Page scanning and dictionary parsing are handled by {@link PageScanner}.
 * </p>
 */
public class PageReader {

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final DecompressorFactory decompressorFactory;

    /**
     * Constructor for page decoding.
     *
     * @param columnMetaData metadata for the column
     * @param column column schema
     * @param decompressorFactory factory for creating decompressors
     */
    public PageReader(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
    }

    /**
     * Checks if this PageReader is compatible with the given column metadata.
     * Used for cross-file prefetching to determine if PageReader can be reused.
     *
     * @param otherMetaData the column metadata to check against
     * @return true if compatible (same codec), false otherwise
     */
    public boolean isCompatibleWith(ColumnMetaData otherMetaData) {
        return columnMetaData.codec() == otherMetaData.codec();
    }

    /**
     * Gets the decompressor factory used by this PageReader.
     *
     * @return the decompressor factory
     */
    public DecompressorFactory getDecompressorFactory() {
        return decompressorFactory;
    }

    /**
     * Decode a single data page from a buffer.
     * <p>
     * The buffer should contain the complete page including header.
     * </p>
     *
     * @param pageBuffer buffer containing just this page (header + data)
     * @param dictionary dictionary for this page, or null if not dictionary-encoded
     * @return decoded page
     */
    public Page decodePage(MappedByteBuffer pageBuffer, Dictionary dictionary) throws IOException {
        // Parse page header directly from buffer
        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerReader.getBytesRead();

        // Slice the page data (avoids copying)
        int compressedSize = pageHeader.compressedPageSize();
        MappedByteBuffer pageData = pageBuffer.slice(headerSize, compressedSize);

        return switch (pageHeader.type()) {
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
    }

    /**
     * Decode levels using RLE/Bit-Packing Hybrid encoding.
     */
    private int[] decodeLevels(byte[] levelData, int numValues, int maxLevel) throws IOException {
        int[] levels = new int[numValues];
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                new ByteArrayInputStream(levelData), getBitWidth(maxLevel));
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /**
     * Count non-null values based on definition levels.
     */
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

    /**
     * Read a 4-byte little-endian integer from the stream.
     */
    private int readLittleEndianInt(InputStream stream) throws IOException {
        byte[] bytes = new byte[4];
        stream.read(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private int getBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }

    private Page parseDataPage(DataPageHeader header, byte[] data, Dictionary dictionary) throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            int repLevelLength = readLittleEndianInt(dataStream);
            byte[] repLevelData = new byte[repLevelLength];
            dataStream.read(repLevelData);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0) {
            int defLevelLength = readLittleEndianInt(dataStream);
            byte[] defLevelData = new byte[defLevelLength];
            dataStream.read(defLevelData);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        return decodeTypedValues(
                header.encoding(), dataStream, header.numValues(),
                definitionLevels, repetitionLevels, dictionary);
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, MappedByteBuffer pageData, int uncompressedPageSize,
            Dictionary dictionary) throws IOException {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.remaining() - valuesOffset;

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            byte[] repLevelData = new byte[repLevelLen];
            pageData.slice(0, repLevelLen).get(repLevelData);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            byte[] defLevelData = new byte[defLevelLen];
            pageData.slice(repLevelLen, defLevelLen).get(defLevelData);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        byte[] valuesData;
        int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;

        if (header.isCompressed() && compressedValuesLen > 0) {
            MappedByteBuffer compressedValues = pageData.slice(valuesOffset, compressedValuesLen);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            valuesData = decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        else {
            valuesData = new byte[compressedValuesLen];
            pageData.slice(valuesOffset, compressedValuesLen).get(valuesData);
        }

        ByteArrayInputStream valuesStream = new ByteArrayInputStream(valuesData);

        return decodeTypedValues(
                header.encoding(), valuesStream, header.numValues(),
                definitionLevels, repetitionLevels, dictionary);
    }

    /**
     * Decode values into Page using primitive arrays where possible.
     */
    private Page decodeTypedValues(Encoding encoding, InputStream dataStream,
                                   int numValues,
                                   int[] definitionLevels, int[] repetitionLevels,
                                   Dictionary dictionary) throws IOException {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

        // Try to decode into primitive arrays for supported type/encoding combinations
        switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(dataStream, type, column.typeLength());
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
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(dataStream);
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
                byte[] allData = dataStream.readAllBytes();
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        allData, numNonNullValues, type, column.typeLength());
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
                int bitWidth = dataStream.read();
                if (bitWidth < 0) {
                    throw new IOException("Failed to read bit width for dictionary indices");
                }
                byte[] indicesData = dataStream.readAllBytes();
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(indicesData, bitWidth);

                return dictionary.decodePage(indexDecoder, numValues, definitionLevels, repetitionLevels, maxDefLevel);
            }
            case RLE -> {
                // RLE encoding for boolean values uses bit-width of 1
                if (type != PhysicalType.BOOLEAN) {
                    throw new UnsupportedOperationException(
                            "RLE encoding for non-boolean types not yet supported: " + type);
                }

                // Read 4-byte length prefix (little-endian)
                byte[] lengthBytes = new byte[4];
                dataStream.read(lengthBytes);
                int rleLength = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

                // Read the RLE-encoded data
                byte[] rleData = new byte[rleLength];
                dataStream.read(rleData);

                RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                        new ByteArrayInputStream(rleData), 1);
                boolean[] values = new boolean[numValues];
                decoder.readBooleans(values, definitionLevels, maxDefLevel);
                return new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(dataStream);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                return new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_BYTE_ARRAY -> {
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(dataStream);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                return new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            default -> throw new UnsupportedOperationException("Encoding not yet supported: " + encoding);
        }
    }
}
