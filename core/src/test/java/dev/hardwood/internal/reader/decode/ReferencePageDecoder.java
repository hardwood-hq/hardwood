/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.encoding.AlpDecoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// The reference the in-place readers are checked against: decodes a data page whole, into its
/// levels and one value per level entry.
///
/// It reads well-formed pages of every encoding the format defines and checks nothing a
/// well-formed page satisfies anyway; the readers' own validation is tested on them.
public final class ReferencePageDecoder {

    /// A data page decoded whole.
    ///
    /// @param definitionLevels one per entry, `null` for a column whose maximum definition level is `0`
    /// @param repetitionLevels one per entry, `null` for a column without repetition
    /// @param values one per entry, at its position: a `boolean[]`, `int[]`, `long[]`, `float[]`,
    ///        `double[]` or `byte[][]` by the column's physical type; a null entry holds `false`,
    ///        `0` or `null`
    public record DecodedPage(int numValues, int[] definitionLevels, int[] repetitionLevels, Object values) {
    }

    private final ColumnSchema column;
    private final CompressionCodec codec;
    private final DecompressorFactory decompressorFactory;

    public ReferencePageDecoder(ColumnSchema column, CompressionCodec codec, DecompressorFactory decompressorFactory) {
        this.column = column;
        this.codec = codec;
        this.decompressorFactory = decompressorFactory;
    }

    /// Decodes the `DATA_PAGE` or `DATA_PAGE_V2` that `page` holds, header included.
    ///
    /// @param dictionary the chunk's dictionary, `null` for a chunk without one
    public DecodedPage decode(ByteBuffer page, Dictionary dictionary) {
        ThriftCompactReader headerReader = new ThriftCompactReader(page, 0);
        PageHeader header = PageHeaderReader.read(headerReader);
        ByteBuffer body = page.slice(headerReader.getBytesRead(), header.compressedPageSize());
        if (header.type() == PageType.DATA_PAGE) {
            DataPageHeader v1 = header.dataPageHeader();
            byte[] data = decompress(body, header.uncompressedPageSize());
            int numValues = v1.numValues();
            int offset = 0;
            int[] repetitionLevels = null;
            if (column.maxRepetitionLevel() > 0) {
                int length = intAt(data, offset);
                repetitionLevels = levels(data, offset + 4, length, numValues, column.maxRepetitionLevel());
                offset += 4 + length;
            }
            int[] definitionLevels = null;
            if (column.maxDefinitionLevel() > 0) {
                int length = intAt(data, offset);
                definitionLevels = levels(data, offset + 4, length, numValues, column.maxDefinitionLevel());
                offset += 4 + length;
            }
            return page(numValues, definitionLevels, repetitionLevels, v1.encoding(), data, offset, data.length,
                    dictionary);
        }
        if (header.type() != PageType.DATA_PAGE_V2) {
            throw new IllegalArgumentException("Not a data page: " + header.type());
        }
        DataPageHeaderV2 v2 = header.dataPageHeaderV2();
        int numValues = v2.numValues();
        int repLength = v2.repetitionLevelsByteLength();
        int defLength = v2.definitionLevelsByteLength();
        byte[] levelBytes = new byte[repLength + defLength];
        body.get(0, levelBytes);
        int[] repetitionLevels = column.maxRepetitionLevel() > 0
                ? levels(levelBytes, 0, repLength, numValues, column.maxRepetitionLevel())
                : null;
        int[] definitionLevels = column.maxDefinitionLevel() > 0
                ? levels(levelBytes, repLength, defLength, numValues, column.maxDefinitionLevel())
                : null;
        ByteBuffer stored = body.slice(repLength + defLength, body.remaining() - repLength - defLength);
        byte[] data;
        if (v2.isCompressed() && stored.remaining() > 0) {
            data = decompress(stored, header.uncompressedPageSize() - repLength - defLength);
        }
        else {
            data = new byte[stored.remaining()];
            stored.get(0, data);
        }
        return page(numValues, definitionLevels, repetitionLevels, v2.encoding(), data, 0, data.length, dictionary);
    }

    private DecodedPage page(int numValues, int[] definitionLevels, int[] repetitionLevels, Encoding encoding,
                             byte[] data, int offset, int limit, Dictionary dictionary) {
        int maxDefinitionLevel = column.maxDefinitionLevel();
        int nonNull = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels == null || definitionLevels[i] == maxDefinitionLevel) {
                nonNull++;
            }
        }
        Object dense = decodeValues(encoding, column.type(), column.typeLength(), data, offset, limit, nonNull,
                dictionary);
        Object values = Array.newInstance(dense.getClass().getComponentType(), numValues);
        int next = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels == null || definitionLevels[i] == maxDefinitionLevel) {
                System.arraycopy(dense, next++, values, i, 1);
            }
        }
        return new DecodedPage(numValues, definitionLevels, repetitionLevels, values);
    }

    private byte[] decompress(ByteBuffer compressed, int uncompressedSize) {
        byte[] decompressed = TestDecodeBuffers.decode(buffers -> decompressorFactory.getDecompressor(codec)
                .decompress(compressed, uncompressedSize, new byte[uncompressedSize], buffers));
        return Arrays.copyOf(decompressed, uncompressedSize);
    }

    private static int[] levels(byte[] data, int offset, int length, int numValues, int maxLevel) {
        int[] levels = new int[numValues];
        new RleBitPackingHybridDecoder(data, offset, length, 32 - Integer.numberOfLeadingZeros(maxLevel))
                .readInts(levels, 0, numValues);
        return levels;
    }

    /// Decodes the `nonNull` values of a value section `[offset, limit)` of `data`, one after
    /// another, into the array [DecodedPage#values] holds for `type`.
    ///
    /// @param typeLength the width of a `FIXED_LEN_BYTE_ARRAY` value, `null` for any other type
    /// @param dictionary the chunk's dictionary, for a dictionary-encoded section
    public static Object decodeValues(Encoding encoding, PhysicalType type, Integer typeLength, byte[] data,
                                      int offset, int limit, int nonNull, Dictionary dictionary) {
        return switch (encoding) {
            case PLAIN -> plain(type, typeLength, data, offset, nonNull);
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> dictionaryValues(dictionary, data, offset, limit, nonNull);
            case RLE -> {
                int[] bits = new int[nonNull];
                new RleBitPackingHybridDecoder(data, offset + 4, intAt(data, offset), 1).readInts(bits, 0, nonNull);
                boolean[] values = new boolean[nonNull];
                for (int i = 0; i < nonNull; i++) {
                    values[i] = bits[i] != 0;
                }
                yield values;
            }
            case DELTA_BINARY_PACKED -> {
                if (type == PhysicalType.INT32) {
                    int[] values = new int[nonNull];
                    new DeltaBinaryPackedDecoder(data, offset, limit).readInts(values, 0, nonNull);
                    yield values;
                }
                long[] values = new long[nonNull];
                new DeltaBinaryPackedDecoder(data, offset, limit).readLongs(values, 0, nonNull);
                yield values;
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                DeltaBinaryPackedDecoder lengths = new DeltaBinaryPackedDecoder(data, offset, limit);
                int[] lengthValues = new int[nonNull];
                lengths.readInts(lengthValues, 0, nonNull);
                int pos = lengths.getPos();
                byte[][] values = new byte[nonNull][];
                for (int i = 0; i < nonNull; i++) {
                    values[i] = Arrays.copyOfRange(data, pos, pos + lengthValues[i]);
                    pos += lengthValues[i];
                }
                yield values;
            }
            case DELTA_BYTE_ARRAY -> deltaByteArray(data, offset, limit, nonNull);
            case BYTE_STREAM_SPLIT -> byteStreamSplit(type, typeLength, data, offset, nonNull);
            case ALP -> alp(type, data, offset, limit, nonNull);
            default -> throw new IllegalArgumentException("No reference decode of " + encoding + " over " + type);
        };
    }

    private static Object plain(PhysicalType type, Integer typeLength, byte[] data, int offset, int nonNull) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).position(offset);
        return switch (type) {
            case BOOLEAN -> {
                boolean[] values = new boolean[nonNull];
                for (int i = 0; i < nonNull; i++) {
                    values[i] = ((data[offset + (i >>> 3)] >>> (i & 7)) & 1) != 0;
                }
                yield values;
            }
            case INT32 -> {
                int[] values = new int[nonNull];
                buffer.slice().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(values);
                yield values;
            }
            case INT64 -> {
                long[] values = new long[nonNull];
                buffer.slice().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(values);
                yield values;
            }
            case FLOAT -> {
                float[] values = new float[nonNull];
                buffer.slice().order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(values);
                yield values;
            }
            case DOUBLE -> {
                double[] values = new double[nonNull];
                buffer.slice().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(values);
                yield values;
            }
            case BYTE_ARRAY -> {
                byte[][] values = new byte[nonNull][];
                for (int i = 0; i < nonNull; i++) {
                    byte[] value = new byte[buffer.getInt()];
                    buffer.get(value);
                    values[i] = value;
                }
                yield values;
            }
            case FIXED_LEN_BYTE_ARRAY, INT96 -> {
                int width = type == PhysicalType.INT96 ? 12 : typeLength;
                byte[][] values = new byte[nonNull][];
                for (int i = 0; i < nonNull; i++) {
                    values[i] = Arrays.copyOfRange(data, offset + i * width, offset + (i + 1) * width);
                }
                yield values;
            }
        };
    }

    private static Object dictionaryValues(Dictionary dictionary, byte[] data, int offset, int limit, int nonNull) {
        int bitWidth = data[offset] & 0xFF;
        int[] ids = new int[nonNull];
        new RleBitPackingHybridDecoder(data, offset + 1, limit - offset - 1, bitWidth).readInts(ids, 0, nonNull);
        return switch (dictionary) {
            case Dictionary.IntDictionary d -> Arrays.stream(ids).map(id -> d.values()[id]).toArray();
            case Dictionary.LongDictionary d -> Arrays.stream(ids).mapToLong(id -> d.values()[id]).toArray();
            case Dictionary.DoubleDictionary d -> Arrays.stream(ids).mapToDouble(id -> d.values()[id]).toArray();
            case Dictionary.FloatDictionary d -> {
                float[] values = new float[nonNull];
                for (int i = 0; i < nonNull; i++) {
                    values[i] = d.values()[ids[i]];
                }
                yield values;
            }
            case Dictionary.ByteArrayDictionary d -> {
                byte[][] values = new byte[nonNull][];
                for (int i = 0; i < nonNull; i++) {
                    values[i] = d.entry(ids[i]);
                }
                yield values;
            }
        };
    }

    private static byte[][] deltaByteArray(byte[] data, int offset, int limit, int nonNull) {
        DeltaBinaryPackedDecoder prefixDecoder = new DeltaBinaryPackedDecoder(data, offset, limit);
        int[] prefixLengths = new int[nonNull];
        prefixDecoder.readInts(prefixLengths, 0, nonNull);
        DeltaBinaryPackedDecoder suffixDecoder = new DeltaBinaryPackedDecoder(data, prefixDecoder.getPos(), limit);
        int[] suffixLengths = new int[nonNull];
        suffixDecoder.readInts(suffixLengths, 0, nonNull);
        int pos = suffixDecoder.getPos();
        byte[][] values = new byte[nonNull][];
        byte[] previous = new byte[0];
        for (int i = 0; i < nonNull; i++) {
            byte[] value = Arrays.copyOf(previous, prefixLengths[i] + suffixLengths[i]);
            System.arraycopy(data, pos, value, prefixLengths[i], suffixLengths[i]);
            pos += suffixLengths[i];
            values[i] = value;
            previous = value;
        }
        return values;
    }

    private static Object byteStreamSplit(PhysicalType type, Integer typeLength, byte[] data, int offset,
                                          int nonNull) {
        int width = switch (type) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            default -> typeLength;
        };
        byte[] plain = new byte[nonNull * width];
        for (int i = 0; i < nonNull; i++) {
            for (int b = 0; b < width; b++) {
                plain[i * width + b] = data[offset + b * nonNull + i];
            }
        }
        return plain(type, width, plain, 0, nonNull);
    }

    private static Object alp(PhysicalType type, byte[] data, int offset, int limit, int nonNull) {
        AlpDecoder decoder = new AlpDecoder(data, offset, limit, nonNull, type);
        decoder.validate();
        int vectorSize = decoder.vectorSize();
        float[] floats = type == PhysicalType.FLOAT ? new float[nonNull + vectorSize] : null;
        double[] doubles = type == PhysicalType.DOUBLE ? new double[nonNull + vectorSize] : null;
        float[] floatVector = floats != null ? new float[vectorSize] : null;
        double[] doubleVector = doubles != null ? new double[vectorSize] : null;
        for (int vector = 0; vector * vectorSize < nonNull; vector++) {
            int count = decoder.decodeVector(vector, floatVector, doubleVector);
            System.arraycopy(floats != null ? floatVector : doubleVector, 0, floats != null ? floats : doubles,
                    vector * vectorSize, count);
        }
        return floats != null ? Arrays.copyOf(floats, nonNull) : Arrays.copyOf(doubles, nonNull);
    }

    private static int intAt(byte[] data, int offset) {
        return ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
