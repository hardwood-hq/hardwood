/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.libdeflate.LibdeflateDecompressor;
import dev.hardwood.internal.encoding.ByteStreamSplitDecoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.internal.encoding.DeltaByteArrayDecoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayDecoder;
import dev.hardwood.internal.encoding.NativeBssDecoder;
import dev.hardwood.internal.encoding.NativePlainDecoder;
import dev.hardwood.internal.encoding.PlainDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.encoding.ValueDecoder;
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

/// Java 22+ override of [PageDecoder] that adds a zero-copy FFM fast path for
/// GZIP-compressed PLAIN and BYTE_STREAM_SPLIT encoded numeric pages.
///
/// ## What changes vs. the Java 21 base
///
/// When the decompressor is [LibdeflateDecompressor] **and** the data-page
/// encoding is [Encoding#PLAIN] or [Encoding#BYTE_STREAM_SPLIT] on a supported
/// physical type, `decodePage` calls [LibdeflateDecompressor#decompressToSegment]
/// instead of [Decompressor#decompress]:
///
/// ```
/// Old path:  native output → MemorySegment.copy → byte[] → Decoder(byte[])
/// New path:  native output → MemorySegment slice → NativeDecoder(MemorySegment)
/// ```
///
/// The `~uncompressedSize` native-to-heap copy is eliminated for these pages.
/// All other encodings and physical types fall back to the existing `byte[]` path.
///
/// ## Multi-release JAR placement
///
/// This class is the `META-INF/versions/22/` override in the multi-release JAR.
/// On Java 21 the base implementation in `dev.hardwood.internal.reader` is used.
public class PageDecoder {

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final DecompressorFactory decompressorFactory;

    public PageDecoder(ColumnMetaData columnMetaData, ColumnSchema column,
                       DecompressorFactory decompressorFactory) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
    }

    public boolean isCompatibleWith(ColumnMetaData otherMetaData) {
        return columnMetaData.codec() == otherMetaData.codec();
    }

    public DecompressorFactory getDecompressorFactory() {
        return decompressorFactory;
    }

    public Page nullPage(int numValues) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (maxDefLevel == 0) {
            throw new IllegalStateException(
                    "Cannot create null placeholder page for required column '"
                    + column.name() + "' — maxDefinitionLevel is 0");
        }
        int[] definitionLevels = new int[numValues];
        int[] repetitionLevels = column.maxRepetitionLevel() > 0 ? new int[numValues] : null;
        PhysicalType type = column.type();
        return switch (type) {
            case INT64   -> new Page.LongPage(new long[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case DOUBLE  -> new Page.DoublePage(new double[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case INT32   -> new Page.IntPage(new int[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case FLOAT   -> new Page.FloatPage(new float[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case BOOLEAN -> new Page.BooleanPage(new boolean[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    new Page.ByteArrayPage(new byte[numValues][], definitionLevels, repetitionLevels, maxDefLevel, numValues);
        };
    }

    /// Decodes a single data page using the FFM fast path when available.
    public Page decodePage(ByteBuffer pageBuffer, Dictionary dictionary) throws IOException {
        PageDecodedEvent event = new PageDecodedEvent();
        event.begin();

        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerReader.getBytesRead();

        int compressedSize = pageHeader.compressedPageSize();
        ByteBuffer pageData = pageBuffer.slice(headerSize, compressedSize);

        if (pageHeader.crc() != null) {
            CrcValidator.assertCorrectCrc(pageHeader.crc(), pageData, column.name());
        }

        Page result = switch (pageHeader.type()) {
            case DATA_PAGE -> {
                DataPageHeader dataHeader = pageHeader.dataPageHeader();
                Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());

                // FFM fast path: skip native→heap copy for supported native pages
                if (decompressor instanceof LibdeflateDecompressor) {
                    LibdeflateDecompressor ld = (LibdeflateDecompressor) decompressor;
                    if (isNativePlainCompatible(dataHeader.encoding(), column.type()) ||
                        isNativeBssCompatible(dataHeader.encoding(), column.type())) {
                        MemorySegment input = MemorySegment.ofBuffer(pageData);
                        MemorySegment nativeData = ld.decompressToSegment(
                                input, pageData.remaining(), pageHeader.uncompressedPageSize());
                        event.nativeFastPath = true;
                        yield parseDataPageFromSegment(dataHeader, nativeData, dictionary);
                    }
                }

                // Fallback: byte[] path
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                yield parseDataPage(dataHeader, uncompressedData, dictionary);
            }
            case DATA_PAGE_V2 -> {
                DataPageHeaderV2 v2Header = pageHeader.dataPageHeaderV2();
                Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());

                if (decompressor instanceof LibdeflateDecompressor && v2Header.isCompressed()) {
                    LibdeflateDecompressor ld = (LibdeflateDecompressor) decompressor;
                    if (isNativePlainCompatible(v2Header.encoding(), column.type()) ||
                        isNativeBssCompatible(v2Header.encoding(), column.type())) {
                        event.nativeFastPath = true;
                        yield parseDataPageV2FromSegment(v2Header, pageData,
                                pageHeader.uncompressedPageSize(), dictionary, ld);
                    }
                }

                yield parseDataPageV2(v2Header, pageData,
                        pageHeader.uncompressedPageSize(), dictionary);
            }
            default -> throw new IOException("Unexpected page type for single-page decode: " + pageHeader.type());
        };

        event.column = column.name();
        event.compressedSize = compressedSize;
        event.uncompressedSize = pageHeader.uncompressedPageSize();
        event.commit();

        return result;
    }

    // ── FFM fast path ───────────────────────────────────────────────────────

    private static boolean isNativePlainCompatible(Encoding encoding, PhysicalType type) {
        if (encoding != Encoding.PLAIN) {
            return false;
        }
        return switch (type) {
            case INT32, INT64, DOUBLE, FLOAT -> true;
            default -> false;
        };
    }

    private static boolean isNativeBssCompatible(Encoding encoding, PhysicalType type) {
        if (encoding != Encoding.BYTE_STREAM_SPLIT) {
            return false;
        }
        return switch (type) {
            case INT32, INT64, DOUBLE, FLOAT, FIXED_LEN_BYTE_ARRAY -> true;
            default -> false;
        };
    }

    private Page parseDataPageFromSegment(DataPageHeader header, MemorySegment seg,
                                          Dictionary dictionary) throws IOException {
        long offset = 0;

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            int repLen = readIntLE(seg, offset);
            offset += Integer.BYTES;
            byte[] repBytes = segmentToBytes(seg, offset, repLen);
            repetitionLevels = decodeLevels(repBytes, 0, repLen,
                    header.numValues(), column.maxRepetitionLevel());
            offset += repLen;
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0) {
            int defLen = readIntLE(seg, offset);
            offset += Integer.BYTES;
            byte[] defBytes = segmentToBytes(seg, offset, defLen);
            definitionLevels = decodeLevels(defBytes, 0, defLen,
                    header.numValues(), column.maxDefinitionLevel());
            offset += defLen;
        }

        return decodeValuesFromSegment(header.encoding(), seg, offset, header.numValues(),
                                       definitionLevels, repetitionLevels);
    }

    private Page parseDataPageV2FromSegment(DataPageHeaderV2 header, ByteBuffer pageData,
                                            int uncompressedPageSize, Dictionary dictionary,
                                            LibdeflateDecompressor ld) throws IOException {
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

        int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;
        MemorySegment compressedValuesSeg = MemorySegment.ofBuffer(pageData.slice(valuesOffset, compressedValuesLen));
        MemorySegment nativeData = ld.decompressToSegment(compressedValuesSeg, compressedValuesLen, uncompressedValuesSize);

        return decodeValuesFromSegment(header.encoding(), nativeData, 0, header.numValues(),
                                       definitionLevels, repetitionLevels);
    }

    private Page decodeValuesFromSegment(Encoding encoding, MemorySegment seg, long offset, int numValues,
                                         int[] definitionLevels, int[] repetitionLevels) throws IOException {
        if (encoding == Encoding.PLAIN) {
            NativePlainDecoder decoder = new NativePlainDecoder(
                    seg, offset, column.type(), column.typeLength());
            return decodeTypedValuesNative(decoder, numValues, definitionLevels, repetitionLevels);
        } else if (encoding == Encoding.BYTE_STREAM_SPLIT) {
            int numNonNullValues = countNonNullValues(numValues, definitionLevels);
            NativeBssDecoder decoder = new NativeBssDecoder(
                    seg, offset, numNonNullValues, column.type(), column.typeLength());
            return decodeTypedValuesNative(decoder, numValues, definitionLevels, repetitionLevels);
        }
        throw new IllegalStateException("Unsupported native encoding: " + encoding);
    }

    private static int readIntLE(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), offset);
    }

    private static byte[] segmentToBytes(MemorySegment seg, long offset, int length) {
        byte[] buf = new byte[length];
        MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, offset, buf, 0, length);
        return buf;
    }

    private Page decodeTypedValuesNative(ValueDecoder decoder, int numValues,
                                         int[] definitionLevels, int[] repetitionLevels) throws IOException {
        int maxDefLevel = column.maxDefinitionLevel();
        return switch (column.type()) {
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
            default -> throw new IllegalStateException(
                    "decodeTypedValuesNative called for unsupported type: " + column.type());
        };
    }

    // ── byte[] fallback path (unchanged from java21 base) ───────────────────

    private int[] decodeLevels(byte[] levelData, int offset, int length,
                                int numValues, int maxLevel) {
        int[] levels = new int[numValues];
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                levelData, offset, length, getBitWidth(maxLevel));
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

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
        return decodeTypedValues(header.encoding(), data, offset, header.numValues(),
                definitionLevels, repetitionLevels, dictionary);
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, ByteBuffer pageData,
                                  int uncompressedPageSize, Dictionary dictionary) throws IOException {
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

        return decodeTypedValues(header.encoding(), valuesData, 0, header.numValues(),
                definitionLevels, repetitionLevels, dictionary);
    }

    private Page decodeTypedValues(Encoding encoding, byte[] data, int offset,
                                    int numValues, int[] definitionLevels,
                                    int[] repetitionLevels, Dictionary dictionary) throws IOException {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

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
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(
                        data, offset, data.length - offset, bitWidth);
                return dictionary.decodePage(indexDecoder, numValues,
                        definitionLevels, repetitionLevels, maxDefLevel);
            }
            case RLE -> {
                if (type != PhysicalType.BOOLEAN) {
                    throw new UnsupportedOperationException(
                            "RLE encoding for non-boolean types not yet supported: " + type);
                }
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
