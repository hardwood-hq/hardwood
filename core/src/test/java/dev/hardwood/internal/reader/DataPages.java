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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.encoding.RleBitPackingHybridEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

/// Builds `DATA_PAGE`s by hand and reads them through [PageDecoder]'s in-place entry points, for
/// tests of what the readers make of a page body.
///
/// A read returns the page's values one per level entry, in the form
/// [ReferencePageDecoder.DecodedPage#values] has, written by the readers' own copy methods, so
/// that a reader's failure, eager or during its fill, is raised by the read.
final class DataPages {

    private DataPages() {
    }

    /// A required column `c` of `type`.
    ///
    /// @param typeLength the column's value width for `FIXED_LEN_BYTE_ARRAY`, `null` otherwise
    static ColumnSchema required(PhysicalType type, Integer typeLength) {
        return new ColumnSchema(FieldPath.of("c"), type, RepetitionType.REQUIRED, typeLength, 0, 0, 0, null);
    }

    /// An optional column `c` of `type`: definition levels of one bit, no repetition.
    static ColumnSchema optional(PhysicalType type, Integer typeLength) {
        return new ColumnSchema(FieldPath.of("c"), type, RepetitionType.OPTIONAL, typeLength, 0, 1, 0, null);
    }

    /// A list's element `c.list.element` of `type`: an optional list of optional elements,
    /// repetition levels of one bit and definition levels of two.
    static ColumnSchema repeated(PhysicalType type, Integer typeLength) {
        return new ColumnSchema(FieldPath.of("c", "list", "element"), type, RepetitionType.OPTIONAL, typeLength, 0,
                3, 1, null);
    }

    /// The RLE/bit-packed hybrid encoding of `levels` at the bit width of `maxLevel`, without a
    /// length prefix.
    static byte[] levels(int maxLevel, int... levels) {
        RleBitPackingHybridEncoder encoder = new RleBitPackingHybridEncoder(32 - Integer.numberOfLeadingZeros(maxLevel));
        encoder.writeInts(levels, 0, levels.length);
        return encoder.toByteArray();
    }

    /// A `V1` level section: the encoding of `levels` behind its 4-byte little-endian length.
    static byte[] levelSection(int maxLevel, int... levels) {
        byte[] encoded = levels(maxLevel, levels);
        return concat(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.length).array(), encoded);
    }

    static byte[] concat(byte[]... parts) {
        int length = 0;
        for (byte[] part : parts) {
            length += part.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, pos, part.length);
            pos += part.length;
        }
        return result;
    }

    static PageDecoder decoder(ColumnSchema column, CompressionCodec codec) {
        ColumnMetaData metaData = new ColumnMetaData(column.type(), List.of(Encoding.PLAIN), FieldPath.of("c"),
                codec, 0, 0, 0, Map.of(), 0, null, null, null, null, null, List.of(), null);
        return new PageDecoder(metaData, column, new DecompressorFactory(null));
    }

    /// An uncompressed `DATA_PAGE` of `numValues` values in `encoding`, its body (levels included)
    /// `body`.
    static ByteBuffer page(Encoding encoding, int numValues, byte[] body) {
        return withBody(dataPageHeader(numValues, body, encoding), body);
    }

    /// A page as [#page] builds it, its header declaring the raw Thrift encoding value
    /// `thriftValue`, which [Encoding] need not name.
    ///
    /// The header writer takes an [Encoding] only, so the header is written twice with encodings
    /// whose values differ, and the one byte the two disagree on (the encoding's zigzag varint)
    /// is replaced. A value below 64 is a single byte in that form.
    static ByteBuffer page(int thriftValue, int numValues, byte[] body) {
        if (thriftValue < 0 || thriftValue >= 64) {
            throw new IllegalArgumentException("Thrift value must be in [0, 64): " + thriftValue);
        }
        byte[] plain = dataPageHeader(numValues, body, Encoding.PLAIN);
        byte[] other = dataPageHeader(numValues, body, Encoding.RLE_DICTIONARY);
        int differing = -1;
        for (int i = 0; i < plain.length; i++) {
            if (plain[i] != other[i]) {
                if (differing != -1) {
                    throw new IllegalStateException("Headers differ in more than one byte");
                }
                differing = i;
            }
        }
        plain[differing] = (byte) (thriftValue << 1);
        return withBody(plain, body);
    }

    /// A `DATA_PAGE_V2` of `numValues` values in `numRows` rows: its level sections stored as given,
    /// its values compressed with `codec` when `compressed`, stored as they are otherwise.
    static ByteBuffer pageV2(Encoding encoding, int numValues, int numRows, byte[] repetitionLevels,
                             byte[] definitionLevels, byte[] values, CompressionCodec codec, boolean compressed) {
        byte[] stored = compressed ? new CompressorFactory().getCompressor(codec).compress(values, 0, values.length)
                : values;
        byte[] body = concat(repetitionLevels, definitionLevels, stored);
        CRC32 crc = new CRC32();
        crc.update(body);
        ThriftCompactWriter header = new ThriftCompactWriter();
        short saved = header.pushFieldIdContext();
        header.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        header.writeI32(DATA_PAGE_V2);
        header.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        header.writeI32(repetitionLevels.length + definitionLevels.length + values.length);
        header.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        header.writeI32(body.length);
        header.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
        header.writeI32((int) crc.getValue());
        header.writeFieldBegin(8, ThriftCompactConstants.FieldType.STRUCT);
        short savedV2 = header.pushFieldIdContext();
        header.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        header.writeI32(numValues);
        header.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        header.writeI32(0);
        header.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        header.writeI32(numRows);
        header.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
        header.writeI32(thriftValue(encoding));
        header.writeFieldBegin(5, ThriftCompactConstants.FieldType.I32);
        header.writeI32(definitionLevels.length);
        header.writeFieldBegin(6, ThriftCompactConstants.FieldType.I32);
        header.writeI32(repetitionLevels.length);
        header.writeBool(7, compressed);
        header.writeFieldStop();
        header.popFieldIdContext(savedV2);
        header.writeFieldStop();
        header.popFieldIdContext(saved);
        return withBody(header.toByteArray(), body);
    }

    /// Thrift value of `PageType.DATA_PAGE_V2`.
    private static final int DATA_PAGE_V2 = 3;

    /// The Thrift value of `encoding`, as `parquet.thrift` numbers it.
    private static int thriftValue(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> 0;
            case PLAIN_DICTIONARY -> 2;
            case RLE -> 3;
            case BIT_PACKED -> 4;
            case DELTA_BINARY_PACKED -> 5;
            case DELTA_LENGTH_BYTE_ARRAY -> 6;
            case DELTA_BYTE_ARRAY -> 7;
            case RLE_DICTIONARY -> 8;
            case BYTE_STREAM_SPLIT -> 9;
            case ALP -> 10;
            case UNKNOWN -> throw new IllegalArgumentException("UNKNOWN has no Thrift value");
        };
    }

    /// Reads an uncompressed `DATA_PAGE` of `numValues` values over a required column `c` of
    /// `type`, its value section `body`.
    static Object readRequired(Encoding encoding, PhysicalType type, Integer typeLength, int numValues,
                               byte[] body) {
        return readRequired(encoding, type, typeLength, numValues, body, null);
    }

    static Object readRequired(Encoding encoding, PhysicalType type, Integer typeLength, int numValues, byte[] body,
                               Dictionary dictionary) {
        ColumnSchema column = required(type, typeLength);
        return read(decoder(column, CompressionCodec.UNCOMPRESSED), column, page(encoding, numValues, body),
                dictionary);
    }

    /// Reads `page` through the entry point that serves `column`: [PageDecoder#decodeNestedInPlace]
    /// for a column with repetition, [PageDecoder#decodeFlat] or [PageDecoder#decodeFlatBinary]
    /// otherwise.
    static Object read(PageDecoder decoder, ColumnSchema column, ByteBuffer page, Dictionary dictionary) {
        PageDecoder.LevelScratch scratch = new PageDecoder.LevelScratch();
        if (column.maxRepetitionLevel() > 0) {
            return values(decoder.decodeNestedInPlace(page, dictionary, scratch), column);
        }
        return isBinary(column.type())
                ? values(decoder.decodeFlatBinary(page, dictionary, scratch), column.maxDefinitionLevel())
                : values(decoder.decodeFlat(page, dictionary, scratch), column.type());
    }

    /// Reads `page` through [PageDecoder#decodeNestedInPlace], whatever the column.
    static Object readNested(PageDecoder decoder, ColumnSchema column, ByteBuffer page, Dictionary dictionary) {
        return values(decoder.decodeNestedInPlace(page, dictionary, new PageDecoder.LevelScratch()), column);
    }

    static Object values(PageDecoder.InPlaceNestedPage page, ColumnSchema column) {
        return page.flat() != null
                ? values(page.flat(), column.type())
                : values(page.binary(), column.maxDefinitionLevel());
    }

    /// The values of a numeric page, one per entry, written by [FlatPageValues#copy].
    static Object values(FlatPageValues values, PhysicalType type) {
        return values(values, type, 0);
    }

    /// The values of a numeric page's entries from `from` on, read through a view whose first
    /// read starts there.
    static Object values(FlatPageValues values, PhysicalType type, int from) {
        int length = values.size() - from;
        Object out = switch (type) {
            case BOOLEAN -> new boolean[length];
            case INT32 -> new int[length];
            case INT64 -> new long[length];
            case FLOAT -> new float[length];
            case DOUBLE -> new double[length];
            default -> throw new IllegalArgumentException("Not a numeric type: " + type);
        };
        values.view().copy(from, out, 0, length);
        return out;
    }

    /// The values of a binary page, one per entry, `null` at a null one, written by
    /// [BinaryPageValues#copyPlain] or [BinaryPageValues#copyIds].
    static byte[][] values(BinaryPageValues values, int maxDefinitionLevel) {
        return values(values, maxDefinitionLevel, 0);
    }

    /// The values of a binary page's entries from `from` on, read through a view whose first read
    /// starts there.
    static byte[][] values(BinaryPageValues values, int maxDefinitionLevel, int from) {
        BinaryPageValues view = values.view();
        int length = view.size() - from;
        int[] starts = new int[length];
        int[] ends = new int[length];
        byte[][] out = new byte[length][];
        if (view.dictionary() != null) {
            int[] ids = new int[length];
            view.copyIds(from, length, ids, starts, ends, 0);
            for (int i = 0; i < length; i++) {
                out[i] = ids[i] < 0 ? null : view.dictionary().entry(ids[i]);
            }
            return out;
        }
        byte[] bytes = new byte[view.bytes(from, view.size())];
        view.copyPlain(from, length, starts, ends, 0, bytes, 0);
        int[] definitionLevels = view.definitionLevels();
        for (int i = 0; i < length; i++) {
            if (definitionLevels == null || definitionLevels[from + i] == maxDefinitionLevel) {
                out[i] = Arrays.copyOfRange(bytes, starts[i], ends[i]);
            }
        }
        return out;
    }

    private static boolean isBinary(PhysicalType type) {
        return type == PhysicalType.BYTE_ARRAY || type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                || type == PhysicalType.INT96;
    }

    private static byte[] dataPageHeader(int numValues, byte[] body, Encoding encoding) {
        CRC32 crc = new CRC32();
        crc.update(body);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, numValues, body.length, body.length,
                (int) crc.getValue(), encoding);
        return header.toByteArray();
    }

    private static ByteBuffer withBody(byte[] header, byte[] body) {
        ByteBuffer page = ByteBuffer.allocate(header.length + body.length);
        page.put(header).put(body).flip();
        return page;
    }
}
