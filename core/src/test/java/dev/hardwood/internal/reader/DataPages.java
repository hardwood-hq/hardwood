/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.thrift.PageHeaderWriter;
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
        Object out = switch (type) {
            case BOOLEAN -> new boolean[values.size()];
            case INT32 -> new int[values.size()];
            case INT64 -> new long[values.size()];
            case FLOAT -> new float[values.size()];
            case DOUBLE -> new double[values.size()];
            default -> throw new IllegalArgumentException("Not a numeric type: " + type);
        };
        values.view().copy(0, out, 0, values.size());
        return out;
    }

    /// The values of a binary page, one per entry, `null` at a null one, written by
    /// [BinaryPageValues#copyPlain] or [BinaryPageValues#copyIds].
    static byte[][] values(BinaryPageValues values, int maxDefinitionLevel) {
        BinaryPageValues view = values.view();
        int size = view.size();
        int[] starts = new int[size];
        int[] ends = new int[size];
        byte[][] out = new byte[size][];
        if (view.dictionary() != null) {
            int[] ids = new int[size];
            view.copyIds(0, size, ids, starts, ends, 0);
            for (int i = 0; i < size; i++) {
                out[i] = ids[i] < 0 ? null : view.dictionary().entry(ids[i]);
            }
            return out;
        }
        byte[] bytes = new byte[view.bytes(0, size)];
        view.copyPlain(0, size, starts, ends, 0, bytes, 0);
        int[] definitionLevels = view.definitionLevels();
        for (int i = 0; i < size; i++) {
            if (definitionLevels == null || definitionLevels[i] == maxDefinitionLevel) {
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
