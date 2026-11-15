/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `DATA_PAGE`s whose header declares the deprecated `BIT_PACKED` encoding for a level section:
/// the section has no length prefix and packs each level from its most significant bit.
class BitPackedLevelsTest {

    /// Ten definition levels at one bit, so the section ends inside its second byte.
    @Test
    void readsBitPackedDefinitionLevels() {
        ColumnSchema column = DataPages.optional(PhysicalType.BYTE_ARRAY, null);
        int[] definitionLevels = { 1, 0, 1, 1, 0, 0, 1, 1, 1, 0 };
        byte[] body = DataPages.concat(DataPages.bitPackedLevels(1, definitionLevels),
                plainStrings("a", "b", "c", "d", "e", "f"));
        ByteBuffer page = DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.BIT_PACKED, 10, body);

        byte[][] values = (byte[][]) DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                page, null);

        assertThat(values).isDeepEqualTo(new byte[][]{ bytes("a"), null, bytes("b"), bytes("c"), null, null,
                bytes("d"), bytes("e"), bytes("f"), null });
    }

    /// A list's element: repetition levels at one bit, definition levels at two, both bit-packed.
    @Test
    void readsBitPackedRepetitionAndDefinitionLevels() {
        int[] repetitionLevels = { 0, 1, 0, 0, 1, 1 };
        int[] definitionLevels = { 3, 3, 0, 3, 2, 3 };
        byte[] body = DataPages.concat(DataPages.bitPackedLevels(1, repetitionLevels),
                DataPages.bitPackedLevels(3, definitionLevels), plainInts(10, 20, 30, 40));

        Levels levels = readLevels(DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.BIT_PACKED, 6, body));

        assertThat(levels.repetition()).containsExactly(repetitionLevels);
        assertThat(levels.definition()).containsExactly(definitionLevels);
        assertThat(levels.values()).containsExactly(10, 20, 0, 30, 0, 40);
    }

    /// Each section is framed by its own declared encoding.
    @Test
    void readsRleRepetitionLevelsBesideBitPackedDefinitionLevels() {
        int[] repetitionLevels = { 0, 1, 1, 0 };
        int[] definitionLevels = { 3, 1, 3, 3 };
        byte[] body = DataPages.concat(DataPages.levelSection(1, repetitionLevels),
                DataPages.bitPackedLevels(3, definitionLevels), plainInts(7, 8, 9));

        Levels levels = readLevels(DataPages.page(Encoding.PLAIN, Encoding.RLE, Encoding.BIT_PACKED, 4, body));

        assertThat(levels.repetition()).containsExactly(repetitionLevels);
        assertThat(levels.definition()).containsExactly(definitionLevels);
        assertThat(levels.values()).containsExactly(7, 0, 8, 9);
    }

    /// The fixed-size-list fast path reads the RLE hybrid only, so a page of two-element lists
    /// takes it with `RLE` levels and is left to the regular path with `BIT_PACKED` ones.
    @Test
    void leavesBitPackedLevelsToTheRegularPath() {
        // Sixteen lists of two, all present, so the definition levels are one RLE run
        int[] repetitionLevels = new int[32];
        int[] definitionLevels = new int[32];
        int[] elements = new int[32];
        for (int i = 0; i < 32; i++) {
            repetitionLevels[i] = i % 2;
            definitionLevels[i] = 2;
            elements[i] = i;
        }
        byte[] values = plainInts(elements);
        byte[] rle = DataPages.concat(DataPages.levelSection(1, repetitionLevels),
                DataPages.levelSection(2, definitionLevels), values);
        byte[] bitPacked = DataPages.concat(DataPages.bitPackedLevels(1, repetitionLevels),
                DataPages.bitPackedLevels(2, definitionLevels), values);

        assertThat(decodeFixedListShape(DataPages.page(Encoding.PLAIN, Encoding.RLE, Encoding.RLE, 32, rle))
                .fixedListK()).isEqualTo(2);

        PageDecoder.InPlaceNestedPage page = decodeFixedListShape(
                DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.BIT_PACKED, 32, bitPacked));
        assertThat(page.fixedListK()).isZero();
        assertThat(copy(page.repetitionLevels(), page.size())).containsExactly(repetitionLevels);
        assertThat(copy(page.definitionLevels(), page.size())).containsExactly(definitionLevels);
    }

    /// parquet-java declares `BIT_PACKED` for a level whose maximum is 0, of which a page stores
    /// no bytes at all.
    @Test
    void readsARequiredColumnDeclaringBitPackedLevels() {
        ColumnSchema column = DataPages.required(PhysicalType.INT32, null);
        ByteBuffer page = DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.BIT_PACKED, 3,
                plainInts(1, 2, 3));

        assertThat((int[]) DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column, page,
                null)).containsExactly(1, 2, 3);
    }

    @Test
    void readsAnOptionalColumnDeclaringBitPackedRepetitionLevels() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT32, null);
        byte[] body = DataPages.concat(DataPages.levelSection(1, 1, 1, 0), plainInts(4, 5));
        ByteBuffer page = DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.RLE, 3, body);

        assertThat((int[]) DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column, page,
                null)).containsExactly(4, 5, 0);
    }

    @Test
    void rejectsBitPackedLevelsLongerThanThePage() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT32, null);
        ByteBuffer page = DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.BIT_PACKED, 100,
                new byte[2]);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                page, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BIT_PACKED definition levels of 100 values at 1 bit take 13 bytes: 2 bytes remain in"
                        + " the page");
    }

    @Test
    void rejectsALevelEncodingThatIsNoLevelEncoding() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT32, null);
        byte[] body = DataPages.concat(DataPages.levelSection(1, 1), plainInts(4));
        ByteBuffer page = DataPages.page(Encoding.PLAIN, Encoding.RLE, Encoding.PLAIN, 1, body);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                page, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("A DATA_PAGE's definition levels cannot be PLAIN; the format encodes them as RLE or"
                        + " BIT_PACKED");
    }

    /// A level encoding this release cannot name is a gap in this release, as it is for values.
    @Test
    void refusesALevelEncodingThisReleaseDoesNotKnow() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT32, null);
        byte[] body = DataPages.concat(DataPages.levelSection(1, 1), plainInts(4));
        ByteBuffer page = DataPages.page(Encoding.PLAIN, 3, 42, 1, body); // 3 is RLE

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                page, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Encoding not yet supported for a DATA_PAGE's definition levels: UNKNOWN (Thrift encoding"
                        + " value 42)");
    }

    private record Levels(int[] repetition, int[] definition, int[] values) {
    }

    /// The levels and values of `page` over a list's `INT32` element, copied out of the scratch
    /// they are decoded into.
    private static Levels readLevels(ByteBuffer page) {
        ColumnSchema column = DataPages.repeated(PhysicalType.INT32, null);
        PageDecoder decoder = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED);
        return TestDecodeBuffers.decode(buffers -> {
            PageDecoder.InPlaceNestedPage decoded = decoder.decodeNestedInPlace(DataPage.parse(page, 0), null,
                    new PageDecoder.LevelScratch(), buffers);
            int size = decoded.size();
            return new Levels(copy(decoded.repetitionLevels(), size), copy(decoded.definitionLevels(), size),
                    (int[]) DataPages.values(decoded, column));
        });
    }

    /// Decodes `page` over the `INT32` element of an optional list of required elements, the
    /// level shape the fixed-size-list fast path takes, with the fast path enabled.
    private static PageDecoder.InPlaceNestedPage decodeFixedListShape(ByteBuffer page) {
        ColumnSchema column = new ColumnSchema(FieldPath.of("c", "list", "element"), PhysicalType.INT32,
                RepetitionType.REQUIRED, null, 0, 2, 1, null);
        PageDecoder decoder = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED);
        return TestDecodeBuffers.decode(buffers -> decoder.decodeNestedInPlace(DataPage.parse(page, 0), null,
                new PageDecoder.LevelScratch(), buffers));
    }

    private static int[] copy(int[] levels, int size) {
        int[] copy = new int[size];
        System.arraycopy(levels, 0, copy, 0, size);
        return copy;
    }

    private static byte[] plainInts(int... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value : values) {
            buffer.putInt(value);
        }
        return buffer.array();
    }

    private static byte[] plainStrings(String... values) {
        byte[][] parts = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            byte[] value = bytes(values[i]);
            parts[i] = DataPages.concat(
                    ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value.length).array(), value);
        }
        return DataPages.concat(parts);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(UTF_8);
    }
}
