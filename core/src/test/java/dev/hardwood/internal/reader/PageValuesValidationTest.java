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
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.encoding.DeltaBinaryPackedEncoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayEncoder;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The checks the in-place readers ([FlatPageValues], [BinaryPageValues]) make of a malformed value
/// section, on hand-built pages read through [PageDecoder]'s entry points. The `PLAIN` checks are
/// in [PageDecoderPlainSizeTest], the dictionary ids of a binary page in
/// [ByteArrayDictionaryInternTest].
class PageValuesValidationTest {

    // ==================== BYTE_STREAM_SPLIT ====================

    @Test
    void rejectsAByteStreamSplitSectionWithBytesLeftOver() {
        assertThatThrownBy(() -> DataPages.readRequired(Encoding.BYTE_STREAM_SPLIT, PhysicalType.INT32, null, 2,
                new byte[9]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BYTE_STREAM_SPLIT value section holds 9 bytes, not the 8 of its 2 INT32 values");
    }

    @Test
    void rejectsAShortByteStreamSplitSectionOfFixedLengthValues() {
        assertThatThrownBy(() -> DataPages.readRequired(Encoding.BYTE_STREAM_SPLIT,
                PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, 2, new byte[5]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BYTE_STREAM_SPLIT value section holds 5 bytes, not the 6 of its 2 FIXED_LEN_BYTE_ARRAY values");
    }

    @Test
    void rejectsAByteStreamSplitSectionOfFixedLengthValuesWithBytesLeftOver() {
        assertThatThrownBy(() -> DataPages.readRequired(Encoding.BYTE_STREAM_SPLIT,
                PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, 2, new byte[7]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BYTE_STREAM_SPLIT value section holds 7 bytes, not the 6 of its 2 FIXED_LEN_BYTE_ARRAY values");
    }

    // ==================== ALP ====================

    @Test
    void rejectsAnAlpSectionWithBytesLeftOver() {
        byte[] body = DataPages.concat(alpPage(4, new int[0]), new byte[2]);

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.ALP, PhysicalType.DOUBLE, null, 4, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP value section does not end with its 1 vectors; leftover bytes: 2");
    }

    /// An exception position is checked when the page is read, before any vector is decoded.
    @Test
    void rejectsAnAlpExceptionPositionOutsideItsVector() {
        assertThatThrownBy(() -> DataPages.readRequired(Encoding.ALP, PhysicalType.DOUBLE, null, 4,
                alpPage(4, new int[]{ 4 })))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP exception position 4 in vector 0 is outside its 4 values");
    }

    @Test
    void rejectsAnAlpElementCountOtherThanThePagesNonNullValues() {
        ColumnSchema column = DataPages.optional(PhysicalType.DOUBLE, null);
        // Three of the four values are present; the ALP header counts four
        byte[] body = DataPages.concat(DataPages.levelSection(1, 1, 0, 1, 1), alpPage(4, new int[0]));

        assertThatThrownBy(() -> read(column, Encoding.ALP, 4, body, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP page header counts 4 values, but the page has 3 non-null values");
    }

    /// The page holds the present values only; they land at the positions the definition levels
    /// mark present.
    @Test
    void readsAlpValuesToThePresentPositions() {
        ColumnSchema column = DataPages.optional(PhysicalType.DOUBLE, null);
        byte[] body = DataPages.concat(DataPages.levelSection(1, 0, 1, 1, 0, 0, 1, 1, 0),
                alpPage(4, new int[0]));

        double[] values = (double[]) read(column, Encoding.ALP, 8, body, null);

        assertThat(values).containsExactly(0, 0.75, 1.5, 0, 0, 2.25, 3.0, 0);
    }

    // ==================== Dictionary ids of a numeric page ====================

    @Test
    void rejectsAnIdBeyondANumericDictionary() {
        Dictionary dictionary = new Dictionary.LongDictionary(new long[]{ 10, 20 });

        // Bit width 2, an RLE run of 4 values with id 3
        assertThatThrownBy(() -> DataPages.readRequired(Encoding.RLE_DICTIONARY, PhysicalType.INT64, null, 4,
                new byte[]{ 2, 8, 0x03 }, dictionary))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 3 out of bounds for a dictionary of 2 entries");
    }

    @Test
    void rejectsANegativeIdOfANumericPage() {
        Dictionary dictionary = new Dictionary.IntDictionary(new int[]{ 10, 20 });

        // Bit width 32, an RLE run of 4 values with id 0x80000000
        assertThatThrownBy(() -> DataPages.readRequired(Encoding.RLE_DICTIONARY, PhysicalType.INT32, null, 4,
                new byte[]{ 32, 8, 0x00, 0x00, 0x00, (byte) 0x80 }, dictionary))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index -2147483648 out of bounds for a dictionary of 2 entries");
    }

    @Test
    void rejectsAnyPresentValueAgainstAnEmptyNumericDictionary() {
        Dictionary dictionary = new Dictionary.DoubleDictionary(new double[0]);

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.RLE_DICTIONARY, PhysicalType.DOUBLE, null, 4,
                new byte[]{ 0 }, dictionary))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Dictionary index 0 out of bounds for a dictionary of 0 entries");
    }

    @Test
    void rejectsAnIndexBitWidthAboveThirtyTwo() {
        Dictionary dictionary = new Dictionary.LongDictionary(new long[]{ 10 });

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.RLE_DICTIONARY, PhysicalType.INT64, null, 4,
                new byte[]{ 33, 8, 0 }, dictionary))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid dictionary index bit width: 33. Must be between 0 and 32");
    }

    /// An `RLE` stream of width 1 decodes `0` and `1` only; the reader still refuses another value.
    @Test
    void rejectsAnRleBooleanOtherThanZeroOrOne() {
        assertThatThrownBy(() -> FlatPageValues.rleBooleans(2, null, 0, new int[]{ 0, 2 }, 2))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid RLE boolean value: 2");
    }

    // ==================== DELTA_LENGTH_BYTE_ARRAY ====================

    @Test
    void rejectsDeltaLengthValueBytesBeyondWhatTheLengthsGive() {
        byte[] body = DataPages.concat(DeltaBinaryPackedEncoder.encodeInts(new int[]{ 3 }, 0, 1), utf8("abcd"));

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.DELTA_LENGTH_BYTE_ARRAY, PhysicalType.BYTE_ARRAY,
                null, 1, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_LENGTH_BYTE_ARRAY value section holds 4 value bytes, not the 3 its lengths give");
    }

    @Test
    void rejectsANegativeDeltaLength() {
        byte[] body = DataPages.concat(DeltaBinaryPackedEncoder.encodeInts(new int[]{ -1 }, 0, 1), utf8("a"));

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.DELTA_LENGTH_BYTE_ARRAY, PhysicalType.BYTE_ARRAY,
                null, 1, body))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid BYTE_ARRAY length: -1");
    }

    // ==================== DELTA_BYTE_ARRAY ====================

    @Test
    void rejectsAPrefixLongerThanThePreviousValue() {
        assertThatThrownBy(() -> readDeltaByteArray(PhysicalType.BYTE_ARRAY, null, new int[]{ 0, 5 },
                new int[]{ 3, 0 }, "abc"))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_BYTE_ARRAY value 1 takes a prefix of 5 bytes from a previous value of 3");
    }

    @Test
    void rejectsANegativeSuffixLength() {
        assertThatThrownBy(() -> readDeltaByteArray(PhysicalType.BYTE_ARRAY, null, new int[]{ 0 }, new int[]{ -1 },
                "a"))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid DELTA_BYTE_ARRAY length: prefix 0, suffix -1");
    }

    @Test
    void rejectsANegativePrefixLength() {
        assertThatThrownBy(() -> readDeltaByteArray(PhysicalType.BYTE_ARRAY, null, new int[]{ -2 }, new int[]{ 1 },
                "a"))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid DELTA_BYTE_ARRAY length: prefix -2, suffix 1");
    }

    @Test
    void rejectsSuffixBytesThatRunOut() {
        assertThatThrownBy(() -> readDeltaByteArray(PhysicalType.BYTE_ARRAY, null, new int[]{ 0 }, new int[]{ 4 },
                "abc"))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading byte array: expected 4, got 3");
    }

    @Test
    void rejectsDeltaByteArrayBytesLeftOver() {
        assertThatThrownBy(() -> readDeltaByteArray(PhysicalType.BYTE_ARRAY, null, new int[]{ 0 }, new int[]{ 3 },
                "abcd"))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_BYTE_ARRAY value section does not end with its 1 values; leftover bytes: 1");
    }

    /// A value of a fixed-length column reconstructed to another width would reach the batch at
    /// that width, and every conversion of the column's values reads its declared width.
    @Test
    void rejectsADeltaByteArrayValueOfAnotherWidthThanItsColumn() {
        assertThatThrownBy(() -> readDeltaByteArray(PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, new int[]{ 0, 2 },
                new int[]{ 4, 1 }, "aaaab"))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_BYTE_ARRAY value 1 is 3 bytes long, not the 4 of its FIXED_LEN_BYTE_ARRAY column");
    }

    @Test
    void readsDeltaByteArrayValuesOfTheirColumnsWidth() {
        byte[][] values = (byte[][]) readDeltaByteArray(PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, new int[]{ 0, 2 },
                new int[]{ 4, 2 }, "aaaabc");

        assertThat(values).isDeepEqualTo(new byte[][]{ utf8("aaaa"), utf8("aabc") });
    }

    // ==================== Pages without a non-null value ====================

    /// The delta length streams carry their headers even when no value is present.
    @Test
    void readsAnAllNullDeltaLengthPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.BYTE_ARRAY, null);
        byte[] body = DataPages.concat(DataPages.levelSection(1, 0, 0, 0),
                DeltaLengthByteArrayEncoder.encode(new byte[0], new int[]{ 0 }, 0, 0));

        assertThat((byte[][]) read(column, Encoding.DELTA_LENGTH_BYTE_ARRAY, 3, body, null))
                .isDeepEqualTo(new byte[3][]);
    }

    @Test
    void readsAnAllNullDeltaByteArrayPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.BYTE_ARRAY, null);
        byte[] body = DataPages.concat(DataPages.levelSection(1, 0, 0, 0),
                DeltaBinaryPackedEncoder.encodeInts(new int[0], 0, 0), DeltaBinaryPackedEncoder.encodeInts(new int[0], 0, 0));

        assertThat((byte[][]) read(column, Encoding.DELTA_BYTE_ARRAY, 3, body, null))
                .isDeepEqualTo(new byte[3][]);
    }

    @Test
    void readsAnAllNullAlpPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.FLOAT, null);
        byte[] header = ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN).put((byte) 0).put((byte) 0)
                .put((byte) 10).putInt(0).array();

        assertThat((float[]) read(column, Encoding.ALP, 3, DataPages.concat(DataPages.levelSection(1, 0, 0, 0), header),
                null)).containsExactly(0f, 0f, 0f);
    }

    @Test
    void readsAnAllNullByteStreamSplitPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT64, null);

        assertThat((long[]) read(column, Encoding.BYTE_STREAM_SPLIT, 3, DataPages.levelSection(1, 0, 0, 0), null))
                .containsExactly(0, 0, 0);
    }

    @Test
    void readsAnAllNullFixedLengthByteStreamSplitPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2);

        assertThat((byte[][]) read(column, Encoding.BYTE_STREAM_SPLIT, 2, DataPages.levelSection(1, 0, 0), null))
                .isDeepEqualTo(new byte[2][]);
    }

    // ==================== Fixed-length columns ====================

    /// A read validates a fixed-length column's width before it starts; a decoder built for a
    /// column without one refuses it at once, so that no page of it, null placeholder included,
    /// is read at an undefined width.
    @Test
    void refusesAFixedLengthColumnWithoutAWidth() {
        assertThatThrownBy(() -> DataPages.decoder(DataPages.optional(PhysicalType.FIXED_LEN_BYTE_ARRAY, null),
                CompressionCodec.UNCOMPRESSED))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("Column 'c' is a FIXED_LEN_BYTE_ARRAY that declares no type length");
    }

    @Test
    void refusesAFixedLengthColumnOfANonPositiveWidth() {
        assertThatThrownBy(() -> DataPages.decoder(DataPages.optional(PhysicalType.FIXED_LEN_BYTE_ARRAY, 0),
                CompressionCodec.UNCOMPRESSED))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("Column 'c' declares a FIXED_LEN_BYTE_ARRAY type length of 0, which must be positive");
    }

    @Test
    void readsANullPlaceholderOfAFixedLengthColumn() {
        PageDecoder decoder = DataPages.decoder(DataPages.optional(PhysicalType.FIXED_LEN_BYTE_ARRAY, 4),
                CompressionCodec.UNCOMPRESSED);

        PageDecoder.InPlaceNestedPage page = decoder.nullsInPlace(3, new PageDecoder.LevelScratch(), false);

        assertThat(DataPages.values(page.binary(), 1)).isDeepEqualTo(new byte[3][]);
    }

    @Test
    void nullPlaceholderIsChargedBeforeItsLevelsAreAllocated() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT64, null);
        PageDecoder decoder = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED,
                new WorkingSet(1, 1024 * 1024, null).column(0, column));

        // A placeholder of the most values a page may declare, from a page its statistics dropped
        assertThatThrownBy(() -> decoder.nullsInPlace(Integer.MAX_VALUE - 9, new PageDecoder.LevelScratch(), false))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Data page needs 25769803656 bytes to decode, which takes the read's working set to "
                        + "25769803656 bytes, more than its limit of 1048576 (hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void refusesV2LevelSectionsLongerThanThePage() {
        ColumnSchema column = DataPages.optional(PhysicalType.INT32, null);
        ByteBuffer page = DataPages.pageV2(Encoding.PLAIN, 1, 1, new byte[0], new byte[] { 2, 1 },
                Integer.MAX_VALUE, new byte[4], CompressionCodec.UNCOMPRESSED, false);

        assertThatThrownBy(() -> DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                page, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DATA_PAGE_V2 level sections of 0 and 2147483647 bytes exceed the page's 6 bytes");
    }

    // ==================== Helpers ====================

    private static Object read(ColumnSchema column, Encoding encoding, int numValues, byte[] body,
                               Dictionary dictionary) {
        return DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column,
                DataPages.page(encoding, numValues, body), dictionary);
    }

    /// A required `DELTA_BYTE_ARRAY` page: the prefix lengths, the suffix lengths, then `suffixes`.
    private static Object readDeltaByteArray(PhysicalType type, Integer typeLength, int[] prefixLengths,
                                             int[] suffixLengths, String suffixes) {
        byte[] body = DataPages.concat(DeltaBinaryPackedEncoder.encodeInts(prefixLengths, 0, prefixLengths.length),
                DeltaBinaryPackedEncoder.encodeInts(suffixLengths, 0, suffixLengths.length), utf8(suffixes));
        return DataPages.readRequired(Encoding.DELTA_BYTE_ARRAY, type, typeLength, prefixLengths.length, body);
    }

    /// An `ALP` page of `count` `DOUBLE` values in one vector of up to 1024: 0.75, 1.5, 2.25, 3.0
    /// and so on at (exponent 2, factor 0), a frame of reference of 75 and 8-bit deltas, with an
    /// exception at each of `exceptionPositions` holding 1.0.
    private static byte[] alpPage(int count, int[] exceptionPositions) {
        int exceptions = exceptionPositions.length;
        ByteBuffer body = ByteBuffer.allocate(7 + 4 + 13 + count + exceptions * 10).order(ByteOrder.LITTLE_ENDIAN);
        body.put((byte) 0).put((byte) 0).put((byte) 10).putInt(count);
        body.putInt(4);
        body.put((byte) 2).put((byte) 0).putShort((short) exceptions).putLong(75).put((byte) 8);
        for (int i = 0; i < count; i++) {
            body.put((byte) (i * 75));
        }
        for (int position : exceptionPositions) {
            body.putShort((short) position);
        }
        for (int i = 0; i < exceptions; i++) {
            body.putDouble(1.0);
        }
        return body.array();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
