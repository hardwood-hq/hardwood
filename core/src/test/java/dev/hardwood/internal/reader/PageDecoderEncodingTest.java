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

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.encoding.DeltaByteArrayEncoder;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What [PageDecoder] does with a data page whose declared encoding its column cannot carry.
///
/// A file may say anything. An encoding is defined over some physical types and not others, so a
/// page declaring one its column has no business carrying is a malformed file, and the decoder has
/// to say so rather than read the bytes as values of the wrong shape — which is silently wrong
/// twice over, once in the values and once in the type the consumer casts to.
/// Being a malformed file rather than a gap in this release, each of these is a
/// [ParquetReadException]; [java.lang.UnsupportedOperationException] is left to mean an encoding
/// no decoder has been written for.
class PageDecoderEncodingTest {

    private static final int NUM_VALUES = 4;
    private static final int FIXED_LENGTH = 4;

    /// The byte-array delta encodings carry a length stream and then bytes, so their values are
    /// byte arrays whatever the column's physical type is, which is not what an `INT32` column's
    /// reader expects.
    @Test
    void rejectsDeltaLengthByteArrayOnAnIntegerColumn() {
        assertThatThrownBy(() -> decode(Encoding.DELTA_LENGTH_BYTE_ARRAY, PhysicalType.INT32))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_LENGTH_BYTE_ARRAY is not defined over INT32; the format defines it"
                        + " over BYTE_ARRAY only");
    }

    /// Unlike `DELTA_BYTE_ARRAY`, `DELTA_LENGTH_BYTE_ARRAY` is defined over `BYTE_ARRAY` alone:
    /// its length stream is what a fixed-length column does not need and no writer emits. The two
    /// guards must therefore not be made to agree.
    @Test
    void rejectsDeltaLengthByteArrayOnAFixedLengthColumn() {
        assertThatThrownBy(() -> decode(Encoding.DELTA_LENGTH_BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_LENGTH_BYTE_ARRAY is not defined over FIXED_LEN_BYTE_ARRAY;"
                        + " the format defines it over BYTE_ARRAY only");
    }

    @Test
    void rejectsDeltaByteArrayOnADoubleColumn() {
        assertThatThrownBy(() -> decode(Encoding.DELTA_BYTE_ARRAY, PhysicalType.DOUBLE))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_BYTE_ARRAY is not defined over DOUBLE; the format defines it over "
                         + "BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY only")
                ;
    }

    /// `DELTA_BYTE_ARRAY` is defined over `FIXED_LEN_BYTE_ARRAY` as well, so that pair must not be
    /// caught by the same guard — the suffix lengths are written even where the values are fixed
    /// length by schema. Decoding a real page of them proves the guard lets the pair through,
    /// which asserting that some exception was not raised would not.
    @Test
    void decodesDeltaByteArrayOnAFixedLengthColumn() {
        Object values = decode(Encoding.DELTA_BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY,
                deltaByteArrayBody("aaaa", "aaab", "abcd", "abce"));

        assertThat(Arrays.stream((byte[][]) values)
                .map(value -> new String(value, UTF_8))
                .toList())
                .containsExactly("aaaa", "aaab", "abcd", "abce");
    }

    /// `RLE` carries booleans in the value position, and the level streams and dictionary indices
    /// elsewhere. An integer column claiming it is refused on the format's terms too, rather than
    /// as something this release has not implemented.
    @Test
    void rejectsRleOnAnIntegerColumn() {
        assertThatThrownBy(() -> decode(Encoding.RLE, PhysicalType.INT32))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("RLE encodes only boolean values in a data page, not INT32")
                ;
    }

    /// `BYTE_STREAM_SPLIT` scatters the bytes of fixed-width values, so it has nothing to say
    /// about a `BYTE_ARRAY` column. The refusal has to come before the decoder is built, since
    /// the decoder's own byte-width lookup would otherwise fail first and on its own terms.
    @Test
    void rejectsByteStreamSplitOnAByteArrayColumn() {
        assertThatThrownBy(() -> decode(Encoding.BYTE_STREAM_SPLIT, PhysicalType.BYTE_ARRAY))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BYTE_STREAM_SPLIT is not defined over BYTE_ARRAY; the format defines it over "
                         + "INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY only")
                ;
    }

    /// `BIT_PACKED` encodes levels; a page claiming it for its values is refused on the format's
    /// terms, not as an encoding this release has not got round to.
    @Test
    void rejectsBitPackedAsAValueEncoding() {
        assertThatThrownBy(() -> decode(Encoding.BIT_PACKED, PhysicalType.INT32))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BIT_PACKED encodes levels and is not valid for a data page's values");
    }

    /// `ALP` turns decimal-scaled integers back into floating-point values, so it is defined over
    /// `FLOAT` and `DOUBLE` only.
    @Test
    void rejectsAlpOnALongColumn() {
        assertThatThrownBy(() -> decode(Encoding.ALP, PhysicalType.INT64))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP is not defined over INT64; the format defines it over FLOAT, DOUBLE only");
    }

    /// An ALP page of one vector holding 1.5, 2.25, 3.0 and 0.75 at (exponent 2, factor 0): the
    /// integers 150, 225, 300 and 75, a frame of reference of 75 and 8-bit deltas.
    @Test
    void decodesAlpOnADoubleColumn() {
        ByteBuffer body = ByteBuffer.allocate(7 + 4 + 13 + 4).order(ByteOrder.LITTLE_ENDIAN);
        body.put((byte) 0).put((byte) 0).put((byte) 10).putInt(NUM_VALUES);
        body.putInt(4);
        body.put((byte) 2).put((byte) 0).putShort((short) 0).putLong(75).put((byte) 8);
        body.put((byte) 75).put((byte) 150).put((byte) 225).put((byte) 0);

        Object values = decode(Encoding.ALP, PhysicalType.DOUBLE, body.array());

        assertThat((double[]) values).containsExactly(1.5, 2.25, 3.0, 0.75);
    }

    /// A Thrift value no member of [Encoding] names reads as [Encoding#UNKNOWN], and the refusal
    /// names the raw value, since `UNKNOWN` alone does not say which encoding the page declared.
    /// This is a gap in this release rather than a malformed file.
    @Test
    void rejectsAnUnrecognizedEncodingByItsThriftValue() {
        ColumnSchema column = DataPages.required(PhysicalType.INT32, null);
        PageDecoder decoder = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED);

        assertThatThrownBy(() -> DataPages.read(decoder, column, DataPages.page(42, NUM_VALUES, new byte[16]), null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Encoding not yet supported: UNKNOWN (Thrift encoding value 42)");
    }

    @Test
    void rejectsByteStreamSplitOnABooleanColumn() {
        assertThatThrownBy(() -> decode(Encoding.BYTE_STREAM_SPLIT, PhysicalType.BOOLEAN))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BYTE_STREAM_SPLIT is not defined over BOOLEAN; the format defines it over "
                         + "INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY only");
    }

    @Test
    void rejectsByteStreamSplitOnAnInt96Column() {
        assertThatThrownBy(() -> decode(Encoding.BYTE_STREAM_SPLIT, PhysicalType.INT96))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("BYTE_STREAM_SPLIT is not defined over INT96; the format defines it over "
                         + "INT32, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY only");
    }

    @Test
    void rejectsDeltaBinaryPackedOnADoubleColumn() {
        assertThatThrownBy(() -> decode(Encoding.DELTA_BINARY_PACKED, PhysicalType.DOUBLE))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_BINARY_PACKED is not defined over DOUBLE; the format defines it over "
                         + "INT32, INT64 only");
    }

    @Test
    void rejectsDeltaBinaryPackedOnAByteArrayColumn() {
        assertThatThrownBy(() -> decode(Encoding.DELTA_BINARY_PACKED, PhysicalType.BYTE_ARRAY))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("DELTA_BINARY_PACKED is not defined over BYTE_ARRAY; the format defines it over "
                         + "INT32, INT64 only");
    }

    @Test
    void rejectsAlpOnAFixedLengthColumn() {
        assertThatThrownBy(() -> decode(Encoding.ALP, PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ALP is not defined over FIXED_LEN_BYTE_ARRAY; the format defines it over "
                        + "FLOAT, DOUBLE only");
    }

    @Test
    void rejectsRleOnAByteArrayColumn() {
        assertThatThrownBy(() -> decode(Encoding.RLE, PhysicalType.BYTE_ARRAY))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("RLE encodes only boolean values in a data page, not BYTE_ARRAY");
    }

    @Test
    void rejectsADictionaryEncodedPageOfAChunkWithoutADictionary() {
        assertThatThrownBy(() -> decode(Encoding.RLE_DICTIONARY, PhysicalType.INT64))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Dictionary page not found for RLE_DICTIONARY encoding");
    }

    /// A chunk of booleans has no dictionary to parse, so a page claiming one is refused like any
    /// other page of a chunk without one.
    @Test
    void rejectsADictionaryEncodedBooleanPage() {
        assertThatThrownBy(() -> decode(Encoding.PLAIN_DICTIONARY, PhysicalType.BOOLEAN))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Dictionary page not found for PLAIN_DICTIONARY encoding");
    }

    @Test
    void rejectsAPageReferringToADictionaryOfAnotherType() {
        Dictionary dictionary = new Dictionary.IntDictionary(new int[]{ 1 });

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.RLE_DICTIONARY, PhysicalType.INT64, null, 1,
                new byte[]{ 0 }, dictionary))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("A INT64 page refers to a dictionary of another type");
    }

    @Test
    void rejectsABinaryPageReferringToADictionaryOfAnotherType() {
        Dictionary dictionary = new Dictionary.IntDictionary(new int[]{ 1 });

        assertThatThrownBy(() -> DataPages.readRequired(Encoding.RLE_DICTIONARY, PhysicalType.BYTE_ARRAY, null, 1,
                new byte[]{ 0 }, dictionary))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("A BYTE_ARRAY page refers to a dictionary of another type");
    }

    /// `DELTA_BYTE_ARRAY` for `values`, each of [#FIXED_LENGTH] bytes, in the layout the encoder
    /// takes a chunk's values in.
    private static byte[] deltaByteArrayBody(String... values) {
        byte[] data = new byte[values.length * FIXED_LENGTH];
        int[] offsets = new int[values.length + 1];
        for (int i = 0; i < values.length; i++) {
            byte[] bytes = values[i].getBytes(UTF_8);
            System.arraycopy(bytes, 0, data, i * FIXED_LENGTH, FIXED_LENGTH);
            offsets[i + 1] = (i + 1) * FIXED_LENGTH;
        }
        return DeltaByteArrayEncoder.encode(data, offsets, 0, values.length);
    }

    /// Reads a minimal `DATA_PAGE` declaring `encoding` over a required column of `type`, its
    /// body arbitrary: the refusals are all made before the page is decompressed.
    private static Object decode(Encoding encoding, PhysicalType type) {
        return decode(encoding, type, new byte[16]);
    }

    private static Object decode(Encoding encoding, PhysicalType type, byte[] body) {
        return DataPages.readRequired(encoding, type,
                type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? FIXED_LENGTH : null, NUM_VALUES, body);
    }
}
