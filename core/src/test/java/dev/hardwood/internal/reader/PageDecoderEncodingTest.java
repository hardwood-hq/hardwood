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

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.encoding.DeltaByteArrayEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
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

/// What [PageDecoder] does with a data page whose declared encoding its column cannot carry.
///
/// A file may say anything. An encoding is defined over some physical types and not others, so a
/// page declaring one its column has no business carrying is a malformed file, and the decoder has
/// to say so rather than decode the bytes into a page of the wrong shape — which is silently wrong
/// twice over, once in the values and once in the [Page] variant handed to the column reader.
/// Being a malformed file rather than a gap in this release, each of these is a
/// [ParquetReadException]; [java.lang.UnsupportedOperationException] is left to mean an encoding
/// no decoder has been written for.
class PageDecoderEncodingTest {

    private static final int NUM_VALUES = 4;
    private static final int FIXED_LENGTH = 4;

    /// The byte-array delta encodings carry a length stream and then bytes, so their values are
    /// byte arrays whatever the column's physical type is. Declared over an integer column they
    /// used to decode into a `ByteArrayPage`, which is not the page an `INT32` column's reader
    /// expects.
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
    void decodesDeltaByteArrayOnAFixedLengthColumn() throws Exception {
        Page page = decode(Encoding.DELTA_BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY,
                deltaByteArrayBody("aaaa", "aaab", "abcd", "abce"));

        assertThat(page).isInstanceOf(Page.ByteArrayPage.class);
        assertThat(Arrays.stream(((Page.ByteArrayPage) page).values())
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

    /// Decodes a minimal `DATA_PAGE` declaring `encoding` over a required column of `type`, its
    /// body arbitrary: the refusals are all made before a value is read.
    private static Page decode(Encoding encoding, PhysicalType type) throws Exception {
        return decode(encoding, type, new byte[16]);
    }

    private static Page decode(Encoding encoding, PhysicalType type, byte[] body) throws Exception {
        ColumnSchema column = new ColumnSchema(FieldPath.of("c"), type, RepetitionType.REQUIRED,
                type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? FIXED_LENGTH : null, 0, 0, 0, null);
        ColumnMetaData metaData = new ColumnMetaData(type, List.of(encoding), FieldPath.of("c"),
                CompressionCodec.UNCOMPRESSED, NUM_VALUES, 0, 0, Map.of(), 0, null, null, null,
                null, null, List.of(), null);

        CRC32 crc = new CRC32();
        crc.update(body);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, NUM_VALUES, body.length, body.length,
                (int) crc.getValue(), encoding);
        byte[] headerBytes = header.toByteArray();

        ByteBuffer page = ByteBuffer.allocate(headerBytes.length + body.length);
        page.put(headerBytes).put(body).flip();

        return new PageDecoder(metaData, column, new DecompressorFactory(null)).decodePage(page, null);
    }
}
