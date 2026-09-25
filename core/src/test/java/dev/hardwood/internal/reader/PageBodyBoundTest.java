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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.SnappyCompressor;
import dev.hardwood.internal.encoding.ByteStreamSplitEncoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedEncoder;
import dev.hardwood.internal.encoding.DeltaByteArrayEncoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page whose header declares more values than its body holds fails as malformed.
///
/// The decompressors hand back a buffer owned by the decoding thread and reused across pages, so
/// it is usually longer than the page. Every read of a page's values is bounded by the page's own
/// size; bounded by the buffer instead, a short page reads the tail of an earlier, larger one on
/// the same thread and yields its values as if they were this page's. Each test therefore decodes a
/// large page first, leaving stale bytes behind the short page that follows.
class PageBodyBoundTest {

    private static final ColumnSchema INT64_COLUMN = new ColumnSchema(FieldPath.of("c"), PhysicalType.INT64,
            RepetitionType.REQUIRED, null, 0, 0, 0, null);
    private static final ColumnSchema OPTIONAL_INT64_COLUMN = new ColumnSchema(FieldPath.of("c"),
            PhysicalType.INT64, RepetitionType.OPTIONAL, null, 0, 1, 0, null);
    private static final ColumnSchema REPEATED_INT64_COLUMN = new ColumnSchema(FieldPath.of("c"),
            PhysicalType.INT64, RepetitionType.REPEATED, null, 0, 1, 1, null);
    private static final ColumnSchema BOOLEAN_COLUMN = new ColumnSchema(FieldPath.of("c"), PhysicalType.BOOLEAN,
            RepetitionType.REQUIRED, null, 0, 0, 0, null);
    private static final ColumnSchema BYTE_ARRAY_COLUMN = new ColumnSchema(FieldPath.of("c"),
            PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, null, 0, 0, 0, null);


    /// Thrift value of `PageType.DATA_PAGE_V2`.
    private static final int DATA_PAGE_V2 = 3;
    /// Thrift value of `Encoding.PLAIN`.
    private static final int PLAIN = 0;

    @Test
    void aSnappyPageDeclaringMoreValuesThanItsBodyHoldsFails() {
        PageDecoder decoder = decoder(CompressionCodec.SNAPPY, Encoding.PLAIN);
        decoder.decodePage(snappyPage(125, PlainEncoder.encodeLongs(sequence(125), 0, 125)), null);

        ByteBuffer shortPage = snappyPage(100, PlainEncoder.encodeLongs(sequence(2), 0, 2));

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading INT64 values");
    }

    @Test
    void anUncompressedPageWhoseBodyIsShorterThanItsDeclaredSizeFails() {
        PageDecoder decoder = decoder(CompressionCodec.UNCOMPRESSED, Encoding.PLAIN);
        byte[] fullBody = PlainEncoder.encodeLongs(sequence(125), 0, 125);
        decoder.decodePage(page(125, fullBody, fullBody.length, Encoding.PLAIN), null);

        byte[] shortBody = PlainEncoder.encodeLongs(sequence(2), 0, 2);
        ByteBuffer shortPage = page(100, shortBody, 800, Encoding.PLAIN);

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Uncompressed page size mismatch: expected 800, got 16");
    }

    /// The index stream of a dictionary-encoded page: a bit-packed run of 13 groups (104 indices at
    /// one bit each) of which the body holds only the first group's byte.
    @Test
    void aDictionaryIndexStreamRunningPastItsPageFails() {
        PageDecoder decoder = decoder(CompressionCodec.SNAPPY, Encoding.RLE_DICTIONARY);
        PageDecoder plainDecoder = decoder(CompressionCodec.SNAPPY, Encoding.PLAIN);
        plainDecoder.decodePage(snappyPage(125, PlainEncoder.encodeLongs(sequence(125), 0, 125)), null);

        byte[] indices = { 1, (13 << 1) | 1, 0b0101_0101 };
        ByteBuffer shortPage = snappyPage(100, indices, Encoding.RLE_DICTIONARY);
        Dictionary dictionary = new Dictionary.LongDictionary(new long[]{ 10, 20 });

        assertThatThrownBy(() -> decoder.decodePage(shortPage, dictionary))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Insufficient RLE/Bit-Packing data: decoded 8 of 100 requested values");
    }

    @Test
    void aDictionaryPageDeclaringMoreEntriesThanItsBodyHoldsFails() throws Exception {
        ColumnMetaData metaData = metaData(CompressionCodec.SNAPPY, Encoding.PLAIN);
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            Dictionary large = DictionaryParser.parse(
                    snappyDictionaryPage(125, PlainEncoder.encodeLongs(sequence(125), 0, 125)),
                    INT64_COLUMN, metaData, context);
            assertThat(large.size()).isEqualTo(125);

            byte[] shortBody = PlainEncoder.encodeLongs(sequence(2), 0, 2);
            ByteBuffer shortPage = snappyDictionaryPage(100, shortBody);
            int compressedSize = new SnappyCompressor().compress(shortBody, 0, shortBody.length).length;

            assertThatThrownBy(() -> DictionaryParser.parse(shortPage, INT64_COLUMN, metaData, context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Failed to parse dictionary (type=INT64, numValues=100, uncompressedSize=16"
                            + ", compressedSize=" + compressedSize + ", codec=SNAPPY)")
                    .cause()
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Unexpected EOF while reading INT64 values");
        }
    }

    /// A compressed `DataPageV2` value region is bounded by the page's declared size less its
    /// level sections, not by the length of the buffer it is decompressed into.
    @Test
    void aV2PageWhoseCompressedValueRegionIsShorterThanItsValuesFails() {
        PageDecoder decoder = decoder(INT64_COLUMN, CompressionCodec.SNAPPY, Encoding.PLAIN);
        byte[] fullBody = PlainEncoder.encodeLongs(sequence(125), 0, 125);
        decoder.decodePage(snappyPageV2(125, fullBody), null);

        ByteBuffer shortPage = snappyPageV2(125, Arrays.copyOf(fullBody, 500));

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF while reading INT64 values");
    }

    @Test
    void aRepetitionLevelLengthRunningPastItsPageFails() {
        primeWithALargePage();
        PageDecoder decoder = decoder(REPEATED_INT64_COLUMN, CompressionCodec.SNAPPY, Encoding.PLAIN);
        ByteBuffer shortPage = snappyPage(100, concat(lengthPrefix(600), new byte[8]), Encoding.PLAIN);

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid repetition level length 600: 8 bytes remain in the page");
    }

    @Test
    void aDefinitionLevelLengthRunningPastItsPageFails() {
        primeWithALargePage();
        PageDecoder decoder = decoder(OPTIONAL_INT64_COLUMN, CompressionCodec.SNAPPY, Encoding.PLAIN);
        ByteBuffer shortPage = snappyPage(100, concat(lengthPrefix(600), new byte[8]), Encoding.PLAIN);

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid definition level length 600: 8 bytes remain in the page");
    }

    @Test
    void aPageEndingInsideALevelLengthPrefixFails() {
        primeWithALargePage();
        PageDecoder decoder = decoder(OPTIONAL_INT64_COLUMN, CompressionCodec.SNAPPY, Encoding.PLAIN);
        ByteBuffer shortPage = snappyPage(100, new byte[]{ 1, 0 }, Encoding.PLAIN);

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading the definition level length");
    }

    @Test
    void anRleBooleanLengthRunningPastItsPageFails() {
        primeWithALargePage();
        PageDecoder decoder = decoder(BOOLEAN_COLUMN, CompressionCodec.SNAPPY, Encoding.RLE);
        ByteBuffer shortPage = snappyPage(100, concat(lengthPrefix(600), new byte[]{ (byte) (100 << 1), 1 }),
                Encoding.RLE);

        assertThatThrownBy(() -> decoder.decodePage(shortPage, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid RLE boolean values length 600: 2 bytes remain in the page");
    }

    /// An RLE run of 100 indices at bit width 9, whose value needs two bytes of which the page
    /// holds one.
    @Test
    void anRleRunValueCutShortByItsPageFails() {
        primeWithALargePage();
        PageDecoder decoder = decoder(INT64_COLUMN, CompressionCodec.SNAPPY, Encoding.RLE_DICTIONARY);
        byte[] indices = { 9, (byte) 0xC8, 0x01, 0x05 };
        ByteBuffer shortPage = snappyPage(100, indices, Encoding.RLE_DICTIONARY);
        Dictionary dictionary = new Dictionary.LongDictionary(new long[]{ 10, 20 });

        assertThatThrownBy(() -> decoder.decodePage(shortPage, dictionary))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading RLE run value: expected 2 bytes, got 1");
    }

    /// Each of the following decodes a page, then the same page cut short: the stale tail the
    /// first leaves in the buffer is exactly the bytes the second lacks, so a decoder bounded by
    /// the buffer would return the first page's values in full.
    @Test
    void aDeltaBinaryPackedPageCutShortFails() {
        byte[] fullBody = DeltaBinaryPackedEncoder.encodeLongs(scattered(125), 0, 125);

        assertThatThrownBy(() -> decodeFullThenCut(INT64_COLUMN, Encoding.DELTA_BINARY_PACKED, fullBody))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading miniblock data: expected 40 bytes, got 34");
    }

    @Test
    void aDeltaLengthByteArrayPageCutShortFails() {
        byte[] fullBody = encodeByteArrays(DeltaLengthByteArrayEncoder::encode);

        assertThatThrownBy(() -> decodeFullThenCut(BYTE_ARRAY_COLUMN, Encoding.DELTA_LENGTH_BYTE_ARRAY, fullBody))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading byte array: expected 8, got 0");
    }

    @Test
    void aDeltaByteArrayPageCutShortFails() {
        byte[] fullBody = encodeByteArrays(DeltaByteArrayEncoder::encode);

        assertThatThrownBy(() -> decodeFullThenCut(BYTE_ARRAY_COLUMN, Encoding.DELTA_BYTE_ARRAY, fullBody))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected EOF reading byte array: expected 7, got 6");
    }

    @Test
    void aByteStreamSplitPageCutShortFails() {
        byte[] plain = PlainEncoder.encodeLongs(sequence(125), 0, 125);
        byte[] fullBody = ByteStreamSplitEncoder.encode(plain, 0, 125, Long.BYTES);

        assertThatThrownBy(() -> decodeFullThenCut(INT64_COLUMN, Encoding.BYTE_STREAM_SPLIT, fullBody))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Insufficient data: expected at least 1000 bytes for 125 values of 8 bytes, got 500");
    }

    private static void decodeFullThenCut(ColumnSchema column, Encoding encoding, byte[] fullBody) {
        PageDecoder decoder = decoder(column, CompressionCodec.SNAPPY, encoding);
        decoder.decodePage(snappyPage(125, fullBody, encoding), null);
        decoder.decodePage(snappyPage(125, Arrays.copyOf(fullBody, fullBody.length / 2), encoding), null);
    }

    /// Leaves a 1,000-byte page body in this thread's decompression buffer, so that a shorter page
    /// decoded next has stale bytes behind its end.
    private static void primeWithALargePage() {
        decoder(INT64_COLUMN, CompressionCodec.SNAPPY, Encoding.PLAIN)
                .decodePage(snappyPage(125, PlainEncoder.encodeLongs(sequence(125), 0, 125)), null);
    }

    /// Values whose deltas vary, so that a delta block packs them at a non-zero bit width.
    private static long[] scattered(int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = (i * 7_919L) % 1_000;
        }
        return values;
    }

    private static byte[] encodeByteArrays(ByteArrayEncoding encoding) {
        StringBuilder text = new StringBuilder();
        int[] offsets = new int[126];
        for (int i = 0; i < 125; i++) {
            offsets[i] = text.length();
            text.append("value-").append(i);
        }
        offsets[125] = text.length();
        return encoding.encode(text.toString().getBytes(StandardCharsets.UTF_8), offsets, 0, 125);
    }

    @FunctionalInterface
    private interface ByteArrayEncoding {
        byte[] encode(byte[] data, int[] offsets, int from, int count);
    }

    private static byte[] lengthPrefix(int length) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(length).array();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static long[] sequence(int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = 1_000L + i;
        }
        return values;
    }

    private static PageDecoder decoder(CompressionCodec codec, Encoding encoding) {
        return decoder(INT64_COLUMN, codec, encoding);
    }

    private static PageDecoder decoder(ColumnSchema column, CompressionCodec codec, Encoding encoding) {
        return new PageDecoder(metaData(column.type(), codec, encoding), column, new DecompressorFactory(null));
    }

    private static ColumnMetaData metaData(CompressionCodec codec, Encoding encoding) {
        return metaData(PhysicalType.INT64, codec, encoding);
    }

    private static ColumnMetaData metaData(PhysicalType type, CompressionCodec codec, Encoding encoding) {
        return new ColumnMetaData(type, List.of(encoding), FieldPath.of("c"), codec, 0, 0, 0,
                Map.of(), 0, null, null, null, null, null, List.of(), null);
    }

    private static ByteBuffer snappyPage(int numValues, byte[] body) {
        return snappyPage(numValues, body, Encoding.PLAIN);
    }

    private static ByteBuffer snappyPage(int numValues, byte[] body, Encoding encoding) {
        byte[] compressed = new SnappyCompressor().compress(body, 0, body.length);
        return page(numValues, compressed, body.length, encoding);
    }

    /// A `DATA_PAGE` with `storedBody` as its body and `uncompressedSize` as its declared size.
    private static ByteBuffer page(int numValues, byte[] storedBody, int uncompressedSize, Encoding encoding) {
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, numValues, uncompressedSize, storedBody.length,
                crc(storedBody), encoding);
        return pageBuffer(header.toByteArray(), storedBody);
    }

    /// A `DATA_PAGE_V2` for a `REQUIRED` `PLAIN` column: no level sections, and a value region
    /// stored Snappy-compressed.
    private static ByteBuffer snappyPageV2(int numValues, byte[] values) {
        byte[] compressed = new SnappyCompressor().compress(values, 0, values.length);
        ThriftCompactWriter header = new ThriftCompactWriter();
        short saved = header.pushFieldIdContext();
        header.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        header.writeI32(DATA_PAGE_V2);
        header.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        header.writeI32(values.length);
        header.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        header.writeI32(compressed.length);
        header.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
        header.writeI32(crc(compressed));
        header.writeFieldBegin(8, ThriftCompactConstants.FieldType.STRUCT);
        short savedV2 = header.pushFieldIdContext();
        header.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        header.writeI32(numValues);
        header.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        header.writeI32(0);
        header.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        header.writeI32(numValues);
        header.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
        header.writeI32(PLAIN);
        header.writeFieldBegin(5, ThriftCompactConstants.FieldType.I32);
        header.writeI32(0);
        header.writeFieldBegin(6, ThriftCompactConstants.FieldType.I32);
        header.writeI32(0);
        header.writeBool(7, true);
        header.writeFieldStop();
        header.popFieldIdContext(savedV2);
        header.writeFieldStop();
        header.popFieldIdContext(saved);
        return pageBuffer(header.toByteArray(), compressed);
    }

    private static ByteBuffer snappyDictionaryPage(int numValues, byte[] body) {
        byte[] compressed = new SnappyCompressor().compress(body, 0, body.length);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDictionaryPageV1(header, numValues, body.length, compressed.length, crc(compressed),
                Encoding.PLAIN);
        return pageBuffer(header.toByteArray(), compressed);
    }

    private static int crc(byte[] storedBody) {
        CRC32 crc = new CRC32();
        crc.update(storedBody);
        return (int) crc.getValue();
    }

    private static ByteBuffer pageBuffer(byte[] header, byte[] body) {
        ByteBuffer page = ByteBuffer.allocate(header.length + body.length);
        page.put(header).put(body).flip();
        return page;
    }
}
