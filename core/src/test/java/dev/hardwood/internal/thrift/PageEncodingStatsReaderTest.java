/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.ElementType;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

import static dev.hardwood.internal.thrift.ThriftCompactConstants.STOP;
import static dev.hardwood.internal.thrift.ThriftStructBuilder.fieldHeader;
import static dev.hardwood.internal.thrift.ThriftStructBuilder.listHeader;
import static dev.hardwood.internal.thrift.ThriftStructBuilder.longFormListHeader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies how `ColumnMetaData.encoding_stats` (field 13) is decoded, using hand-crafted Thrift
/// Compact Protocol bytes so each shape can be isolated. Headers are composed by
/// [ThriftStructBuilder#fieldHeader] and [ThriftStructBuilder#listHeader]; `i32` values are
/// zigzag varints (`zigzag(-1) = 1`, `zigzag(1) = 2`, `zigzag(2) = 4`, `zigzag(8) = 16`).
class PageEncodingStatsReaderTest {

    /// The `encoding_stats` field header: field 13 as a list. The trailing STOP closing the
    /// ColumnMetaData struct is supplied by each test.
    private static final byte ENCODING_STATS_FIELD = fieldHeader(13, FieldType.LIST);

    /// The header of the next field within a `PageEncodingStats` entry, whose three fields —
    /// `page_type`, `encoding` and `count` — are consecutive `i32`s.
    private static final byte NEXT_I32 = fieldHeader(1, FieldType.I32);

    /// The same one field along as an `i64`, for the shapes that mistype a field or step past
    /// `encoding_stats` into `bloom_filter_offset`.
    private static final byte NEXT_I64 = fieldHeader(1, FieldType.I64);

    private static ThriftCompactReader reader(int... bytes) {
        byte[] b = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            b[i] = (byte) bytes[i];
        }
        return new ThriftCompactReader(ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    void readsEntriesInWrittenOrder() throws IOException {
        // Two entries: (DICTIONARY_PAGE, PLAIN, 1) then (DATA_PAGE, RLE_DICTIONARY, 2).
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(2, ElementType.STRUCT),
                NEXT_I32, 0x04, NEXT_I32, 0x00, NEXT_I32, 0x02, STOP,
                NEXT_I32, 0x00, NEXT_I32, 0x10, NEXT_I32, 0x04, STOP,
                STOP));

        assertThat(metaData.encodingStats()).containsExactly(
                new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 2));
    }

    @Test
    void absentFieldYieldsEmptyList() throws IOException {
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void unrecognizedPageTypeIsCarriedThroughAsUnknown() throws IOException {
        // page_type = 7, which no released version of the format defines. The counts are
        // informational, so this must not make the footer unreadable.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.STRUCT),
                NEXT_I32, 0x0E, NEXT_I32, 0x00, NEXT_I32, 0x02, STOP,
                STOP));

        assertThat(metaData.encodingStats())
                .containsExactly(new PageEncodingStats(PageType.UNKNOWN, Encoding.PLAIN, 1));
    }

    @Test
    void unrecognizedEncodingIsCarriedThroughAsUnknown() throws IOException {
        // encoding = 10, which no released version of the format defines. Like an unrecognized
        // page type, it must not make the footer unreadable.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.STRUCT),
                NEXT_I32, 0x00, NEXT_I32, 0x14, NEXT_I32, 0x02, STOP,
                STOP));

        assertThat(metaData.encodingStats())
                .containsExactly(new PageEncodingStats(PageType.DATA_PAGE, Encoding.UNKNOWN, 1));
    }

    @Test
    void nonListFieldIsSkipped() throws IOException {
        // Field 13 declared as an i32 rather than a list. The field is optional and informational,
        // so it is skipped rather than failing the footer.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(fieldHeader(13, FieldType.I32), 0x02, STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void impossibleEntryCountIsRejectedAsMalformedMetadata() {
        // Long-form list size 2^31, which casts to a negative capacity. Pre-sizing from it throws
        // IllegalArgumentException, which escapes ParquetMetadataReader's catch (IOException) and
        // reaches the caller unchecked and without the file name.
        assertThatThrownBy(() -> ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, longFormListHeader(ElementType.STRUCT),
                0x80, 0x80, 0x80, 0x80, 0x08,
                STOP)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: collection declares 2147483648 elements but only "
                         + "1 bytes remain");
    }

    @Test
    void unknownStructFieldIsSkipped() throws IOException {
        // A field 4 the format does not define today, appended after count.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.STRUCT),
                NEXT_I32, 0x00, NEXT_I32, 0x00, NEXT_I32, 0x02,
                NEXT_I32, 0x02, STOP,
                STOP));

        assertThat(metaData.encodingStats())
                .containsExactly(new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 1));
    }

    @Test
    void missingCountDropsTheField() throws IOException {
        // Entry carrying only page_type and encoding. All three fields are required, so the
        // entry cannot be reconstructed — and since the counts only mean anything as a complete
        // partition of the chunk's pages, the whole field is reported as absent.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.STRUCT),
                NEXT_I32, 0x00, NEXT_I32, 0x00, STOP,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void negativeCountDropsTheField() throws IOException {
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.STRUCT),
                NEXT_I32, 0x00, NEXT_I32, 0x00, NEXT_I32, 0x01, STOP,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void oneMalformedEntryDropsTheWholeField() throws IOException {
        // A well-formed (DATA_PAGE, PLAIN, 1) entry followed by one missing its count. Keeping
        // the first would present 1 page as the chunk's full page inventory when it is not.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(2, ElementType.STRUCT),
                NEXT_I32, 0x00, NEXT_I32, 0x00, NEXT_I32, 0x02, STOP,
                NEXT_I32, 0x04, NEXT_I32, 0x00, STOP,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void nonStructListElementTypeDropsTheField() throws IOException {
        // Field 13 declared as list<i32> carrying the value 1. Elements are read as structs, so
        // decoding it would misread value bytes as field headers; skipping by the declared
        // element type consumes it instead.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.I32),
                0x02,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void wrongWireTypeOnRequiredFieldDropsTheField() throws IOException {
        // page_type declared as i64 rather than i32.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.STRUCT),
                NEXT_I64, 0x00, NEXT_I32, 0x00, NEXT_I32, 0x02, STOP,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
    }

    @Test
    void malformedFieldDoesNotDisturbLaterFields() throws IOException {
        // encoding_stats as list<i32>, then field 14 (bloom_filter_offset, i64) = 8. The skip
        // has to leave the reader exactly on the next field header for this to decode.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(1, ElementType.I32),
                0x02,
                NEXT_I64, 0x10,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
        assertThat(metaData.bloomFilterOffset()).isEqualTo(8L);
    }

    @Test
    void boolListElementTypeDoesNotDisturbLaterFields() throws IOException {
        // The same shape with element type bool, whose elements are a bare byte each rather than
        // a value carried in a field header. Skipping them by field rules would consume none of
        // the two element bytes and read them as the next field header.
        ColumnMetaData metaData = ColumnMetaDataReader.read(reader(
                ENCODING_STATS_FIELD, listHeader(2, ElementType.BOOL),
                0x01, 0x02,
                NEXT_I64, 0x10,
                STOP));

        assertThat(metaData.encodingStats()).isEmpty();
        assertThat(metaData.bloomFilterOffset()).isEqualTo(8L);
    }
}
