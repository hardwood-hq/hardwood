/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetReadException;

import static dev.hardwood.internal.thrift.ThriftCompactConstants.STOP;
import static dev.hardwood.internal.thrift.ThriftStructBuilder.fieldHeader;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/// Verifies that the Thrift metadata readers reject malformed/adversarial
/// sizes, counts and offsets with a controlled error instead of letting a
/// negative value reach a buffer allocation or slice downstream (see the
/// cross-reader compatibility matrix discussed on dev@parquet, May 2026).
///
/// Inputs are hand-crafted Thrift Compact Protocol bytes. Field headers are composed by
/// [ThriftStructBuilder#fieldHeader]; `i32`/`i64` values are zigzag varints (`zigzag(-1) = 1`,
/// `zigzag(10) = 20`, `zigzag(8) = 16`).
class MalformedMetadataValidationTest {

    /// The header of the next `i32` field, which every `PageHeader` shape below steps through.
    private static final byte NEXT_I32 = fieldHeader(1, FieldType.I32);

    private static ThriftCompactReader reader(int... bytes) {
        byte[] b = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            b[i] = (byte) bytes[i];
        }
        return reader(b);
    }

    private static ThriftCompactReader reader(byte[] bytes) {
        return new ThriftCompactReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    void negativeCompressedPageSizeRejected() {
        // PageHeader: field1 type=DATA_PAGE(0), field2 uncompressed=10, field3 compressed=-1
        assertThatThrownBy(() -> PageHeaderReader.read(
                reader(NEXT_I32, 0x00, NEXT_I32, 0x14,
                        NEXT_I32, 0x01, STOP)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader.compressed_page_size \u2014 must be non-negative but was -1");
    }

    @Test
    void negativeUncompressedPageSizeRejected() {
        // PageHeader: field1 type=DATA_PAGE(0), field2 uncompressed=-1
        assertThatThrownBy(() -> PageHeaderReader.read(
                reader(NEXT_I32, 0x00, NEXT_I32, 0x01, STOP)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader.uncompressed_page_size \u2014 must be non-negative but was -1");
    }

    @Test
    void negativeDataPageOffsetRejected() {
        // ColumnMetaData: field9 data_page_offset (i64) = -1
        assertThatThrownBy(() -> ColumnMetaDataReader.read(
                reader(fieldHeader(9, FieldType.I64), 0x01, STOP)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnMetaData.data_page_offset \u2014 must be non-negative but was -1");
    }

    @Test
    void unknownPageTypeRejected() {
        // PageHeader: field1 type=4, which no released version of the format defines. Unlike
        // encoding_stats, a page we cannot classify cannot be decoded either, so it must fail.
        assertThatThrownBy(() -> PageHeaderReader.read(
                reader(NEXT_I32, 0x08, NEXT_I32, 0x14,
                        NEXT_I32, 0x10, STOP)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("PageHeader has unknown page type: 4");
    }

    @Test
    void negativeOffsetIndexLengthRejected() {
        // ColumnChunk: field5 offset_index_length (i32) = -1, which RowGroupIndexBuffers would
        // otherwise pass straight to ByteBuffer.slice().
        byte[] chunk = new ThriftStructBuilder()
                .field(5, FieldType.I32).i32(-1)
                .stop().build();
        assertThatThrownBy(() -> ColumnChunkReader.read(reader(chunk)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnChunk.offset_index_length \u2014 must be non-negative but was -1");
    }

    @Test
    void oversizedBinaryLengthRejected() {
        // SchemaElement: field4 name declares 2^32 + 2 bytes. Truncated to int that is 2, so the
        // read would hand back a two-byte name and leave the cursor inside the field, taking the
        // rest of it for the headers of the fields behind it.
        byte[] element = new ThriftStructBuilder()
                .field(4, FieldType.BINARY).raw(0x82, 0x80, 0x80, 0x80, 0x10)
                .stop().build();
        assertThatThrownBy(() -> SchemaElementReader.read(reader(element)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: binary value declares 4294967298 bytes but only 1 "
                         + "remain");
    }

    @Test
    void binaryLengthPastTheBufferRejected() {
        // The same field declaring 8 MiB in a footer of a few bytes. The length is a valid int,
        // so it reaches `new byte[length]` intact — this bound is all that stands between a
        // five-byte varint and the allocation.
        byte[] element = new ThriftStructBuilder()
                .field(4, FieldType.BINARY).raw(0x80, 0x80, 0x80, 0x04)
                .stop().build();
        assertThatThrownBy(() -> SchemaElementReader.read(reader(element)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: binary value declares 8388608 bytes but only 1 "
                         + "remain");
    }

    @Test
    void splitFileColumnChunkParsesButCannotBeRead() {
        // ColumnChunk: field1 file_path names another file, so every offset in this chunk's
        // metadata addresses that file rather than the one being read. The footer still parses —
        // the metadata of such a file stays inspectable — and the refusal lands where the data
        // would be read.
        byte[] chunk = new ThriftStructBuilder()
                .field(1, FieldType.BINARY).binary("data-2.parquet".getBytes(UTF_8))
                .stop().build();
        ColumnChunk columnChunk = assertDoesNotThrow(() -> ColumnChunkReader.read(reader(chunk)));
        assertThat(columnChunk.filePath()).isEqualTo("data-2.parquet");
        assertThatThrownBy(columnChunk::requireSameFile)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column chunk stores its data in a separate file ('data-2.parquet'); the "
                         + "split-file layout is not supported")
                ;
    }

    @Test
    void emptyFilePathIsThisFile() {
        // An empty file_path is the writer naming the file it is writing, not a split layout.
        byte[] chunk = new ThriftStructBuilder()
                .field(1, FieldType.BINARY).binary(new byte[0])
                .field(5, FieldType.I32).i32(64)
                .stop().build();
        ColumnChunk columnChunk = assertDoesNotThrow(() -> ColumnChunkReader.read(reader(chunk)));
        assertThat(columnChunk.offsetIndexLength()).isEqualTo(64);
        assertDoesNotThrow(columnChunk::requireSameFile);
    }

    @Test
    void absentFilePathIsThisFile() {
        // The field is optional; a chunk that omits it makes no claim about another file.
        byte[] chunk = new ThriftStructBuilder()
                .field(5, FieldType.I32).i32(64)
                .stop().build();
        ColumnChunk columnChunk = assertDoesNotThrow(() -> ColumnChunkReader.read(reader(chunk)));
        assertThat(columnChunk.filePath()).isEmpty();
        assertDoesNotThrow(columnChunk::requireSameFile);
    }

    @Test
    void requiredListOfWrongElementTypeRejected() {
        // FileMetaData: field2 schema declared as list<i32>. Reading the elements as
        // SchemaElement structs would take value bytes for field headers and misparse the
        // rest of the footer, so the whole read fails instead.
        byte[] footer = new ThriftStructBuilder()
                .field(2, FieldType.LIST).i32List(1, 2, 3)
                .stop().build();
        assertThatThrownBy(() -> FileMetaDataReader.read(reader(footer)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("FileMetaData.schema \u2014 wrong Thrift element type 0x5 (expected 0xc)");
    }

    @Test
    void optionalListOfWrongElementTypeIsSkipped() {
        // SizeStatistics: field2 repetition_level_histogram declared as list<i32>. The field is
        // optional, so it is reported absent — and field3 behind it still parses, which is what
        // skipping the elements by their declared type buys.
        byte[] stats = new ThriftStructBuilder()
                .field(2, FieldType.LIST).i32List(1, 2)
                .field(3, FieldType.LIST).i64List(7L, 8L)
                .stop().build();
        SizeStatistics sizeStatistics = assertDoesNotThrow(() -> SizeStatisticsReader.read(reader(stats)));
        assertThat(sizeStatistics.repetitionLevelHistogram()).isNull();
        assertThat(sizeStatistics.definitionLevelHistogram()).containsExactly(7L, 8L);
    }

    @Test
    void oversizedMapSizeRejected() {
        // An unknown field of type map declaring 2^32 + 2 entries: truncated to int that is 2,
        // so the skip would stop early and read the remaining entries as fields of this struct.
        byte[] metaData = new ThriftStructBuilder()
                .field(100, FieldType.MAP).raw(0x82, 0x80, 0x80, 0x80, 0x10)
                .stop().build();
        assertThatThrownBy(() -> ColumnMetaDataReader.read(reader(metaData)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: collection declares 4294967298 elements but only "
                         + "1 bytes remain");
    }

    @Test
    void columnIndexWithShortPerPageArrayRejected() {
        // ColumnIndex: two pages, but only one null count. PageFilterEvaluator indexes
        // null_counts with a page count that comes from elsewhere.
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.LIST).boolList(false, false)
                .field(2, FieldType.LIST).binaryList(new byte[]{ 1 }, new byte[]{ 2 })
                .field(3, FieldType.LIST).binaryList(new byte[]{ 3 }, new byte[]{ 4 })
                .field(5, FieldType.LIST).i64List(0L)
                .stop().build();
        assertThatThrownBy(() -> ColumnIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: ColumnIndex.null_counts has length 1 but the "
                         + "index describes 2 pages")
                ;
    }

    @Test
    void columnIndexWithConsistentPerPageArraysParses() {
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.LIST).boolList(false, true)
                .field(2, FieldType.LIST).binaryList(new byte[]{ 1 }, new byte[]{ 2 })
                .field(3, FieldType.LIST).binaryList(new byte[]{ 3 }, new byte[]{ 4 })
                .field(5, FieldType.LIST).i64List(0L, 5L)
                .stop().build();
        ColumnIndex columnIndex = assertDoesNotThrow(() -> ColumnIndexReader.read(reader(index)));
        assertThat(columnIndex.nullPages()).containsExactly(false, true);
        assertThat(columnIndex.nullCounts()).containsExactly(0L, 5L);
    }

    @Test
    void mistypedBooleanFieldDoesNotDesyncTheStruct() {
        // Statistics: field7 is_max_value_exact is a bool, whose value a compact field header
        // carries in its own type nibble. Declared as binary it has a body, and leaving that
        // body unread would take it for the next field header.
        byte[] statistics = new ThriftStructBuilder()
                .field(7, FieldType.BINARY).binary(new byte[]{ 9 })
                .field(9, FieldType.I64).i64(3)
                .stop().build();
        Statistics parsed = assertDoesNotThrow(() -> StatisticsReader.read(reader(statistics)));
        assertThat(parsed.nanCount()).isEqualTo(3L);
        assertThat(parsed.isMaxValueExact()).isTrue();
    }

    @Test
    void logicalTypeUnionWithoutVariantRejected() {
        // SchemaElement.logicalType = TimeType whose `unit` union has no member set. The union
        // has no default, so there is nothing to report but a failure.
        byte[] unit = new ThriftStructBuilder().stop().build();
        byte[] timeType = new ThriftStructBuilder()
                .field(2, FieldType.STRUCT).nested(unit)
                .stop().build();
        byte[] logicalType = new ThriftStructBuilder()
                .field(7, FieldType.STRUCT).nested(timeType)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("TimeUnit has no variant set");
    }

    @Test
    void logicalTypeUnionWithTwoVariantsRejected() {
        // The same union with both MILLIS and MICROS set. Which unit the column uses is then
        // ambiguous, and the byte after the first variant is another field header rather than
        // STOP — reading on would take the second variant's value for a field of the enclosing
        // TimeType and misparse the rest of the schema element.
        byte[] emptyStruct = new ThriftStructBuilder().stop().build();
        byte[] unit = new ThriftStructBuilder()
                .field(1, FieldType.STRUCT).nested(emptyStruct)
                .field(2, FieldType.STRUCT).nested(emptyStruct)
                .stop().build();
        byte[] timeType = new ThriftStructBuilder()
                .field(2, FieldType.STRUCT).nested(unit)
                .stop().build();
        byte[] logicalType = new ThriftStructBuilder()
                .field(7, FieldType.STRUCT).nested(timeType)
                .stop().build();
        assertThatThrownBy(() -> LogicalTypeReader.read(reader(logicalType)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("TimeUnit has more than one variant set")
                ;
    }

    @Test
    void geographyAlgorithmIsReadAsAnEnumNotAUnion() {
        // GeographyType.algorithm is `optional EdgeInterpolationAlgorithm`, a Thrift *enum*
        // (SPHERICAL=0 .. KARNEY=4), so it is an i32 on the wire. Reading it as a struct made
        // every non-default algorithm fall through to the SPHERICAL default in silence.
        byte[] geography = new ThriftStructBuilder()
                .field(2, FieldType.I32).i32(2)
                .stop().build();
        byte[] logicalType = new ThriftStructBuilder()
                .field(18, FieldType.STRUCT).nested(geography)
                .stop().build();
        LogicalType parsed = assertDoesNotThrow(() -> LogicalTypeReader.read(reader(logicalType)));
        assertThat(parsed).isInstanceOf(LogicalType.GeographyType.class);
        assertThat(((LogicalType.GeographyType) parsed).edgeInterpolation())
                .isEqualTo(LogicalType.EdgeInterpolationAlgorithm.THOMAS);
    }

    @Test
    void geographyWithoutAlgorithmDefaultsToSpherical() {
        // The field is optional and the spec names SPHERICAL as its default.
        byte[] geography = new ThriftStructBuilder()
                .field(1, FieldType.BINARY).binary("OGC:CRS84".getBytes(UTF_8))
                .stop().build();
        byte[] logicalType = new ThriftStructBuilder()
                .field(18, FieldType.STRUCT).nested(geography)
                .stop().build();
        LogicalType parsed = assertDoesNotThrow(() -> LogicalTypeReader.read(reader(logicalType)));
        assertThat(((LogicalType.GeographyType) parsed).edgeInterpolation())
                .isEqualTo(LogicalType.EdgeInterpolationAlgorithm.SPHERICAL);
    }

    @Test
    void unknownGeographyAlgorithmIsReportedAsUnknown() {
        // An algorithm the format adds after this release. It says how to interpolate between
        // the column's values, not how to decode them, so the file stays readable.
        byte[] geography = new ThriftStructBuilder()
                .field(2, FieldType.I32).i32(5)
                .stop().build();
        byte[] logicalType = new ThriftStructBuilder()
                .field(18, FieldType.STRUCT).nested(geography)
                .stop().build();
        LogicalType parsed = assertDoesNotThrow(() -> LogicalTypeReader.read(reader(logicalType)));
        assertThat(((LogicalType.GeographyType) parsed).edgeInterpolation())
                .isEqualTo(LogicalType.EdgeInterpolationAlgorithm.UNKNOWN);
    }

    @Test
    void columnIndexWithRaggedHistogramRejected() {
        // Two pages and five histogram entries: no per-page stride divides that, so
        // ColumnIndex.repetitionLevelHistogram(page) has no slice it could return.
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.LIST).boolList(false, false)
                .field(2, FieldType.LIST).binaryList(new byte[]{ 1 }, new byte[]{ 2 })
                .field(3, FieldType.LIST).binaryList(new byte[]{ 3 }, new byte[]{ 4 })
                .field(6, FieldType.LIST).i64List(0L, 1L, 2L, 3L, 4L)
                .stop().build();
        assertThatThrownBy(() -> ColumnIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: ColumnIndex.repetition_level_histograms has "
                         + "length 5 for 2 pages, which is not a whole number of entries per page")
                ;
    }

    @Test
    void nullPagesOfWrongElementTypeRejected() {
        // ColumnIndex.null_pages declared as list<i32>. It is required and its length defines
        // the page count every other member is checked against, so there is nothing to fall
        // back to — and its elements are four bytes where bools are one, which would desync
        // the rest of the struct if decoded anyway.
        byte[] index = new ThriftStructBuilder()
                .field(1, FieldType.LIST).i32List(0, 1)
                .stop().build();
        assertThatThrownBy(() -> ColumnIndexReader.read(reader(index)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("ColumnIndex.null_pages — wrong Thrift element type 0x5"
                        + " (expected 0x1 or 0x2)");
    }

    @Test
    void validPageHeaderStillParses() {
        // PageHeader: field1 type=DATA_PAGE(0), field2 uncompressed=10, field3 compressed=8
        PageHeader header = assertDoesNotThrow(() -> PageHeaderReader.read(
                reader(NEXT_I32, 0x00, NEXT_I32, 0x14,
                        NEXT_I32, 0x10, STOP)));
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE);
        assertThat(header.uncompressedPageSize()).isEqualTo(10);
        assertThat(header.compressedPageSize()).isEqualTo(8);
    }
}
