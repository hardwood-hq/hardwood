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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OffsetIndexReaderTest {

    @Test
    void readsPageLocationsAndUnencodedSizes() throws IOException {
        byte[] thrift = struct()
                .field(1, FieldType.LIST).structList(pageLocation(100, 40, 0), pageLocation(140, 60, 8))
                .field(2, FieldType.LIST).i64List(512, 768)
                .stop().build();

        OffsetIndex index = read(thrift);

        assertThat(index.pageLocations()).hasSize(2);
        assertThat(index.pageLocations().get(1).offset()).isEqualTo(140L);
        assertThat(index.pageLocations().get(1).compressedPageSize()).isEqualTo(60);
        assertThat(index.pageLocations().get(1).firstRowIndex()).isEqualTo(8L);
        assertThat(index.unencodedByteArrayDataBytes()).containsExactly(512L, 768L);
    }

    @Test
    void reportsOmittedUnencodedSizesAsNull() throws IOException {
        // Field 2 is optional and only defined for BYTE_ARRAY data, so a writer leaves it
        // unset on every other column. That must not read back as a zero-length list.
        byte[] thrift = struct()
                .field(1, FieldType.LIST).structList(pageLocation(100, 40, 0))
                .stop().build();

        assertThat(read(thrift).unencodedByteArrayDataBytes()).isNull();
    }

    @Test
    void readsPresentButEmptyUnencodedSizes() throws IOException {
        byte[] thrift = struct()
                .field(1, FieldType.LIST).structList(pageLocation(100, 40, 0))
                .field(2, FieldType.LIST).i64List()
                .stop().build();

        assertThat(read(thrift).unencodedByteArrayDataBytes()).isEmpty();
    }

    @Test
    void skipsWrongTypedUnencodedSizesField() throws IOException {
        // A malformed file types field 2 as an i64 rather than a list.
        byte[] thrift = struct()
                .field(1, FieldType.LIST).structList(pageLocation(100, 40, 0))
                .field(2, FieldType.I64).i64(4096)
                .field(3, FieldType.I64).i64(7)
                .stop().build();

        OffsetIndex index = read(thrift);

        assertThat(index.unencodedByteArrayDataBytes()).isNull();
        assertThat(index.pageLocations()).hasSize(1);
    }

    @Test
    void skipsStructElementsAtUnencodedSizesFieldWithoutDesync() throws IOException {
        // Field 2 typed list<struct>. Decoding those bytes as varints would leave the
        // cursor mid-struct, so the element type has to drive the skip.
        byte[] thrift = struct()
                .field(1, FieldType.LIST).structList(pageLocation(100, 40, 0))
                .field(2, FieldType.LIST).structList(pageLocation(1, 2, 3), pageLocation(4, 5, 6))
                .field(3, FieldType.I64).i64(7)
                .stop().build();

        OffsetIndex index = read(thrift);

        assertThat(index.unencodedByteArrayDataBytes()).isNull();
        assertThat(index.pageLocations()).hasSize(1);
    }

    @Test
    void skipsBoolElementsAtUnencodedSizesFieldWithoutDesync() throws IOException {
        // A bool element is a bare byte, unlike a bool field whose value rides in its
        // header's type nibble. Skipping these by field rules would consume none of them
        // and read the first element byte as the next field header.
        byte[] thrift = struct()
                .field(1, FieldType.LIST).structList(pageLocation(100, 40, 0))
                .field(2, FieldType.LIST).boolList(true, false, true)
                .field(3, FieldType.I64).i64(7)
                .stop().build();

        OffsetIndex index = read(thrift);

        assertThat(index.unencodedByteArrayDataBytes()).isNull();
        assertThat(index.pageLocations()).hasSize(1);
        assertThat(index.pageLocations().get(0).offset()).isEqualTo(100L);
    }

    @Test
    void rejectsNoPageLocationsForAChunkWithValues() {
        byte[] thrift = struct().field(1, FieldType.LIST).structList().stop().build();

        assertThatThrownBy(() -> OffsetIndexReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(thrift)), chunkWithValues(12)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Malformed Parquet metadata: OffsetIndex.page_locations is empty but the"
                        + " column chunk has 12 values");
    }

    @Test
    void acceptsNoPageLocationsForAChunkWithoutValues() {
        byte[] thrift = struct().field(1, FieldType.LIST).structList().stop().build();

        OffsetIndex index = OffsetIndexReader.read(
                new ThriftCompactReader(ByteBuffer.wrap(thrift)), chunkWithValues(0));

        assertThat(index.pageLocations()).isEmpty();
    }

    private static ColumnMetaData chunkWithValues(long numValues) {
        return new ColumnMetaData(PhysicalType.INT64, List.of(Encoding.PLAIN), FieldPath.of("c"),
                CompressionCodec.UNCOMPRESSED, numValues, 0, 0, Map.of(), 0, null, null, null, null, null,
                List.of(), null);
    }

    /// A `PageLocation` struct body: offset, compressed_page_size, first_row_index.
    private static byte[] pageLocation(long offset, int compressedPageSize, long firstRowIndex) {
        return struct()
                .field(1, FieldType.I64).i64(offset)
                .field(2, FieldType.I32).i32(compressedPageSize)
                .field(3, FieldType.I64).i64(firstRowIndex)
                .stop().build();
    }

    private static OffsetIndex read(byte[] thrift) throws IOException {
        return OffsetIndexReader.read(new ThriftCompactReader(ByteBuffer.wrap(thrift)));
    }

    private static ThriftStructBuilder struct() {
        return new ThriftStructBuilder();
    }
}
