/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Float16LogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/float16_logical_type_test.parquet");

    /// Offset of `duration`'s `LogicalType` union variant in `interval_logical_type_test.parquet`.
    private static final int LOGICAL_TYPE_VARIANT_BYTE = 187;

    // Row 0: 0.0
    // Row 1: 1.0
    // Row 2: -1.5
    // Row 3: 65504.0  (max finite binary16)
    // Row 4: +Infinity
    // Row 5: NaN
    // Row 6: null

    private ColumnSchema halfColumn;
    private int halfIdx;
    private final List<Float> values = new ArrayList<>();

    @BeforeAll
    void readAll() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            halfColumn = fileReader.getFileSchema().getColumn("half");
            halfIdx = halfColumn.columnIndex();
            while (rowReader.hasNext()) {
                rowReader.next();
                values.add(rowReader.isNull("half") ? null : rowReader.getFloat("half"));
            }
        }
    }

    @Test
    void testSchemaReportsFloat16LogicalTypeOnFixedLenByteArray() {
        assertThat(halfColumn.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
        assertThat(halfColumn.typeLength()).isEqualTo(2);
        assertThat(halfColumn.logicalType()).isInstanceOf(LogicalType.Float16Type.class);
    }

    @Test
    void testGetFloatReturnsDecodedValuesForFloat16Column() {
        assertThat(values).hasSize(7);
        assertThat(values.get(0)).isEqualTo(0.0f);
        assertThat(values.get(1)).isEqualTo(1.0f);
        assertThat(values.get(2)).isEqualTo(-1.5f);
        assertThat(values.get(3)).isEqualTo(65504.0f);
        assertThat(values.get(4)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(values.get(5)).isNotNull();
        assertThat(Float.isNaN(values.get(5))).isTrue();
        assertThat(values.get(6)).isNull();
    }

    @Test
    void testGetFloatByIndexReturnsSameValueForFloat16Column() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            assertThat(rowReader.getFloat(halfIdx)).isEqualTo(0.0f);
        }
    }

    /// Primitive accessor convention: NPE when the field is null, just like
    /// `getInt`/`getLong`/`getDouble`/`getBoolean`.
    @Test
    void testGetFloatThrowsNpeOnNullFloat16Value() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            for (int i = 0; i < 7; i++) {
                rowReader.next();
            }
            assertThatThrownBy(() -> rowReader.getFloat("half"))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    void testConvertToFloat16RejectsWrongPhysicalType() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.convertToFloat16(0L, PhysicalType.INT64))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FLOAT16 logical type requires FIXED_LEN_BYTE_ARRAY physical type, got INT64");
    }

    @Test
    void testConvertToFloat16RejectsWrongByteLength() {
        assertThatThrownBy(() ->
                LogicalTypeConverter.convertToFloat16(new byte[4], PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FLOAT16 requires exactly 2 bytes, got 4");
    }

    /// `getFloat` on a non-FLOAT column whose physical type is FLBA but is not
    /// a half-precision payload (here: FLBA(12) annotated INTERVAL) is the caller
    /// asking a column for a type it does not hold, so it keeps
    /// `IllegalArgumentException` and names no file — the file is not at fault — and
    /// says what the column actually is rather than the width FLOAT16 would need.
    @Test
    void testGetFloatOnNonFloat16FlbaColumnRaisesIllegalArgumentException() throws IOException {
        Path intervalFile = Paths.get("src/test/resources/interval_logical_type_test.parquet");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(intervalFile));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            assertThatThrownBy(() -> rowReader.getFloat("duration"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[interval_logical_type_test.parquet] Column 'duration' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated INTERVAL,"
                            + " which cannot be read as a float");
        }
    }

    /// `FilterPredicate.gt(col, 0.5f)` against a FLOAT16 column dispatches in the
    /// resolver to a `Float16Predicate`, which decodes the 2-byte payload before
    /// comparing. From the caller's perspective the call is identical to filtering
    /// a physical FLOAT column.
    @Test
    void testFloatPredicateOnFloat16ColumnFiltersByDecodedValue() throws IOException {
        // half values: 0.0, 1.0, -1.5, 65504.0, +Inf, NaN, null.
        // Float.compare orders NaN after all finite values and +Inf, and treats
        // +0.0 > -0.0; with `gt 0.5f` we keep 1.0, 65504.0, +Inf, NaN — null drops.
        List<Float> kept = new ArrayList<>();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.buildRowReader()
                     .filter(FilterPredicate.gt("half", 0.5f))
                     .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                kept.add(rowReader.getFloat("half"));
            }
        }
        assertThat(kept).hasSize(4);
        assertThat(kept.get(0)).isEqualTo(1.0f);
        assertThat(kept.get(1)).isEqualTo(65504.0f);
        assertThat(kept.get(2)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(Float.isNaN(kept.get(3))).isTrue();
    }

    /// `getFloat` must work through the record-matcher
    /// delegating wrapper installed by `buildRowReader().filter(...)`.
    @Test
    void testGetFloatThroughRecordMatcher() throws IOException {
        // id=1..7 maps to half=0.0, 1.0, -1.5, 65504.0, +Inf, NaN, null. With id>3
        // the filtered reader yields rows id=4..7.
        List<Float> filtered = new ArrayList<>();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 3))
                     .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                filtered.add(rowReader.isNull("half") ? null : rowReader.getFloat("half"));
            }
        }
        assertThat(filtered).hasSize(4);
        assertThat(filtered.get(0)).isEqualTo(65504.0f);
        assertThat(filtered.get(1)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(filtered.get(2)).isNotNull();
        assertThat(Float.isNaN(filtered.get(2))).isTrue();
        assertThat(filtered.get(3)).isNull();
    }

    /// Byte 187 is the `LogicalType` union variant on `duration`, a `FIXED_LEN_BYTE_ARRAY(12)`
    /// column the file annotates `INTERVAL` (variant 9). Relabelling it `FLOAT16` (variant 15)
    /// leaves the values untouched and the annotation contradicting the width.
    ///
    /// The offset is a property of the checked-in bytes, so regenerating the fixture moves
    /// it. The test then fails and reports the bytes it did produce, which is what a new
    /// offset is derived from.
    ///
    /// `FLOAT16` is defined as a two-byte payload, so a column annotated `FLOAT16` that
    /// declares twelve bytes is invalid. The format says to read past the annotation rather
    /// than refuse the file, so the column is reported and read as the
    /// `FIXED_LEN_BYTE_ARRAY` it physically is.
    @Test
    void testAnAnnotationTheWidthContradictsIsDropped() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                     InputFile.of(ByteBuffer.wrap(relabelled(0xFC))));
             RowReader rowReader = fileReader.rowReader()) {

            ColumnSchema duration = fileReader.getFileSchema().getColumn("duration");
            assertThat(duration.logicalType()).isNull();
            assertThat(duration.type()).isEqualTo(PhysicalType.FIXED_LEN_BYTE_ARRAY);
            assertThat(duration.typeLength()).isEqualTo(12);

            rowReader.next();
            // The bytes were never in question, and now nothing claims to decode them.
            assertThat(rowReader.getBinary("duration")).hasSize(12);
            assertThat(rowReader.getValue("duration")).isInstanceOf(byte[].class);

            // Asking an unannotated FIXED_LEN_BYTE_ARRAY for a float is the caller's
            // mistake, and says so without mentioning FLOAT16 — which no longer applies.
            assertThatThrownBy(() -> rowReader.getFloat("duration"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[<memory>] Column 'duration' is FIXED_LEN_BYTE_ARRAY,"
                            + " which cannot be read as a float");
        }
    }

    /// The same relabelling, aimed at the annotations the other conversions read.
    /// `duration` is `FIXED_LEN_BYTE_ARRAY(12)`, so none of `DATE`, `TIME` or `TIMESTAMP`
    /// can be read from it whatever the values say, and each is dropped the same way. The
    /// accessor that would have converted then fails as it does on any unannotated column,
    /// rather than reporting the file.
    ///
    /// @param variantByte the `LogicalType` union variant to relabel `duration` to
    @ParameterizedTest
    @MethodSource("contradictedAnnotations")
    void testEveryContradictedAnnotationIsDropped(int variantByte) throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                     InputFile.of(ByteBuffer.wrap(relabelled(variantByte))));
             RowReader rowReader = fileReader.rowReader()) {

            assertThat(fileReader.getFileSchema().getColumn("duration").logicalType()).isNull();
            rowReader.next();
            assertThat(rowReader.getBinary("duration")).hasSize(12);
        }
    }

    /// Union variant bytes: the low nibble is the STRUCT wire type, the high nibble the
    /// field-id delta from zero — so `0x6C` is `DATE` (6), `0x7C` `TIME` (7), `0x8C`
    /// `TIMESTAMP` (8) and `0xEC` `UUID` (14). The first three cannot be read from a
    /// `FIXED_LEN_BYTE_ARRAY` at all; `UUID` can, but only at sixteen bytes.
    static Stream<Arguments> contradictedAnnotations() {
        return Stream.of(Arguments.of(0x6C), Arguments.of(0x7C), Arguments.of(0x8C),
                Arguments.of(0xEC));
    }

    /// `interval_logical_type_test.parquet` with `duration`'s `LogicalType` union variant
    /// byte overwritten, leaving the values and the declared width untouched.
    private static byte[] relabelled(int variantByte) throws IOException {
        byte[] bytes = Files.readAllBytes(
                Paths.get("src/test/resources/interval_logical_type_test.parquet"));
        bytes[LOGICAL_TYPE_VARIANT_BYTE] = (byte) variantByte;
        return bytes;
    }

    /// The nested reader answers a caller the same way the flat one does. The two decide it
    /// from the same facts by different routes — the flat reader per projected column, the
    /// nested view per primitive — so a test that only exercised one would not notice them
    /// drifting apart.
    @Test
    void testNestedReaderRejectsAFloatReadTheSameWay() throws IOException {
        Path nested = Paths.get("src/test/resources/nested_struct_test.parquet");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(nested));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            assertThatThrownBy(() -> rowReader.getFloat("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Column 'id' is INT32,"
                            + " which cannot be read as a float");

            // The by-index half of the same method, which reaches the column by a
            // different route and used to answer with a rewrapped ClassCastException.
            assertThatThrownBy(() -> rowReader.getFloat(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Column 'id' is INT32,"
                            + " which cannot be read as a float");

            // And a field one level down, read through the struct flyweight.
            assertThatThrownBy(() -> rowReader.getStruct("address").getFloat("city"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Column 'city' is BYTE_ARRAY"
                            + " annotated STRING, which cannot be read as a float");
        }
    }
}
