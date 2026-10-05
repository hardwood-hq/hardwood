/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.writer.LogicalTypeValueRange;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/// The range a column's logical-type annotation declares, enforced by both write APIs: a value
/// outside it produces a file whose values fall outside the range its own annotation declares,
/// and statistics bounds describing values that annotation says cannot exist.
class WriterAnnotationRangeTest {

    /// Every annotation that narrows what its physical type may hold, with the extremes of the
    /// range it declares.
    ///
    /// The bounds are constants rather than values read back out of `LogicalTypeValueRange`. That
    /// is the point of the table: the writer derives them by arithmetic on the annotation, and a
    /// test that derived them the same way would agree with it by construction whatever the
    /// arithmetic said.
    ///
    /// The parameters are crossed rather than sampled where crossing them is what a defect would
    /// hide behind — both signednesses of each narrow `INT` width, both settings of a `TIME`'s
    /// UTC flag against each unit (the flag changes no bound, and a bound that moved with it
    /// would be a defect), and a `DECIMAL` at the smallest precision and at the largest each
    /// carrier holds, with a scale of zero and a scale as large as the precision.
    static Stream<Arguments> boundedColumns() {
        return Stream.of(
                arguments(PhysicalType.INT32, LogicalType.intType(8, true), -128L, 127L),
                arguments(PhysicalType.INT32, LogicalType.intType(8, false), 0L, 255L),
                arguments(PhysicalType.INT32, LogicalType.intType(16, true), -32_768L, 32_767L),
                arguments(PhysicalType.INT32, LogicalType.intType(16, false), 0L, 65_535L),

                arguments(PhysicalType.INT32, LogicalType.decimal(1, 0), -9L, 9L),
                arguments(PhysicalType.INT32, LogicalType.decimal(1, 1), -9L, 9L),
                arguments(PhysicalType.INT32, LogicalType.decimal(9, 2), -999_999_999L, 999_999_999L),
                arguments(PhysicalType.INT32, LogicalType.decimal(9, 9), -999_999_999L, 999_999_999L),
                arguments(PhysicalType.INT64, LogicalType.decimal(1, 0), -9L, 9L),
                arguments(PhysicalType.INT64, LogicalType.decimal(18, 4),
                        -999_999_999_999_999_999L, 999_999_999_999_999_999L),
                arguments(PhysicalType.INT64, LogicalType.decimal(18, 18),
                        -999_999_999_999_999_999L, 999_999_999_999_999_999L),

                arguments(PhysicalType.INT32, LogicalType.time(true, LogicalType.TimeUnit.MILLIS),
                        0L, 86_399_999L),
                arguments(PhysicalType.INT32, LogicalType.time(false, LogicalType.TimeUnit.MILLIS),
                        0L, 86_399_999L),
                arguments(PhysicalType.INT64, LogicalType.time(false, LogicalType.TimeUnit.MICROS),
                        0L, 86_399_999_999L),
                arguments(PhysicalType.INT64, LogicalType.time(true, LogicalType.TimeUnit.MICROS),
                        0L, 86_399_999_999L),
                arguments(PhysicalType.INT64, LogicalType.time(true, LogicalType.TimeUnit.NANOS),
                        0L, 86_399_999_999_999L),
                arguments(PhysicalType.INT64, LogicalType.time(false, LogicalType.TimeUnit.NANOS),
                        0L, 86_399_999_999_999L));
    }

    /// Annotations that narrow nothing, so every value of the physical type stays writable. The
    /// unsigned full widths are the interesting ones: a value above the signed maximum is spelled
    /// as a negative, which is also how the reader returns it.
    static Stream<Arguments> unboundedColumns() {
        return Stream.of(
                arguments(PhysicalType.INT32, LogicalType.intType(32, false), -1L),
                arguments(PhysicalType.INT64, LogicalType.intType(64, false), -1L),
                arguments(PhysicalType.INT32, LogicalType.date(), Integer.MIN_VALUE + 0L),
                arguments(PhysicalType.INT64, LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS),
                        Long.MIN_VALUE));
    }

    /// The bounded table's rows really are bounded, and the unbounded table's really are not, as
    /// the writer resolves them — so a row filed under the wrong one fails rather than passing
    /// vacuously.
    @ParameterizedTest(name = "{1}")
    @MethodSource("boundedColumns")
    void aBoundedRowIsBounded(PhysicalType type, LogicalType annotation, long min, long max) {
        assertThat(LogicalTypeValueRange.of(single(type, annotation).getColumn(0)).isBounded())
                .as("%s bounds its column", annotation)
                .isTrue();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("boundedColumns")
    void theExtremesOfTheDeclaredRangeAreWritable(PhysicalType type, LogicalType annotation, long min, long max)
            throws Exception {
        FileSchema schema = single(type, annotation);

        InMemoryOutputFile out = writeBatch(schema, batch -> fill(batch, type, min, max));

        assertThat(readIntegers(out, type)).containsExactly(min, max);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("boundedColumns")
    void aValueOutsideTheDeclaredRangeIsRejectedByTheColumnarApi(PhysicalType type, LogicalType annotation,
            long min, long max) {
        FileSchema schema = single(type, annotation);

        for (long rejected : new long[] { min - 1, max + 1 }) {
            assertThatThrownBy(() -> writeBatch(schema, batch -> fill(batch, type, rejected)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column 0 (v) has value " + rejected + " at row 0, out of range for a "
                            + annotation + " column");
        }
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("boundedColumns")
    void aValueOutsideTheDeclaredRangeIsRejectedByTheRowApi(PhysicalType type, LogicalType annotation,
            long min, long max) {
        FileSchema schema = single(type, annotation);

        for (long rejected : new long[] { min - 1, max + 1 }) {
            assertThatThrownBy(() -> writeRow(schema, row -> set(row, type, rejected)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field v: " + rejected + " is out of range for a " + annotation
                            + " column");
        }
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("unboundedColumns")
    void anAnnotationThatNarrowsNothingKeepsEveryValueWritable(PhysicalType type, LogicalType annotation,
            long value) throws Exception {
        FileSchema schema = single(type, annotation);

        InMemoryOutputFile out = writeBatch(schema, batch -> fill(batch, type, value));

        assertThat(readIntegers(out, type)).containsExactly(value);
    }

    /// The columnar rejection names the row it found, so a caller handing over a long array knows
    /// which value to look at.
    @Test
    void theColumnarRejectionNamesTheOffendingRow() {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.intType(8, false));

        assertThatThrownBy(() -> writeBatch(schema, batch -> batch.ints(0, new int[] { 0, 1, 300 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) has value 300 at row 2, out of range for a UINT_8 column");
    }

    /// A `TIME` is the elapsed time after midnight, so a full day is already the next day's zero
    /// and the reader materializes no [java.time.LocalTime] for it. The bound excludes it, which
    /// also rejects the `24:00:00` spelling some producers emit for the end of a day.
    @Test
    void aFullDayIsOutsideATimeColumn() {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.time(true, LogicalType.TimeUnit.MILLIS));

        assertThatThrownBy(() -> writeBatch(schema, batch -> batch.ints(0, new int[] { 86_400_000 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) has value 86400000 at row 0, out of range for a TIME(MILLIS, "
                         + "UTC) column");
        assertThatThrownBy(() -> writeRow(schema, row -> row.setInt("v", 86_400_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: 86400000 is out of range for a TIME(MILLIS, UTC) column");
    }

    /// The values at null rows are ignored by the writer, so they are not range-checked either:
    /// a caller filling the slots of a nullable column with a placeholder must not be rejected
    /// for a value that is never encoded.
    @Test
    void theValueAtANullRowIsNotChecked() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL,
                        c -> c.logicalType(LogicalType.intType(8, false)))
                .build();

        InMemoryOutputFile out = writeBatch(schema, batch -> batch.ints(0, new int[] { 300, 5 },
                Validity.ofNulls(new boolean[] { true, false })));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("v")).isTrue();
            rows.next();
            assertThat(rows.getInt("v")).isEqualTo(5);
        }
    }

    /// A `DECIMAL` over a binary type bounds the magnitude of the two's complement unscaled value
    /// its bytes carry, whatever the width of the column those bytes sit in.
    @Test
    void aBinaryDecimalRejectsAnUnscaledValueBeyondItsPrecision() {
        FileSchema variable = single(PhysicalType.BYTE_ARRAY, LogicalType.decimal(4, 0));
        FileSchema fixed = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                        c -> c.typeLength(2).logicalType(LogicalType.decimal(4, 0)))
                .build();
        byte[] tooLarge = { 0x30, 0x39 };      // 12345, five digits against a declared four

        assertThatThrownBy(() -> writeBatch(variable, batch -> batch.bytes(0, new byte[][] { tooLarge })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) has a value at row 0 that is not an unscaled value the column's "
                         + "DECIMAL(4, 0) can hold");
        assertThatThrownBy(() -> writeBatch(fixed, batch -> batch.fixed(0, new byte[][] { tooLarge })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) has a value at row 0 that is not an unscaled value the column's "
                         + "DECIMAL(4, 0) can hold");
        assertThatThrownBy(() -> writeRow(fixed, row -> row.setBinary("v", tooLarge)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: the value is not an unscaled value the column's DECIMAL(4, 0) can "
                         + "hold");
    }

    /// The largest unscaled value the precision holds is writable, negatives included: the bound
    /// is on the magnitude of the value the bytes denote, not on the bytes.
    @Test
    void aBinaryDecimalTakesTheExtremesOfItsPrecision() throws Exception {
        FileSchema schema = single(PhysicalType.BYTE_ARRAY, LogicalType.decimal(4, 0));
        byte[] largest = { 0x27, 0x0f };            // 9999
        byte[] smallest = { (byte) 0xd8, (byte) 0xf1 };  // -9999

        InMemoryOutputFile out = writeBatch(schema,
                batch -> batch.bytes(0, new byte[][] { largest, smallest }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getBinary("v")).isEqualTo(largest);
            rows.next();
            assertThat(rows.getBinary("v")).isEqualTo(smallest);
        }
    }

    /// A `BYTE_ARRAY` value carries its own length, so one short enough cannot reach the declared
    /// precision whatever its bytes are and is written without being decoded. A value long enough
    /// to reach it still is.
    @Test
    void aValueTooShortToReachThePrecisionIsWritten() throws Exception {
        FileSchema schema = single(PhysicalType.BYTE_ARRAY, LogicalType.decimal(20, 2));
        byte[] shortValue = { (byte) 0xff };                                  // -1
        byte[] eightBytes = { 0x7f, -1, -1, -1, -1, -1, -1, -1 };             // 2^63 - 1
        byte[] tooLarge = { 0x7f, -1, -1, -1, -1, -1, -1, -1, -1 };           // 2^71 - 1, 22 digits

        InMemoryOutputFile out = writeBatch(schema,
                batch -> batch.bytes(0, new byte[][] { shortValue, eightBytes }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getBinary("v")).isEqualTo(shortValue);
            rows.next();
            assertThat(rows.getBinary("v")).isEqualTo(eightBytes);
        }
        assertThatThrownBy(() -> writeBatch(schema, batch -> batch.bytes(0, new byte[][] { tooLarge })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) has a value at row 0 that is not an unscaled value the column's "
                         + "DECIMAL(20, 2) can hold");
    }

    /// A `REQUIRED` leaf under an absent ancestor has no null bit to set, so its unreachable slot
    /// carries a placeholder that the batch checks like any other value. Under a `DECIMAL` that
    /// placeholder is a decodable zero rather than the empty value, which no precision admits.
    @Test
    void thePlaceholderOfAnAbsentAncestorIsAValueTheColumnHolds() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                c -> c.logicalType(LogicalType.decimal(1, 0))))
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setNull("s"));
            rows.writeRow(row -> row.setStruct("s", s -> s.setBinary("v", new byte[] { 9 })));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getBinary("v")).containsExactly(9);
        }
    }

    /// Two's complement has no zero-byte encoding, so an empty value denotes no unscaled value at
    /// all and the reader raises `Zero length BigInteger` on it. Both APIs reject it where it is
    /// handed over.
    @Test
    void anEmptyValueIsNotAnUnscaledDecimal() {
        FileSchema schema = single(PhysicalType.BYTE_ARRAY, LogicalType.decimal(4, 0));

        assertThatThrownBy(() -> writeBatch(schema, batch -> batch.bytes(0, new byte[][] { new byte[0] })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) has a value at row 0 that is not an unscaled value the column's "
                         + "DECIMAL(4, 0) can hold");
        assertThatThrownBy(() -> writeRow(schema, row -> row.setBinary("v", new byte[0])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v: the value is not an unscaled value the column's DECIMAL(4, 0) can "
                         + "hold");
    }

    /// `UNKNOWN` is the annotation whose declared range is empty: it says the column holds only
    /// nulls, and the reader refuses to materialize a value found under it. Writing one is the
    /// same defect as writing a value outside a declared range, so both APIs reject it.
    @Test
    void anUnknownColumnHoldsOnlyNulls() {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.nullType()))
                .build();

        assertThatThrownBy(() -> writeBatch(schema, batch -> batch.ints(0, new int[] { 7 })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) is annotated UNKNOWN, which holds only nulls; set it through a "
                         + "setter taking a null mask");
        assertThatThrownBy(() -> writeBatch(schema,
                batch -> batch.ints(0, new int[] { 0, 7 }, new boolean[] { true, false })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) is annotated UNKNOWN, which holds only nulls, but row 1 is not "
                         + "null");
        assertThatThrownBy(() -> writeRow(schema, row -> row.setInt("v", 7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v is annotated UNKNOWN, which holds only nulls; setInt cannot set a "
                         + "value on it");
    }

    /// The all-null column the annotation does describe stays writable through both APIs.
    @Test
    void anUnknownColumnTakesItsNulls() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.nullType()))
                .build();

        InMemoryOutputFile out = writeBatch(schema,
                batch -> batch.ints(0, new int[2], new boolean[] { true, true }));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("v")).isTrue();
            rows.next();
            assertThat(rows.isNull("v")).isTrue();
        }
        assertThat(writeRow(schema, row -> row.setNull("v"))).isNotNull();
    }

    /// An `UNKNOWN` leaf under an absent struct is written by a record that never reaches it. The
    /// slot the row layer gives that leaf is not a value the annotation forbids — it is a slot the
    /// file never encodes — so the all-null rule must not fire on it.
    @Test
    void anUnknownLeafUnderAnAbsentStructIsWritable() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL,
                                c -> c.logicalType(LogicalType.nullType())))
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1).setNull("s"));
            rows.writeRow(row -> row.setInt("id", 2).setStruct("s", s -> s.setNull("v")));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").isNull("v")).isTrue();
        }
    }

    /// Every record omitting the struct is the same shape with no null bit set on the leaf at all,
    /// which is the other way the all-null rule can misfire on a slot that carries no value.
    @Test
    void anUnknownLeafUnderAStructAbsentFromEveryRecordIsWritable() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL,
                                c -> c.logicalType(LogicalType.nullType())))
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1).setNull("s"));
            rows.writeRow(row -> row.setInt("id", 2).setNull("s"));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
        }
    }

    /// An `UNKNOWN` element of a list is the same rule one nesting shape over: the entries a record
    /// does add must be null, and a record adding none leaves the leaf no entry to be checked.
    @Test
    void anUnknownListElementHoldsOnlyNulls() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.OPTIONAL, element -> element.primitive(
                        PhysicalType.INT32, RepetitionType.OPTIONAL, c -> c.logicalType(LogicalType.nullType())))
                .build();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setList("v", v -> v.addNull().addNull()));
            rows.writeRow(row -> row.setNull("v"));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getList("v").size()).isEqualTo(2);
            rows.next();
            assertThat(rows.isNull("v")).isTrue();
        }

        assertThatThrownBy(() -> {
            InMemoryOutputFile rejected = OutputFile.inMemory();
            try (ParquetFileWriter writer = ParquetFileWriter.create(rejected, schema)) {
                writer.rowWriter().writeRow(row -> row.setList("v", v -> v.addInt(7)));
            }
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field v.list.element is annotated UNKNOWN, which holds only nulls; setInt "
                         + "cannot set a value on it");
    }

    /// A bounded annotation on a leaf whose ancestor can be absent: the row layer writes the
    /// records that omit the struct without the leaf's bound firing on a slot the file never
    /// encodes, whether the leaf is `REQUIRED` (a placeholder in range) or `OPTIONAL` (a null).
    @Test
    void aBoundedAnnotationUnderAnAbsentStructIsWritable() throws Exception {
        FileSchema schema = nestedUnsignedByte();

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1).setNull("s"));
            rows.writeRow(row -> row.setInt("id", 2)
                    .setStruct("s", s -> s.setInt("req", 255).setInt("opt", 0)));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getInt("req")).isEqualTo(255);
            assertThat(rows.getStruct("s").getInt("opt")).isZero();
        }
    }

    /// The columnar API cannot see which rows the file drops — the levels it derives from the
    /// struct's nulls decide that later — so it checks the value at an unreachable row like any
    /// other. A caller filling those slots picks a value the column can hold; the row layer, which
    /// does know the slot is unreachable, marks it null instead.
    @Test
    void theColumnarApiChecksTheSlotsBeneathAnAbsentStruct() throws Exception {
        FileSchema schema = nestedUnsignedByte();
        Validity absentThenPresent = Validity.ofNulls(new boolean[] { true, false });

        InMemoryOutputFile out = writeBatch(schema, batch -> batch
                .ints("id", new int[] { 1, 2 })
                .struct("s", absentThenPresent)
                .ints("s.req", new int[] { 0, 255 })
                .ints("s.opt", new int[] { 0, 7 }, absentThenPresent));

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getInt("req")).isEqualTo(255);
        }

        assertThatThrownBy(() -> writeBatch(schema, batch -> batch
                .ints("id", new int[] { 1, 2 })
                .struct("s", absentThenPresent)
                .ints("s.req", new int[] { 300, 255 })
                .ints("s.opt", new int[] { 0, 7 }, absentThenPresent)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 1 (req) has value 300 at row 0, out of range for a UINT_8"
                        + " column");
    }

    private static FileSchema nestedUnsignedByte() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .addColumn("req", PhysicalType.INT32, RepetitionType.REQUIRED,
                                c -> c.logicalType(LogicalType.intType(8, false)))
                        .addColumn("opt", PhysicalType.INT32, RepetitionType.OPTIONAL,
                                c -> c.logicalType(LogicalType.intType(8, false))))
                .build();
    }

    /// A column whose physical type the setter does not match is reported as the type mismatch it
    /// is, rather than as a range failure against the annotation of the column it landed on.
    @Test
    void aPhysicalTypeMismatchIsReportedAheadOfTheRange() {
        FileSchema schema = single(PhysicalType.INT32, LogicalType.intType(8, false));

        assertThatThrownBy(() -> writeBatch(schema, batch -> batch.longs(0, new long[] { 300L })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 0 (v) is INT32, not INT64");
    }

    /// A column whose annotation the pairing grid refuses over its physical type has no range,
    /// rather than the range the annotation would declare over a type it is defined over.
    @Test
    void anAnnotationOverATypeItIsNotDefinedOverHasNoRange() {
        ColumnSchema column = column(PhysicalType.BYTE_ARRAY, null, LogicalType.time(true, LogicalType.TimeUnit.MILLIS));

        assertThatThrownBy(() -> LogicalTypeValueRange.of(column))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TIME(MILLIS, UTC) is not defined over BYTE_ARRAY");
    }

    /// A width-fixing annotation over a `FIXED_LEN_BYTE_ARRAY` of another width has no range.
    @Test
    void anAnnotationOverAWidthItIsNotDefinedOverHasNoRange() {
        ColumnSchema column = column(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.uuid());

        assertThatThrownBy(() -> LogicalTypeValueRange.of(column))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("UUID is not defined over FIXED_LEN_BYTE_ARRAY(8)");
    }

    /// An annotation of a group has no range over any primitive column.
    @Test
    void aGroupAnnotationHasNoRange() {
        ColumnSchema column = column(PhysicalType.INT32, null, LogicalType.list());

        assertThatThrownBy(() -> LogicalTypeValueRange.of(column))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("LIST is not defined over INT32");
    }

    // ==================== Helpers ====================

    private static ColumnSchema column(PhysicalType type, Integer typeLength, LogicalType logicalType) {
        return new ColumnSchema(FieldPath.of("v"), type, RepetitionType.REQUIRED, typeLength, 0, 0, 0, logicalType, null);
    }

    private static FileSchema single(PhysicalType type, LogicalType logicalType) {
        return FileSchema.builder("schema")
                .addColumn("v", type, RepetitionType.REQUIRED, c -> c.logicalType(logicalType))
                .build();
    }

    private static void fill(ColumnBatch batch, PhysicalType type, long... values) {
        if (type == PhysicalType.INT32) {
            int[] ints = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                ints[i] = (int) values[i];
            }
            batch.ints(0, ints);
        }
        else {
            batch.longs(0, values);
        }
    }

    private static void set(StructBuilder row, PhysicalType type, long value) {
        if (type == PhysicalType.INT32) {
            row.setInt("v", (int) value);
        }
        else {
            row.setLong("v", value);
        }
    }

    private static long[] readIntegers(InMemoryOutputFile out, PhysicalType type) throws Exception {
        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            long[] values = new long[Math.toIntExact(reader.getFileMetaData().numRows())];
            for (int i = 0; i < values.length; i++) {
                rows.next();
                values[i] = type == PhysicalType.INT32 ? rows.getInt("v") : rows.getLong("v");
            }
            return values;
        }
    }

    private static InMemoryOutputFile writeBatch(FileSchema schema, Consumer<ColumnBatch> filler)
            throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(filler);
        }
        return out;
    }

    private static InMemoryOutputFile writeRow(FileSchema schema, Consumer<StructBuilder> filler)
            throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(filler);
        }
        return out;
    }

    private static ParquetFileReader open(InMemoryOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(out.buffer()));
    }
}
