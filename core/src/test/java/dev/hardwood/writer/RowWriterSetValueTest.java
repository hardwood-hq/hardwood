/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqFile;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [StructBuilder#setValue]: a value in the form the reader's `getValue` returns is written as
/// the typed setter for the field's type writes it, and a value in any other form is rejected.
class RowWriterSetValueTest {

    /// One field of every value form `setValue` takes, nested groups included.
    private static final FileSchema EVERY_FORM = FileSchema.builder("schema")
            .addColumn("bool", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
            .addColumn("i32", PhysicalType.INT32, RepetitionType.REQUIRED)
            .addColumn("i64", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("f32", PhysicalType.FLOAT, RepetitionType.REQUIRED)
            .addColumn("f64", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
            .addColumn("i8", PhysicalType.INT32, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.intType(8, true)))
            .addColumn("i16", PhysicalType.INT32, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.intType(16, true)))
            .addColumn("u8", PhysicalType.INT32, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.intType(8, false)))
            .addColumn("u64", PhysicalType.INT64, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.intType(64, false)))
            .addColumn("text", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.string()))
            .addColumn("kind", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.enumType()))
            .addColumn("doc", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.json()))
            .addColumn("bin", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
            .addColumn("fixed", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, col -> col.typeLength(3))
            .addColumn("day", PhysicalType.INT32, RepetitionType.REQUIRED, col -> col.logicalType(LogicalType.date()))
            .addColumn("clock", PhysicalType.INT32, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.time(true, TimeUnit.MILLIS)))
            .addColumn("moment", PhysicalType.INT64, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.timestamp(true, TimeUnit.MICROS)))
            .addColumn("wall", PhysicalType.INT64, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.timestamp(false, TimeUnit.NANOS)))
            .addColumn("amount", PhysicalType.INT64, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.decimal(18, 2)))
            .addColumn("big", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.logicalType(LogicalType.decimal(30, 4)))
            .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.typeLength(16).logicalType(LogicalType.uuid()))
            .addColumn("span", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.typeLength(12).logicalType(LogicalType.interval()))
            .addColumn("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.typeLength(2).logicalType(LogicalType.float16()))
            .addColumn("absent", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                    col -> col.logicalType(LogicalType.string()))
            .struct("address", RepetitionType.OPTIONAL, address -> address
                    .addColumn("street", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                            col -> col.logicalType(LogicalType.string()))
                    .addColumn("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
            .list("tags", RepetitionType.OPTIONAL,
                    el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                            col -> col.logicalType(LogicalType.string())))
            .list("matrix", RepetitionType.OPTIONAL, el -> el.list(RepetitionType.OPTIONAL,
                    inner -> inner.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
            .list("points", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL, point -> point
                    .addColumn("x", PhysicalType.DOUBLE, RepetitionType.REQUIRED)))
            .map("counts", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                    key -> key.logicalType(LogicalType.string()),
                    value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
            .file("attachment", RepetitionType.OPTIONAL)
            .build();

    // ==================== Every form ====================

    @Test
    void copyingEveryFieldThroughSetValueWritesTheSameFile() throws Exception {
        InMemoryOutputFile original = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(original, EVERY_FORM)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(RowWriterSetValueTest::fullRecord);
            rows.writeRow(RowWriterSetValueTest::sparseRecord);
        }

        // A generic copier: every field by position, its value as the reader returns it.
        InMemoryOutputFile copy = OutputFile.inMemory();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(original.buffer()));
                RowReader in = reader.rowReader();
                ParquetFileWriter writer = ParquetFileWriter.create(copy, EVERY_FORM)) {
            RowWriter rows = writer.rowWriter();
            while (in.hasNext()) {
                in.next();
                rows.writeRow(out -> {
                    for (int i = 0; i < in.getFieldCount(); i++) {
                        out.setValue(i, in.getValue(i));
                    }
                });
            }
        }

        assertThat(copy.buffer()).isEqualTo(original.buffer());
    }

    @Test
    void writesEachFormByName() throws Exception {
        InMemoryOutputFile out = write(EVERY_FORM, row -> row
                .setValue("bool", true)
                .setValue("i32", 7)
                .setValue("i64", 8L)
                .setValue("f32", 1.5f)
                .setValue("f64", 2.5)
                .setValue("i8", (byte) -3)
                .setValue("i16", (short) -300)
                .setValue("u8", 255)
                .setValue("u64", -1L)
                .setValue("text", "hardwood")
                .setValue("kind", "A")
                .setValue("doc", "{}")
                .setValue("bin", new byte[] { 1 })
                .setValue("fixed", new byte[] { 1, 2, 3 })
                .setValue("day", LocalDate.of(2026, 11, 15))
                .setValue("clock", LocalTime.of(12, 30))
                .setValue("moment", Instant.parse("2026-11-15T12:00:00Z"))
                .setValue("wall", LocalDateTime.of(2026, 11, 15, 12, 0, 0, 1))
                .setValue("amount", new BigDecimal("12.34"))
                .setValue("big", new BigDecimal("-12345678901234567890.1234"))
                .setValue("id", new UUID(1, 2))
                .setValue("span", new PqInterval(1, 2, 3))
                .setValue("half", 0.5f));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getValue("i8")).isEqualTo((byte) -3);
            assertThat(rows.getValue("u64")).isEqualTo(-1L);
            assertThat(rows.getValue("wall")).isEqualTo(LocalDateTime.of(2026, 11, 15, 12, 0, 0, 1));
            assertThat(rows.getValue("big")).isEqualTo(new BigDecimal("-12345678901234567890.1234"));
            assertThat(rows.getValue("half")).isEqualTo(0.5f);
            assertThat(rows.isNull("absent")).isTrue();
            assertThat(rows.isNull("address")).isTrue();
        }
    }

    // ==================== Struct copies ====================

    @Test
    void copiesAStructByNameIgnoringFieldsTheTargetDoesNotDeclare() throws Exception {
        FileSchema source = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, s -> s
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("extra", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                col -> col.logicalType(LogicalType.string())))
                .build();
        FileSchema target = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, s -> s
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                col -> col.logicalType(LogicalType.string()))
                        .addColumn("country", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                col -> col.logicalType(LogicalType.string()))
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        InMemoryOutputFile out = copyStruct(write(source, row -> row.setStruct("s", s -> s
                .setInt("zip", 10115).setInt("extra", 1).setString("city", "Berlin"))), target);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                RowReader rows = reader.rowReader()) {
            rows.next();
            PqStruct copied = rows.getStruct("s");
            assertThat(copied.getString("city")).isEqualTo("Berlin");
            assertThat(copied.getInt("zip")).isEqualTo(10115);
            assertThat(copied.isNull("country")).isTrue();
        }
    }

    @Test
    void aRequiredFieldTheSourceLacksFailsTheRecord() throws Exception {
        FileSchema source = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, s -> s
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                col -> col.logicalType(LogicalType.string())))
                .build();
        FileSchema target = FileSchema.builder("schema")
                .struct("s", RepetitionType.REQUIRED, s -> s
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                col -> col.logicalType(LogicalType.string()))
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        InMemoryOutputFile file = write(source, row -> row.setStruct("s", s -> s.setString("city", "Berlin")));

        assertThatThrownBy(() -> copyStruct(file, target))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field s.zip is REQUIRED; it must be set to a non-null value in every record");
    }

    // ==================== Rejections ====================

    static Stream<Arguments> mismatches() {
        return Stream.of(
                Arguments.of("f64", "12.5", "Field f64 is DOUBLE and takes a Double; it was given a String"),
                Arguments.of("i64", 1, "Field i64 is INT64 and takes a Long; it was given a Integer"),
                Arguments.of("i8", 1, "Field i8 is INT32 annotated " + LogicalType.intType(8, true)
                        + " and takes a Byte; it was given a Integer"),
                Arguments.of("text", "x".getBytes(StandardCharsets.UTF_8), "Field text is BYTE_ARRAY annotated "
                        + LogicalType.string() + " and takes a String; it was given a byte[]"),
                Arguments.of("day", 20_000, "Field day is INT32 annotated " + LogicalType.date()
                        + " and takes a LocalDate; it was given a Integer"),
                Arguments.of("moment", LocalDateTime.of(2026, 1, 1, 0, 0), "Field moment is INT64 annotated "
                        + LogicalType.timestamp(true, TimeUnit.MICROS)
                        + " and takes a Instant; it was given a LocalDateTime"),
                Arguments.of("half", 0.5, "Field half is FIXED_LEN_BYTE_ARRAY annotated " + LogicalType.float16()
                        + " and takes a Float; it was given a Double"),
                Arguments.of("address", "Berlin",
                        "Field address is a struct group and takes a PqStruct; it was given a String"),
                Arguments.of("tags", new String[] { "a" },
                        "Field tags is a LIST group and takes a PqList; it was given a String[]"),
                Arguments.of("counts", 1, "Field counts is a MAP group and takes a PqMap; it was given a Integer"),
                Arguments.of("attachment", "s3://b/c",
                        "Field attachment is a FILE group and takes a PqFile; it was given a String"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mismatches")
    void rejectsAValueNotInTheFieldsForm(String field, Object value, String message) {
        assertThatThrownBy(() -> writeWith(field, value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    @Test
    void rejectsAVariant() {
        // A variant holding null: one header byte, primitive type 0.
        PqVariant variant = new PqVariantImpl(new byte[] { 0x01, 0x00, 0x00 }, new byte[] { 0x00 });

        assertThatThrownBy(() -> writeWith("bin", variant))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field bin was given a PqVariant; VARIANT values cannot be written");
    }

    @Test
    void rejectsAFloat16ValueAHalfCannotHold() {
        assertThatThrownBy(() -> writeWith("half", 0.1f))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field half: 0.1 has no exact FLOAT16 representation");
    }

    @Test
    void rejectsAValueOutOfItsAnnotationsRangeAsTheTypedSetterDoes() {
        assertThatThrownBy(() -> writeWith("u8", 256))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field u8: 256 is out of range for a " + LogicalType.intType(8, false) + " column");
    }

    @Test
    void aNullValueSetsTheFieldNull() throws Exception {
        InMemoryOutputFile out = writeWith("absent", null);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("absent")).isTrue();
        }
    }

    // ==================== Helpers ====================

    private static void fullRecord(StructBuilder row) {
        row.setBoolean("bool", true)
                .setInt("i32", 7)
                .setLong("i64", 8L)
                .setFloat("f32", 1.5f)
                .setDouble("f64", 2.5)
                .setInt("i8", -3)
                .setInt("i16", -300)
                .setInt("u8", 255)
                .setLong("u64", -1L)
                .setString("text", "hardwood")
                .setString("kind", "A")
                .setString("doc", "{\"a\":1}")
                .setBinary("bin", new byte[] { 1, 2 })
                .setBinary("fixed", new byte[] { 1, 2, 3 })
                .setDate("day", LocalDate.of(2026, 11, 15))
                .setTime("clock", LocalTime.of(12, 30, 1, 2_000_000))
                .setTimestamp("moment", Instant.parse("2026-11-15T12:00:00.000001Z"))
                .setLocalTimestamp("wall", LocalDateTime.of(2026, 11, 15, 12, 0, 0, 1))
                .setDecimal("amount", new BigDecimal("12.34"))
                .setDecimal("big", new BigDecimal("-12345678901234567890.1234"))
                .setUuid("id", new UUID(1, 2))
                .setInterval("span", new PqInterval(1, 2, 3))
                .setBinary("half", new byte[] { 0x00, 0x38 })
                .setNull("absent")
                .setStruct("address", address -> address.setString("street", "Main").setInt("zip", 10115))
                .setList("tags", tags -> tags.addString("a").addNull().addString("b"))
                .setList("matrix", matrix -> matrix
                        .addList(inner -> inner.addInt(1).addInt(2))
                        .addNull()
                        .addList(inner -> { }))
                .setList("points", points -> points
                        .addStruct(point -> point.setDouble("x", 1.0))
                        .addNull())
                .setMap("counts", counts -> counts
                        .addEntry(entry -> entry.setString("key", "a").setLong("value", 1L))
                        .addEntry(entry -> entry.setString("key", "b").setNull("value")))
                .setFile("attachment", PqFile.external("s3://bucket/doc.pdf", 9));
    }

    /// A value for every REQUIRED field of [#EVERY_FORM], by name.
    private static final Map<String, Consumer<StructBuilder>> REQUIRED_VALUES = requiredValues();

    private static Map<String, Consumer<StructBuilder>> requiredValues() {
        Map<String, Consumer<StructBuilder>> values = new LinkedHashMap<>();
        values.put("bool", row -> row.setBoolean("bool", false));
        values.put("i32", row -> row.setInt("i32", 0));
        values.put("i64", row -> row.setLong("i64", 0L));
        values.put("f32", row -> row.setFloat("f32", Float.NaN));
        values.put("f64", row -> row.setDouble("f64", -0.0));
        values.put("i8", row -> row.setInt("i8", 0));
        values.put("i16", row -> row.setInt("i16", 0));
        values.put("u8", row -> row.setInt("u8", 0));
        values.put("u64", row -> row.setLong("u64", 0L));
        values.put("text", row -> row.setString("text", ""));
        values.put("kind", row -> row.setString("kind", "B"));
        values.put("doc", row -> row.setString("doc", "null"));
        values.put("bin", row -> row.setBinary("bin", new byte[0]));
        values.put("fixed", row -> row.setBinary("fixed", new byte[3]));
        values.put("day", row -> row.setDate("day", LocalDate.EPOCH));
        values.put("clock", row -> row.setTime("clock", LocalTime.MIDNIGHT));
        values.put("moment", row -> row.setTimestamp("moment", Instant.EPOCH));
        values.put("wall", row -> row.setLocalTimestamp("wall", LocalDateTime.of(1970, 1, 1, 0, 0)));
        values.put("amount", row -> row.setDecimal("amount", new BigDecimal("0.00")));
        values.put("big", row -> row.setDecimal("big", new BigDecimal("0.0000")));
        values.put("id", row -> row.setUuid("id", new UUID(0, 0)));
        values.put("span", row -> row.setInterval("span", new PqInterval(0, 0, 0)));
        values.put("half", row -> row.setBinary("half", new byte[2]));
        return values;
    }

    /// Every REQUIRED field set, every optional one left null but an empty list.
    private static void sparseRecord(StructBuilder row) {
        REQUIRED_VALUES.values().forEach(setter -> setter.accept(row));
        row.setList("tags", tags -> { });
    }

    /// A record of [#EVERY_FORM] whose fields are those of [#sparseRecord], except `field`,
    /// which is set through `setValue` to `value`.
    private static InMemoryOutputFile writeWith(String field, Object value) {
        return write(EVERY_FORM, row -> {
            REQUIRED_VALUES.forEach((name, setter) -> {
                if (!name.equals(field)) {
                    setter.accept(row);
                }
            });
            row.setValue(field, value);
        });
    }

    private static InMemoryOutputFile write(FileSchema schema, Consumer<StructBuilder> filler) {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(filler);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    /// Writes into `target` one record whose field `s` is `setValue` of field `s` of the
    /// first record of `source`.
    private static InMemoryOutputFile copyStruct(InMemoryOutputFile source, FileSchema target) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(source.buffer()));
                RowReader rows = reader.rowReader()) {
            rows.next();
            PqStruct struct = rows.getStruct("s");
            return write(target, row -> row.setValue("s", struct));
        }
    }
}
