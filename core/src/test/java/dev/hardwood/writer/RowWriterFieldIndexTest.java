/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Addressing a field by its position rather than by its name: [StructBuilder]'s index-taking
/// setters, and the [StructBuilder#getFieldCount] / [StructBuilder#getFieldName] pair that
/// reports the positions they address.
///
/// The two addressing forms must be the same program, so the equivalence tests here write the
/// same records both ways and require byte-identical files rather than merely equal values —
/// the same assertion [RowWriterEquivalenceTest] makes of the row layer against the columnar
/// one. The rules are asserted to hold unchanged: only the way a field is named differs, so an
/// out-of-range index takes the place of an unknown name and everything else is untouched.
class RowWriterFieldIndexTest {

    private static final LocalDate DAY = LocalDate.of(2026, 8, 19);
    private static final LocalTime CLOCK = LocalTime.of(13, 45, 30, 250_000_000);
    private static final Instant MOMENT = Instant.ofEpochSecond(1_755_600_000L, 123_456_000);
    private static final LocalDateTime WALL = LocalDateTime.of(2026, 8, 19, 13, 45, 30, 250_000_000);
    private static final BigDecimal AMOUNT = new BigDecimal("1234.56");
    private static final UUID UUID_VALUE = UUID.fromString("4b3f8e2a-6c1d-4f5a-9b8e-2d7c6a5f4e31");
    private static final PqInterval SPAN = new PqInterval(14, 3, 7_200_000);

    /// Every typed setter, plus `setNull`, addressed both ways over the same schema. The
    /// indices are the field positions in declaration order.
    @Test
    void everySetterByIndexProducesTheSameFileAsByName() throws Exception {
        FileSchema schema = everyTypeSchema();

        byte[] byName = writeRows(schema, rows -> rows.writeRow(row -> row
                .setBoolean("bool", true)
                .setInt("i32", 42)
                .setLong("i64", 9_000_000_000L)
                .setFloat("f32", 1.5f)
                .setDouble("f64", 2.25)
                .setString("text", "hardwood")
                .setBinary("bin", bytes("raw"))
                .setDate("day", DAY)
                .setTime("clock", CLOCK)
                .setTimestamp("moment", MOMENT)
                .setLocalTimestamp("wall", WALL)
                .setDecimal("amount", AMOUNT)
                .setUuid("uuid", UUID_VALUE)
                .setInterval("span", SPAN)
                .setNull("absent")));

        byte[] byIndex = writeRows(schema, rows -> rows.writeRow(row -> row
                .setBoolean(0, true)
                .setInt(1, 42)
                .setLong(2, 9_000_000_000L)
                .setFloat(3, 1.5f)
                .setDouble(4, 2.25)
                .setString(5, "hardwood")
                .setBinary(6, bytes("raw"))
                .setDate(7, DAY)
                .setTime(8, CLOCK)
                .setTimestamp(9, MOMENT)
                .setLocalTimestamp(10, WALL)
                .setDecimal(11, AMOUNT)
                .setUuid(12, UUID_VALUE)
                .setInterval(13, SPAN)
                .setNull(14)));

        assertThat(byIndex).isEqualTo(byName);
    }

    /// The group verbs too, including a `MAP` entry — whose `key` and `value` are positions
    /// `0` and `1` of the struct the entry builder hands over.
    @Test
    void nestedSettersByIndexProduceTheSameFileAsByName() throws Exception {
        FileSchema schema = nestedSchema();

        byte[] byName = writeRows(schema, rows -> {
            rows.writeRow(row -> row
                    .setInt("id", 1)
                    .setStruct("address", address -> address.setString("city", "Berlin"))
                    .setList("tags", tags -> tags.addString("a").addString("b"))
                    .setMap("props", props -> props.addEntry(
                            entry -> entry.setString("key", "x").setLong("value", 7))));
            rows.writeRow(row -> row.setInt("id", 2).setList("tags", tags -> { }));
        });

        byte[] byIndex = writeRows(schema, rows -> {
            rows.writeRow(row -> row
                    .setInt(0, 1)
                    .setStruct(1, address -> address.setString(0, "Berlin"))
                    .setList(2, tags -> tags.addString("a").addString("b"))
                    .setMap(3, props -> props.addEntry(
                            entry -> entry.setString(0, "x").setLong(1, 7))));
            rows.writeRow(row -> row.setInt(0, 2).setList(2, tags -> { }));
        });

        assertThat(byIndex).isEqualTo(byName);
    }

    /// Name and index address the same field, so the two forms are interchangeable within one
    /// record — which is what lets a caller reach for an index only where it pays.
    @Test
    void nameAndIndexAddressTheSameFieldWithinOneRecord() throws Exception {
        FileSchema schema = nestedSchema();

        byte[] mixed = writeRows(schema, rows -> rows.writeRow(row -> row
                .setInt(0, 1)
                .setStruct("address", address -> address.setString(0, "Berlin"))
                .setList(2, tags -> tags.addString("a"))
                .setMap("props", props -> props.addEntry(
                        entry -> entry.setString(0, "x").setLong("value", 7)))));

        byte[] byName = writeRows(schema, rows -> rows.writeRow(row -> row
                .setInt("id", 1)
                .setStruct("address", address -> address.setString("city", "Berlin"))
                .setList("tags", tags -> tags.addString("a"))
                .setMap("props", props -> props.addEntry(
                        entry -> entry.setString("key", "x").setLong("value", 7)))));

        assertThat(mixed).isEqualTo(byName);
    }

    @Test
    void fieldCountAndNamesReportThePositionsTheSettersAddress() throws Exception {
        FileSchema schema = nestedSchema();

        AtomicReference<String[]> record = new AtomicReference<>();
        AtomicReference<String[]> address = new AtomicReference<>();
        AtomicReference<String[]> entry = new AtomicReference<>();

        writeRows(schema, rows -> rows.writeRow(row -> {
            record.set(fieldNames(row));
            row.setInt("id", 1)
                    .setStruct("address", nested -> {
                        address.set(fieldNames(nested));
                        nested.setString("city", "Berlin");
                    })
                    .setMap("props", props -> props.addEntry(keyValue -> {
                        entry.set(fieldNames(keyValue));
                        keyValue.setString("key", "x").setLong("value", 7);
                    }));
        }));

        assertThat(record.get()).containsExactly("id", "address", "tags", "props");
        assertThat(address.get()).containsExactly("city");
        assertThat(entry.get()).containsExactly("key", "value");
    }

    /// The motivating shape: a row read back by index and written forward through the same
    /// positions, with no name in the loop. The copy must reproduce the file it read.
    @Test
    void aRowCopiedByIndexReproducesTheFileItWasReadFrom() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType())
                .addColumn("score", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .build();

        int records = 300;
        byte[] original = writeRows(schema, rows -> {
            for (int i = 0; i < records; i++) {
                int value = i;
                rows.writeRow(row -> {
                    row.setInt(0, value);
                    row.setString(1, value % 3 == 0 ? null : "n" + value);
                    row.setDouble(2, value * 1.5);
                });
            }
        });

        byte[] copy = writeRows(schema, rows -> {
            try (ParquetFileReader reader = open(original); RowReader source = reader.rowReader()) {
                while (source.hasNext()) {
                    source.next();
                    rows.writeRow(row -> {
                        for (int field = 0; field < row.getFieldCount(); field++) {
                            copyField(source, row, field);
                        }
                    });
                }
            }
        });

        assertThat(copy).isEqualTo(original);
    }

    /// The reader's field index and the writer's are the same position, so the copy loop above
    /// needs no name to line the two up.
    private static void copyField(RowReader source, StructBuilder target, int field) {
        assertThat(target.getFieldName(field)).isEqualTo(source.getFieldName(field));
        if (source.isNull(field)) {
            target.setNull(field);
            return;
        }
        switch (field) {
            case 0 -> target.setInt(field, source.getInt(field));
            case 1 -> target.setString(field, source.getString(field));
            default -> target.setDouble(field, source.getDouble(field));
        }
    }

    // ==================== Rules ====================

    @Test
    void indexOutsideTheStructsFieldsIsRejected() throws Exception {
        withRowWriter(rows -> {
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(4, 1)))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Field index 4 is out of bounds for the record, which has 4 fields")
                    ;
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(-1, 1)))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Field index -1 is out of bounds for the record, which has 4 fields");
            assertThatThrownBy(() -> rows.writeRow(row -> row.getFieldName(4)))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        });
    }

    /// A nested struct's indices are its own, so a position valid in the record is not
    /// automatically valid inside it.
    @Test
    void indexOutsideANestedStructsFieldsIsRejected() throws Exception {
        withRowWriter(rows -> assertThatThrownBy(() -> rows.writeRow(row -> row
                .setInt(0, 1)
                .setStruct(3, address -> address.setString(1, "Berlin"))))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Field index 1 is out of bounds for struct address, which has 1 fields"));
    }

    @Test
    void aFieldSetByNameAndThenByIndexIsRejected() throws Exception {
        withRowWriter(rows -> {
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 1).setInt(0, 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field id is already set in this record");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(0, 1).setInt("id", 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field id is already set in this record");
        });
    }

    @Test
    void aVerbThatDoesNotFitTheFieldAtThatIndexIsRejected() throws Exception {
        withRowWriter(rows -> {
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(0, 1).setInt(3, 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field address is a struct group; setInt applies to a leaf field")
                    ;
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(0, 1).setStruct(2, tags -> { })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field tags is a LIST group; setStruct applies to a struct group")
                    ;
        });
    }

    /// The scope rule covers the by-index surface whole, introspection included: a retained
    /// builder is not a usable view of the schema either.
    @Test
    void indexedUseOfARetainedBuilderIsRejected() throws Exception {
        AtomicReference<StructBuilder> escaped = new AtomicReference<>();
        withRowWriter(rows -> {
            rows.writeRow(row -> {
                row.setInt(0, 1);
                escaped.set(row);
            });
            assertThatThrownBy(() -> escaped.get().setInt(0, 2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("This builder's scope has ended; a builder is only valid inside the lambda "
                             + "it was passed to");
            assertThatThrownBy(() -> escaped.get().getFieldCount())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("This builder's scope has ended; a builder is only valid inside the lambda "
                             + "it was passed to");
            assertThatThrownBy(() -> escaped.get().getFieldName(0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("This builder's scope has ended; a builder is only valid inside the lambda "
                             + "it was passed to");
        });
    }

    // ==================== Fixtures ====================

    private static FileSchema everyTypeSchema() {
        return FileSchema.builder("schema")
                .addColumn("bool", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .addColumn("i32", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("i64", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("f32", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("f64", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("text", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .addColumn("bin", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .addColumn("day", PhysicalType.INT32, RepetitionType.REQUIRED, new LogicalType.DateType())
                .addColumn("clock", PhysicalType.INT32, RepetitionType.REQUIRED,
                        new LogicalType.TimeType(true, TimeUnit.MILLIS))
                .addColumn("moment", PhysicalType.INT64, RepetitionType.REQUIRED,
                        new LogicalType.TimestampType(true, TimeUnit.MICROS))
                .addColumn("wall", PhysicalType.INT64, RepetitionType.REQUIRED,
                        new LogicalType.TimestampType(false, TimeUnit.MILLIS))
                .addColumn("amount", PhysicalType.INT64, RepetitionType.REQUIRED,
                        new LogicalType.DecimalType(2, 18))
                .addColumn("uuid", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                        new LogicalType.UuidType())
                .addColumn("span", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 12,
                        new LogicalType.IntervalType())
                .addColumn("absent", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();
    }

    private static FileSchema nestedSchema() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                new LogicalType.StringType()))
                .list("tags", RepetitionType.OPTIONAL, element -> element.primitive(
                        PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType()))
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, new LogicalType.StringType(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();
    }

    /// The rules fixture puts the group fields at known positions: `id` `0`, `tags` `2`,
    /// `address` `3`, so a wrong-verb or out-of-range case can name one.
    private static FileSchema rulesSchema() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType())
                .list("tags", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED))
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED))
                .build();
    }

    private static String[] fieldNames(StructBuilder builder) {
        String[] names = new String[builder.getFieldCount()];
        for (int i = 0; i < names.length; i++) {
            names[i] = builder.getFieldName(i);
        }
        return names;
    }

    /// Writes records through the row-oriented API.
    private interface RowWrite {
        void accept(RowWriter rows) throws Exception;
    }

    private static byte[] writeRows(FileSchema schema, RowWrite filler) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            filler.accept(writer.rowWriter());
        }
        return out.toByteArray();
    }

    private static void withRowWriter(RowWrite body) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, rulesSchema())) {
            RowWriter rows = writer.rowWriter();
            body.accept(rows);
            // Leave one valid record behind so the file closes on a complete batch.
            rows.writeRow(row -> row.setInt(0, 0));
        }
    }

    private static ParquetFileReader open(byte[] file) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
