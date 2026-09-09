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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

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
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Round-trip tests for the row-oriented layer: write records through [RowWriter], then read
/// them back through [RowReader] — the API it mirrors — and assert every field survives.
class RowWriterRoundTripTest {

    @Test
    void writesEveryPhysicalTypeFlat() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("bool", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .addColumn("i32", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("i64", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("f32", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("f64", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("bin", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .addColumn("fixed", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 3)
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            for (int i = 0; i < 3; i++) {
                int value = i;
                rows.writeRow(row -> row
                        .setBoolean("bool", value % 2 == 0)
                        .setInt("i32", value)
                        .setLong("i64", value * 1_000_000_000L)
                        .setFloat("f32", value + 0.5f)
                        .setDouble("f64", value + 0.25)
                        .setBinary("bin", ("v" + value).getBytes(StandardCharsets.UTF_8))
                        .setBinary("fixed", new byte[] { (byte) value, 1, 2 }));
            }
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            for (int i = 0; i < 3; i++) {
                rows.next();
                assertThat(rows.getBoolean("bool")).isEqualTo(i % 2 == 0);
                assertThat(rows.getInt("i32")).isEqualTo(i);
                assertThat(rows.getLong("i64")).isEqualTo(i * 1_000_000_000L);
                assertThat(rows.getFloat("f32")).isEqualTo(i + 0.5f);
                assertThat(rows.getDouble("f64")).isEqualTo(i + 0.25);
                assertThat(rows.getBinary("bin")).isEqualTo(("v" + i).getBytes(StandardCharsets.UTF_8));
                assertThat(rows.getBinary("fixed")).isEqualTo(new byte[] { (byte) i, 1, 2 });
            }
            assertThat(rows.hasNext()).isFalse();
        }
    }

    /// An `OPTIONAL` field is null when it is set to null, when it is set through [
    /// StructBuilder#setNull], and when it is never set at all — the three spellings of the
    /// same record.
    @Test
    void nullsArriveFromEverySpelling() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("a", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .addColumn("b", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .addColumn("c", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1)
                    .setString("a", null)
                    .setNull("b"));
            rows.writeRow(row -> row.setInt("id", 2)
                    .setString("a", "present")
                    .setString("b", "present")
                    .setString("c", "present"));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            assertThat(rows.isNull("a")).isTrue();
            assertThat(rows.isNull("b")).isTrue();
            assertThat(rows.isNull("c")).isTrue();
            rows.next();
            assertThat(rows.getString("a")).isEqualTo("present");
            assertThat(rows.getString("b")).isEqualTo("present");
            assertThat(rows.getString("c")).isEqualTo("present");
        }
    }

    @Test
    void writesLogicalTypeValuesAsTheReaderReturnsThem() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())
                .addColumn("day", PhysicalType.INT32, RepetitionType.REQUIRED, LogicalType.date())
                .addColumn("clock", PhysicalType.INT32, RepetitionType.REQUIRED,
                        LogicalType.time(true, TimeUnit.MILLIS))
                .addColumn("moment", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.timestamp(true, TimeUnit.MICROS))
                .addColumn("wall", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.timestamp(false, TimeUnit.MILLIS))
                .addColumn("amount", PhysicalType.INT64, RepetitionType.REQUIRED,
                        LogicalType.decimal(18, 2))
                .addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 16,
                        LogicalType.uuid())
                .addColumn("span", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 12,
                        LogicalType.interval())
                .build();

        LocalDate day = LocalDate.of(2026, 8, 19);
        LocalTime clock = LocalTime.of(13, 45, 30, 250_000_000);
        Instant moment = Instant.ofEpochSecond(1_755_600_000L, 123_456_000);
        LocalDateTime wall = LocalDateTime.of(2026, 8, 19, 13, 45, 30, 250_000_000);
        BigDecimal amount = new BigDecimal("1234.56");
        UUID id = UUID.fromString("4b3f8e2a-6c1d-4f5a-9b8e-2d7c6a5f4e31");
        PqInterval span = new PqInterval(14, 3, 7_200_000);

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.rowWriter().writeRow(row -> row
                    .setString("name", "hardwood")
                    .setDate("day", day)
                    .setTime("clock", clock)
                    .setTimestamp("moment", moment)
                    .setLocalTimestamp("wall", wall)
                    .setDecimal("amount", amount)
                    .setUuid("id", id)
                    .setInterval("span", span));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getString("name")).isEqualTo("hardwood");
            assertThat(rows.getDate("day")).isEqualTo(day);
            assertThat(rows.getTime("clock")).isEqualTo(clock);
            assertThat(rows.getTimestamp("moment")).isEqualTo(moment);
            assertThat(rows.getLocalTimestamp("wall")).isEqualTo(wall);
            assertThat(rows.getDecimal("amount")).isEqualTo(amount);
            assertThat(rows.getUuid("id")).isEqualTo(id);
            assertThat(rows.getInterval("span")).isEqualTo(span);
        }
    }

    @Test
    void writesNestedStructsIncludingAbsentOnes() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                LogicalType.string())
                        .addColumn("zip", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                                LogicalType.string()))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1)
                    .setStruct("address", address -> address.setString("city", "Berlin").setString("zip", "10115")));
            // No address at all: the struct is absent, not a struct of nulls.
            rows.writeRow(row -> row.setInt("id", 2));
            rows.writeRow(row -> row.setInt("id", 3)
                    .setStruct("address", address -> address.setString("city", "Aarhus")));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            PqStruct address = rows.getStruct("address");
            assertThat(address.getString("city")).isEqualTo("Berlin");
            assertThat(address.getString("zip")).isEqualTo("10115");
            rows.next();
            assertThat(rows.isNull("address")).isTrue();
            rows.next();
            address = rows.getStruct("address");
            assertThat(address.getString("city")).isEqualTo("Aarhus");
            assertThat(address.isNull("zip")).isTrue();
        }
    }

    /// A `REQUIRED FIXED_LEN_BYTE_ARRAY` leaf under an absent struct has no value to stage, and
    /// the slot it is nevertheless given has to be a value of the column's declared width: the
    /// batch it is handed to accepts nothing else, however unreachable the entry is.
    @Test
    void writesAFixedWidthLeafUnderAnAbsentStruct() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("key", RepetitionType.OPTIONAL, key -> key
                        .addColumn("bytes", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            rows.writeRow(row -> row.setInt("id", 2)
                    .setStruct("key", key -> key.setBinary("bytes", new byte[] { 1, 2, 3, 4 })));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("key")).isTrue();
            rows.next();
            assertThat(rows.getStruct("key").getBinary("bytes")).containsExactly(1, 2, 3, 4);
        }
    }

    /// An absent list, an empty list and a list holding a null entry are three distinct
    /// records, and the row API spells each of them.
    @Test
    void writesListsIncludingEmptyAbsentAndNullEntries() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .list("scores", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1).setList("scores", scores -> scores.addInt(7).addInt(9)));
            rows.writeRow(row -> row.setInt("id", 2).setList("scores", scores -> { }));
            rows.writeRow(row -> row.setInt("id", 3));
            rows.writeRow(row -> row.setInt("id", 4).setList("scores", scores -> scores.addInt(1).addNull()));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getList("scores").ints().toArray()).containsExactly(7, 9);
            rows.next();
            assertThat(rows.getList("scores").size()).isZero();
            rows.next();
            assertThat(rows.isNull("scores")).isTrue();
            rows.next();
            PqList scores = rows.getList("scores");
            assertThat(scores.size()).isEqualTo(2);
            assertThat(scores.isNull(1)).isTrue();
        }
    }

    /// A nullable struct directly enclosing a list is a fifth state beyond a flat list's four:
    /// the struct itself can be absent, on top of the list being null, empty, or holding a
    /// null entry.
    @Test
    void writesAnOptionalStructEnclosingAnOptionalList() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .list("phones", RepetitionType.OPTIONAL,
                                element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            rows.writeRow(row -> row.setInt("id", 2).setStruct("s", s -> { }));
            rows.writeRow(row -> row.setInt("id", 3).setStruct("s", s -> s.setList("phones", phones -> { })));
            rows.writeRow(row -> row.setInt("id", 4)
                    .setStruct("s", s -> s.setList("phones", phones -> phones.addNull())));
            rows.writeRow(row -> row.setInt("id", 5)
                    .setStruct("s", s -> s.setList("phones", phones -> phones.addInt(42))));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").isNull("phones")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getList("phones").size()).isZero();
            rows.next();
            PqList phones = rows.getStruct("s").getList("phones");
            assertThat(phones.size()).isEqualTo(1);
            assertThat(phones.isNull(0)).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getList("phones").ints().toArray()).containsExactly(42);
        }
    }

    /// The same fifth state a struct enclosing a list adds applies to a struct enclosing a map.
    @Test
    void writesAnOptionalStructEnclosingAnOptionalMap() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                                value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            rows.writeRow(row -> row.setInt("id", 2).setStruct("s", s -> { }));
            rows.writeRow(row -> row.setInt("id", 3).setStruct("s", s -> s.setMap("props", props -> { })));
            rows.writeRow(row -> row.setInt("id", 4).setStruct("s", s -> s.setMap("props", props -> props
                    .addEntry(entry -> entry.setInt("key", 1).setNull("value")))));
            rows.writeRow(row -> row.setInt("id", 5).setStruct("s", s -> s.setMap("props", props -> props
                    .addEntry(entry -> entry.setInt("key", 2).setInt("value", 99)))));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.isNull("s")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").isNull("props")).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getMap("props").size()).isZero();
            rows.next();
            PqMap props = rows.getStruct("s").getMap("props");
            assertThat(props.size()).isEqualTo(1);
            assertThat(props.getEntries().get(0).getIntKey()).isEqualTo(1);
            assertThat(props.getEntries().get(0).isValueNull()).isTrue();
            rows.next();
            props = rows.getStruct("s").getMap("props");
            assertThat(props.getEntries().get(0).getIntKey()).isEqualTo(2);
            assertThat(props.getEntries().get(0).getIntValue()).isEqualTo(99);
        }
    }

    /// The same struct-enclosing-a-list shape as an element of an outer list, rather than the
    /// record root — the `chapters.list.element.sections` shape of `nested_list_struct_test.parquet`.
    @Test
    void writesAnOptionalStructEnclosingAnOptionalListNestedInsideAList() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("chapters", RepetitionType.OPTIONAL, chapter -> chapter.struct(RepetitionType.OPTIONAL,
                        element -> element.list("sections", RepetitionType.OPTIONAL,
                                section -> section.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            // Book 0: chapters = [{sections: [10, 20]}, null (chapter itself absent)].
            rows.writeRow(row -> row.setList("chapters", chapters -> chapters
                    .addStruct(chapter -> chapter.setList("sections", sections -> sections.addInt(10).addInt(20)))
                    .addNull()));
            rows.writeRow(row -> row.setList("chapters", chapters -> { })); // book 1: chapters = []
            rows.writeRow(row -> { }); // book 2: chapters absent
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            PqList chapters = rows.getList("chapters");
            assertThat(chapters.size()).isEqualTo(2);
            assertThat(chapters.structs().get(0).getList("sections").ints().toArray()).containsExactly(10, 20);
            assertThat(chapters.isNull(1)).isTrue();
            rows.next();
            assertThat(rows.getList("chapters").size()).isZero();
            rows.next();
            assertThat(rows.isNull("chapters")).isTrue();
        }
    }

    @Test
    void writesListsOfStructsAndListsOfLists() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("people", RepetitionType.REQUIRED, element -> element.struct(RepetitionType.REQUIRED,
                        person -> person.addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                LogicalType.string())))
                .list("grid", RepetitionType.REQUIRED, element -> element.list(RepetitionType.REQUIRED,
                        inner -> inner.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row
                    .setList("people", people -> people
                            .addStruct(person -> person.setString("name", "ada"))
                            .addStruct(person -> person.setString("name", "alan")))
                    .setList("grid", grid -> grid
                            .addList(inner -> inner.addInt(1).addInt(2))
                            .addList(inner -> inner.addInt(3))));
            rows.writeRow(row -> row
                    .setList("people", people -> people.addStruct(person -> person.setString("name", "grace")))
                    .setList("grid", grid -> { }));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            List<PqStruct> people = rows.getList("people").structs();
            assertThat(people).hasSize(2);
            assertThat(people.get(0).getString("name")).isEqualTo("ada");
            assertThat(people.get(1).getString("name")).isEqualTo("alan");
            List<PqList> grid = rows.getList("grid").lists();
            assertThat(grid.get(0).ints().toArray()).containsExactly(1, 2);
            assertThat(grid.get(1).ints().toArray()).containsExactly(3);
            rows.next();
            assertThat(rows.getList("people").structs().get(0).getString("name")).isEqualTo("grace");
            assertThat(rows.getList("grid").size()).isZero();
        }
    }

    @Test
    void writesMapsThroughTheirEntryStruct() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1).setMap("props", props -> props
                    .addEntry(entry -> entry.setString("key", "reads").setLong("value", 12))
                    .addEntry(entry -> entry.setString("key", "writes").setNull("value"))));
            rows.writeRow(row -> row.setInt("id", 2));
        }

        try (ParquetFileReader reader = open(out); RowReader rows = reader.rowReader()) {
            rows.next();
            PqMap props = rows.getMap("props");
            assertThat(props.size()).isEqualTo(2);
            assertThat(props.getEntries().get(0).getStringKey()).isEqualTo("reads");
            assertThat(props.getEntries().get(0).getLongValue()).isEqualTo(12L);
            assertThat(props.getEntries().get(1).getStringKey()).isEqualTo("writes");
            assertThat(props.getEntries().get(1).isValueNull()).isTrue();
            rows.next();
            assertThat(rows.isNull("props")).isTrue();
        }
    }

    /// The layer stages records into batches of a fixed size and submits them as they fill,
    /// so a record count that straddles the boundary must still read back as one file.
    @Test
    void recordCountStraddlingTheBatchBoundarySurvives() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .build();

        int records = 2_500;
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            for (int i = 0; i < records; i++) {
                int value = i;
                rows.writeRow(row -> {
                    row.setInt("id", value);
                    if (value % 3 != 0) {
                        row.setString("name", "n" + value);
                    }
                });
            }
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(records);
            try (RowReader rows = reader.rowReader()) {
                for (int i = 0; i < records; i++) {
                    rows.next();
                    assertThat(rows.getInt("id")).isEqualTo(i);
                    if (i % 3 == 0) {
                        assertThat(rows.isNull("name")).isTrue();
                    }
                    else {
                        assertThat(rows.getString("name")).isEqualTo("n" + i);
                    }
                }
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    /// A record of large values submits its batch on the payload trigger rather than on the
    /// record count, so staging stays bounded by one row group's worth.
    @Test
    void largeValuesSubmitBatchesBeforeTheRecordCountTrigger() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("blob", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();

        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(64 * 1024).build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            RowWriter rows = writer.rowWriter();
            for (int i = 0; i < 32; i++) {
                int value = i;
                // A blob per row rather than one blob 32 times: repeats of a single value are
                // interned once and retain a dictionary entry between them, so the writer would
                // be holding 16 KiB however many rows arrived, and the target it is being held
                // against would never be reached.
                rows.writeRow(row -> row.setInt("id", value).setBinary("blob", largeBlob(value)));
            }
        }

        try (ParquetFileReader reader = open(out)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(32);
            assertThat(reader.getFileMetaData().rowGroups().size()).isGreaterThan(1);
            try (RowReader rows = reader.rowReader()) {
                for (int i = 0; i < 32; i++) {
                    rows.next();
                    assertThat(rows.getInt("id")).isEqualTo(i);
                    assertThat(rows.getBinary("blob")).isEqualTo(largeBlob(i));
                }
            }
        }
    }

    /// A 16 KiB value distinct from every other, so that what the writer retains for it grows
    /// with the rows written rather than being folded into one dictionary entry.
    private static byte[] largeBlob(int row) {
        byte[] blob = new byte[16 * 1024];
        Arrays.fill(blob, (byte) ('a' + row % 26));
        blob[0] = (byte) row;
        return blob;
    }

    private static ParquetFileReader open(ByteBufferOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
    }
}
