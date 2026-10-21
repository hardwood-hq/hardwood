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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.StructBuilder;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// Values the row reader returns, held after it has moved on. A flyweight refers to the batch it
/// was read from, whose arrays no later batch refills, so each one held across many batches and row
/// groups keeps reading its own row: on every kind of flyweight, over plain and dictionary-encoded
/// pages, binary and fixed-width leaves, nulls at each level, and fixed-size lists.
class HeldRowValuesTest {

    private static final int ROWS = 300_000;
    private static final int ROW_GROUP_ROWS = 25_000;

    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .struct("person", RepetitionType.OPTIONAL, person -> person
                    .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                    .addColumn("age", PhysicalType.INT64, RepetitionType.REQUIRED))
            .list("ints", RepetitionType.OPTIONAL, element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
            .list("longs", RepetitionType.REQUIRED, element -> element.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
            .list("vector", RepetitionType.REQUIRED, element -> element.primitive(PhysicalType.DOUBLE, RepetitionType.REQUIRED))
            .list("names", RepetitionType.OPTIONAL, element -> element.primitive(PhysicalType.BYTE_ARRAY,
                    RepetitionType.OPTIONAL, LogicalType.string()))
            .list("items", RepetitionType.REQUIRED, element -> element.struct(RepetitionType.REQUIRED, item -> item
                    .addColumn("sku", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string())))
            .map("counts", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                    value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
            .build();

    /// With the fixed-size-list fast path on, `vector`, a list of three required doubles in every
    /// row, is read as a fixed-width batch.
    @ParameterizedTest
    @CsvSource({ "true, false", "false, false", "true, true", "false, true" })
    void flyweightsHeldAcrossBatchesReadTheirOwnRow(boolean dictionary, boolean fixedListFastPath) throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA,
                WriterConfig.builder().rowGroupTargetRows(ROW_GROUP_ROWS).build())) {
            for (int i = 0; i < ROWS; i++) {
                int row = i;
                writer.rowWriter().writeRow(r -> writeRow(r, row, dictionary));
            }
        }

        List<Held> held = new ArrayList<>(ROWS);
        ReaderConfig config = ReaderConfig.builder()
                .option("hardwood.fixed-list-fast-path", Boolean.toString(fixedListFastPath)).build();
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()), context, config);
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(ROWS / ROW_GROUP_ROWS);
            for (ColumnChunk column : reader.getFileMetaData().rowGroups().get(0).columns()) {
                if (column.metaData().pathInSchema().leafName().equals("name")) {
                    assertThat(column.metaData().encodings().contains(Encoding.RLE_DICTIONARY)).isEqualTo(dictionary);
                }
            }
            while (rows.hasNext()) {
                rows.next();
                PqList ints = rows.getList("ints");
                PqList longs = rows.getList("longs");
                PqList vector = rows.getList("vector");
                held.add(new Held(rows.getStruct("person"), ints, ints == null ? null : ints.ints(),
                        longs.longs(), vector, vector.doubles(), rows.getList("names"),
                        rows.getList("items").structs(), rows.getMap("counts")));
            }
        }

        assertThat(held).hasSize(ROWS);
        for (int row = 0; row < ROWS; row++) {
            assertRow(held.get(row), row, dictionary);
        }
    }

    /// The values of row `row`, so that a value read from another row differs: distinct per row on a
    /// plain page, one of five per field on a dictionary-encoded one, with each field shifted so that
    /// neighbouring rows differ either way.
    private static long value(int row, int field, boolean dictionary) {
        return dictionary ? (row + field) % 5 : (long) row * 16 + field;
    }

    private static void writeRow(StructBuilder r, int row, boolean dictionary) {
        if (row % 7 == 3) {
            r.setNull("person");
        }
        else {
            r.setStruct("person", p -> {
                if (row % 5 == 1) {
                    p.setNull("name");
                }
                else {
                    p.setString("name", "name-" + value(row, 0, dictionary));
                }
                p.setLong("age", value(row, 1, dictionary));
            });
        }
        if (row % 11 == 4) {
            r.setNull("ints");
        }
        else {
            r.setList("ints", l -> {
                for (int i = 0; i < row % 4; i++) {
                    if (i == 1) {
                        l.addNull();
                    }
                    else {
                        l.addInt((int) value(row, 2 + i, dictionary));
                    }
                }
            });
        }
        r.setList("longs", l -> l.addLong(value(row, 6, dictionary)).addLong(value(row, 7, dictionary)));
        r.setList("vector", l -> {
            for (int i = 0; i < 3; i++) {
                l.addDouble(value(row, 8 + i, dictionary));
            }
        });
        r.setList("names", l -> l.addString("n-" + value(row, 11, dictionary)).addNull());
        r.setList("items", l -> {
            for (int i = 0; i < 1 + row % 3; i++) {
                int item = i;
                l.addStruct(s -> s.setString("sku", "sku-" + value(row, 12 + item, dictionary)));
            }
        });
        if (row % 13 == 6) {
            r.setNull("counts");
        }
        else {
            r.setMap("counts", m -> m
                    .addEntry(e -> e.setString("key", "k-" + value(row, 14, dictionary)).setLong("value", value(row, 15, dictionary)))
                    .addEntry(e -> e.setString("key", "absent").setNull("value")));
        }
    }

    private static void assertRow(Held held, int row, boolean dictionary) {
        if (row % 7 == 3) {
            assertThat(held.person()).as("row %d", row).isNull();
        }
        else {
            assertThat(held.person().getString("name")).as("row %d", row)
                    .isEqualTo(row % 5 == 1 ? null : "name-" + value(row, 0, dictionary));
            assertThat(held.person().getLong("age")).as("row %d", row).isEqualTo(value(row, 1, dictionary));
        }

        if (row % 11 == 4) {
            assertThat(held.ints()).as("row %d", row).isNull();
        }
        else {
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < row % 4; i++) {
                expected.add(i == 1 ? null : (int) value(row, 2 + i, dictionary));
            }
            assertThat(held.ints().values()).as("row %d", row).isEqualTo(expected);
            assertThat(held.intView().size()).as("row %d", row).isEqualTo(expected.size());
            for (int i = 0; i < expected.size(); i++) {
                assertThat(held.intView().isNull(i)).as("row %d", row).isEqualTo(expected.get(i) == null);
                if (expected.get(i) != null) {
                    assertThat(held.intView().get(i)).as("row %d", row).isEqualTo(expected.get(i));
                }
            }
        }

        assertThat(held.longs().toArray()).as("row %d", row)
                .containsExactly(value(row, 6, dictionary), value(row, 7, dictionary));

        double[] vector = {value(row, 8, dictionary), value(row, 9, dictionary), value(row, 10, dictionary)};
        assertThat(held.vector().values()).as("row %d", row).containsExactly(vector[0], vector[1], vector[2]);
        assertThat(held.vectorView().toArray()).as("row %d", row).containsExactly(vector);

        assertThat(held.names().strings()).as("row %d", row).containsExactly("n-" + value(row, 11, dictionary), null);

        assertThat(held.items()).as("row %d", row).hasSize(1 + row % 3);
        for (int i = 0; i < held.items().size(); i++) {
            assertThat(held.items().get(i).getString("sku")).as("row %d", row)
                    .isEqualTo("sku-" + value(row, 12 + i, dictionary));
        }

        if (row % 13 == 6) {
            assertThat(held.counts()).as("row %d", row).isNull();
        }
        else {
            String key = "k-" + value(row, 14, dictionary);
            assertThat(held.counts().size()).as("row %d", row).isEqualTo(2);
            assertThat(held.counts().getEntries().get(0).getKey()).as("row %d", row).isEqualTo(key);
            assertThat(held.counts().getValue(key)).as("row %d", row).isEqualTo(value(row, 15, dictionary));
            assertThat(held.counts().containsKey("absent")).as("row %d", row).isTrue();
            assertThat(held.counts().getValue("absent")).as("row %d", row).isNull();
        }
    }

    /// The flyweights of one row, kept until the whole file has been read.
    private record Held(PqStruct person, PqList ints, PqIntList intView, PqLongList longs, PqList vector,
                        PqDoubleList vectorView, PqList names, List<PqStruct> items, PqMap counts) {
    }

    /// A Variant built over bytes copied out of the batch: held after the reader has moved on, on the
    /// row, list and map routes, it keeps its own value.
    @Test
    void variantsHeldAcrossBatchesKeepTheirOwnValue() throws IOException {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 3),
                SchemaElement.group("v", RepetitionType.REQUIRED, 2, LogicalType.variant(1)),
                SchemaElement.primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                SchemaElement.primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                SchemaElement.group("vs", RepetitionType.REQUIRED, 1, LogicalType.list()),
                SchemaElement.group("list", RepetitionType.REPEATED, 1),
                SchemaElement.group("element", RepetitionType.REQUIRED, 2, LogicalType.variant(1)),
                SchemaElement.primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                SchemaElement.primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                SchemaElement.group("vm", RepetitionType.REQUIRED, 1, LogicalType.map()),
                SchemaElement.group("key_value", RepetitionType.REPEATED, 2),
                SchemaElement.primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string()),
                SchemaElement.group("value", RepetitionType.REQUIRED, 2, LogicalType.variant(1)),
                SchemaElement.primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                SchemaElement.primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)));
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().rowGroupTargetRows(ROW_GROUP_ROWS).build())) {
            for (int i = 0; i < ROWS; i++) {
                int row = i;
                writer.rowWriter().writeRow(r -> r
                        .setStruct("v", v -> setVariant(v, row * 3))
                        .setList("vs", l -> l.addStruct(v -> setVariant(v, row * 3 + 1)))
                        .setMap("vm", m -> m.addEntry(e -> e.setString("key", "k")
                                .setStruct("value", v -> setVariant(v, row * 3 + 2)))));
            }
        }

        List<PqVariant> held = new ArrayList<>(ROWS * 3);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(ROWS / ROW_GROUP_ROWS);
            while (rows.hasNext()) {
                rows.next();
                held.add(rows.getVariant("v"));
                held.add(rows.getList("vs").variants().get(0));
                held.add(rows.getMap("vm").getEntries().get(0).getVariantValue());
            }
        }

        assertThat(held).hasSize(ROWS * 3);
        for (int i = 0; i < held.size(); i++) {
            assertThat(held.get(i).asInt()).as("value %d", i).isEqualTo(i);
        }
    }

    /// Writes `value` as an `INT32` Variant with an empty metadata dictionary.
    private static void setVariant(StructBuilder variant, int value) {
        byte[] encoded = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) (5 << 2)).putInt(value).array();
        variant.setBinary("metadata", new byte[]{ 0x01, 0x00, 0x00 }).setBinary("value", encoded);
    }
}
