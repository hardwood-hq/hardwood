/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The nested half of the write-path interop gate (`_designs/WRITER_INTEROP_GATE.md`): Hardwood
/// writes the `struct`, `LIST` and `MAP` shapes, and parquet-java reads them back.
///
/// These are the shapes that carry repetition and definition level streams, which a flat column
/// does not exercise at all. The distinctions the levels encode — an empty list against an absent
/// one, a null element inside a present list, a null `struct` instance whose leaves still occupy
/// a definition level — are exactly what a level-stream defect erases, and none of them is
/// visible from the values alone.
///
/// The flat matrix is in [WriterInteropTest].
class WriterNestedInteropTest {

    // ==================== Structs ====================

    @Test
    void requiredAndOptionalStructs(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .struct("origin", RepetitionType.REQUIRED, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED))
                .struct("person", RepetitionType.OPTIONAL, s -> s
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                        .addColumn("born", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        // Record 1 has no person at all; record 2 has one whose name is null.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .ints("origin.x", new int[] { 10, 20, 30 })
                .struct("person", Validity.ofNulls(new boolean[] { false, true, false }))
                .bytes("person.name", new byte[][] { utf8("ada"), utf8("ignored"), utf8("alan") },
                        new boolean[] { false, false, true })
                .ints("person.born", new int[] { 1815, 0, 1912 }));

        assertThat(rows).hasSize(3);
        for (int r = 0; r < 3; r++) {
            assertThat(rows.get(r).getGroup("origin", 0).getInteger("x", 0))
                    .as("origin.x of row %d", r).isEqualTo(10 * (r + 1));
        }

        assertThat(count(rows.get(0), "person")).as("row 0 has a person").isOne();
        assertThat(rows.get(0).getGroup("person", 0).getBinary("name", 0).getBytes()).isEqualTo(utf8("ada"));
        assertThat(rows.get(0).getGroup("person", 0).getInteger("born", 0)).isEqualTo(1815);

        assertThat(count(rows.get(1), "person")).as("row 1 has no person").isZero();

        Group present = rows.get(2).getGroup("person", 0);
        assertThat(count(present, "name")).as("row 2 has a person with no name").isZero();
        assertThat(present.getInteger("born", 0)).isEqualTo(1912);
    }

    @Test
    void nestedStructs(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .struct("a", RepetitionType.OPTIONAL, outer -> outer
                        .struct("b", RepetitionType.OPTIONAL, inner -> inner
                                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // Row 0: a.b.v = 1; row 1: a present, b absent; row 2: a absent; row 3: a.b present, v null.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .struct("a", Validity.ofNulls(new boolean[] { false, false, true, false }))
                .struct("a.b", Validity.ofNulls(new boolean[] { false, true, true, false }))
                .ints("a.b.v", new int[] { 1, 0, 0, 0 }, new boolean[] { false, true, true, true }));

        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).getGroup("a", 0).getGroup("b", 0).getInteger("v", 0)).isEqualTo(1);
        assertThat(count(rows.get(1).getGroup("a", 0), "b")).as("row 1 has no b").isZero();
        assertThat(count(rows.get(2), "a")).as("row 2 has no a").isZero();
        assertThat(count(rows.get(3).getGroup("a", 0).getGroup("b", 0), "v")).as("row 3 has no v").isZero();
    }

    // ==================== Lists ====================

    @Test
    void optionalListOfOptionalElements(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("phones", RepetitionType.OPTIONAL,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        // Row 0: [1,2]; row 1: []; row 2: null; row 3: [3, null, 5].
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("phones", new int[] { 0, 2, 2, 2, 5 },
                        Validity.ofNulls(new boolean[] { false, false, true, false }))
                .ints("phones.list.element", new int[] { 1, 2, 3, 0, 5 },
                        new boolean[] { false, false, false, true, false }));

        assertThat(intLists(rows, "phones")).containsExactly(
                List.of(1, 2), List.of(), null, Arrays.asList(3, null, 5));
    }

    @Test
    void requiredListOfRequiredElements(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("v", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        // No outer optional level: the definition levels only tell an empty list from an element.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("v", new int[] { 0, 2, 2, 3 })
                .ints("v.list.element", new int[] { 1, 2, 3 }));

        assertThat(intLists(rows, "v")).containsExactly(List.of(1, 2), List.of(), List.of(3));
    }

    @Test
    void listOfLists(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("m", RepetitionType.OPTIONAL, el -> el.list(RepetitionType.OPTIONAL,
                        inner -> inner.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // Row 0: [[1,2],[3]]; 1: []; 2: null; 3: [[]]; 4: [null] — two repetition levels.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("m", new int[] { 0, 2, 2, 2, 3, 4 },
                        Validity.ofNulls(new boolean[] { false, false, true, false, false }))
                .list("m.list.element", new int[] { 0, 2, 3, 3, 3 },
                        Validity.ofNulls(new boolean[] { false, false, false, true }))
                .ints("m.list.element.list.element", new int[] { 1, 2, 3 }));

        List<List<List<Integer>>> actual = new ArrayList<>();
        for (Group row : rows) {
            if (count(row, "m") == 0) {
                actual.add(null);
                continue;
            }
            Group outer = row.getGroup("m", 0);
            List<List<Integer>> inner = new ArrayList<>();
            for (int i = 0; i < count(outer, "list"); i++) {
                Group entry = outer.getGroup("list", i);
                inner.add(count(entry, "element") == 0 ? null : intList(entry.getGroup("element", 0)));
            }
            actual.add(inner);
        }

        assertThat(actual).containsExactly(
                List.of(List.of(1, 2), List.of(3)),
                List.of(),
                null,
                List.of(List.of()),
                Arrays.asList((List<Integer>) null));
    }

    @Test
    void listOfStructs(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("people", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("y", PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // Row 0: [{1,10}, {2,null}]; row 1: [{3,30}].
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("people", new int[] { 0, 2, 3 })
                .ints("people.list.element.x", new int[] { 1, 2, 3 })
                .ints("people.list.element.y", new int[] { 10, 0, 30 },
                        new boolean[] { false, true, false }));

        Group first = rows.get(0).getGroup("people", 0);
        assertThat(count(first, "list")).isEqualTo(2);
        assertThat(first.getGroup("list", 0).getGroup("element", 0).getInteger("x", 0)).isEqualTo(1);
        assertThat(first.getGroup("list", 0).getGroup("element", 0).getInteger("y", 0)).isEqualTo(10);
        Group secondElement = first.getGroup("list", 1).getGroup("element", 0);
        assertThat(secondElement.getInteger("x", 0)).isEqualTo(2);
        assertThat(count(secondElement, "y")).as("second element has no y").isZero();

        Group second = rows.get(1).getGroup("people", 0);
        assertThat(count(second, "list")).isOne();
        assertThat(second.getGroup("list", 0).getGroup("element", 0).getInteger("x", 0)).isEqualTo(3);
    }

    @Test
    void listWithNullStructElement(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("people", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL,
                        s -> s.addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        // One record whose middle element is a null struct rather than a struct with null fields.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("people", new int[] { 0, 3 })
                .struct("people.list.element", Validity.ofNulls(new boolean[] { false, true, false }))
                .ints("people.list.element.x", new int[] { 1, 0, 3 }));

        Group list = rows.get(0).getGroup("people", 0);
        assertThat(count(list, "list")).isEqualTo(3);
        assertThat(list.getGroup("list", 0).getGroup("element", 0).getInteger("x", 0)).isEqualTo(1);
        assertThat(count(list.getGroup("list", 1), "element")).as("middle element is a null struct").isZero();
        assertThat(list.getGroup("list", 2).getGroup("element", 0).getInteger("x", 0)).isEqualTo(3);
    }

    // ==================== Maps ====================

    @Test
    void optionalMapOfIntToInt(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        // Row 0: {1:10, 2:null}; row 1: {}; row 2: null; row 3: {3:30}.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .map("props", new int[] { 0, 2, 2, 2, 3 },
                        Validity.ofNulls(new boolean[] { false, false, true, false }))
                .ints("props.key_value.key", new int[] { 1, 2, 3 })
                .ints("props.key_value.value", new int[] { 10, 0, 30 },
                        new boolean[] { false, true, false }));

        Map<Integer, Integer> first = new LinkedHashMap<>();
        first.put(1, 10);
        first.put(2, null);
        assertThat(intMap(rows.get(0), "props")).isEqualTo(first);
        assertThat(intMap(rows.get(1), "props")).isEmpty();
        assertThat(intMap(rows.get(2), "props")).as("row 2 has no map").isNull();
        assertThat(intMap(rows.get(3), "props")).isEqualTo(Map.of(3, 30));
    }

    @Test
    void requiredMapWithStringKeys(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .map("props", RepetitionType.REQUIRED, PhysicalType.BYTE_ARRAY,
                        LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();

        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .map("props", new int[] { 0, 2, 2 })
                .bytes("props.key_value.key", new byte[][] { utf8("a"), utf8("b") })
                .longs("props.key_value.value", new long[] { 1L, 2L }));

        Group entries = rows.get(0).getGroup("props", 0);
        assertThat(count(entries, "key_value")).isEqualTo(2);
        assertThat(entries.getGroup("key_value", 0).getBinary("key", 0).getBytes()).isEqualTo(utf8("a"));
        assertThat(entries.getGroup("key_value", 0).getLong("value", 0)).isEqualTo(1L);
        assertThat(entries.getGroup("key_value", 1).getBinary("key", 0).getBytes()).isEqualTo(utf8("b"));
        assertThat(entries.getGroup("key_value", 1).getLong("value", 0)).isEqualTo(2L);
        assertThat(count(rows.get(1).getGroup("props", 0), "key_value")).as("row 1 is an empty map").isZero();
    }

    @Test
    void mapOfIntToListOfInts(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .map("m", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        value -> value.list(RepetitionType.OPTIONAL,
                                el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // Row 0: {1:[10,11], 2:[]}; row 1: {3:null}.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .map("m", new int[] { 0, 2, 3 })
                .ints("m.key_value.key", new int[] { 1, 2, 3 })
                .list("m.key_value.value", new int[] { 0, 2, 2, 2 },
                        Validity.ofNulls(new boolean[] { false, false, true }))
                .ints("m.key_value.value.list.element", new int[] { 10, 11 }));

        Group first = rows.get(0).getGroup("m", 0);
        assertThat(first.getGroup("key_value", 0).getInteger("key", 0)).isEqualTo(1);
        assertThat(intList(first.getGroup("key_value", 0).getGroup("value", 0))).containsExactly(10, 11);
        assertThat(first.getGroup("key_value", 1).getInteger("key", 0)).isEqualTo(2);
        assertThat(intList(first.getGroup("key_value", 1).getGroup("value", 0))).isEmpty();

        Group second = rows.get(1).getGroup("m", 0);
        assertThat(second.getGroup("key_value", 0).getInteger("key", 0)).isEqualTo(3);
        assertThat(count(second.getGroup("key_value", 0), "value")).as("row 1's value list is absent").isZero();
    }

    @Test
    void mapOfIntToStruct(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .map("m", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        value -> value.struct(RepetitionType.OPTIONAL, s -> s
                                .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        // Row 0: {1:{x=10}, 2:null}.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .map("m", new int[] { 0, 2 })
                .ints("m.key_value.key", new int[] { 1, 2 })
                .struct("m.key_value.value", Validity.ofNulls(new boolean[] { false, true }))
                .ints("m.key_value.value.x", new int[] { 10, 0 }));

        Group entries = rows.get(0).getGroup("m", 0);
        assertThat(entries.getGroup("key_value", 0).getGroup("value", 0).getInteger("x", 0)).isEqualTo(10);
        assertThat(count(entries.getGroup("key_value", 1), "value")).as("second value is null").isZero();
    }

    // ==================== A nullable struct enclosing a repeated field (#1026) ====================

    @Test
    void optionalStructEnclosingOptionalList(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .list("phones", RepetitionType.OPTIONAL,
                                el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // Row 0: s null; row 1: s present, phones null; row 2: phones empty; row 3: phones = [null];
        // row 4: phones = [42].
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .struct("s", Validity.ofNulls(new boolean[] { true, false, false, false, false }))
                .list("s.phones", new int[] { 0, 0, 0, 0, 1, 2 },
                        Validity.ofNulls(new boolean[] { true, true, false, false, false }))
                .ints("s.phones.list.element", new int[] { 0, 42 }, new boolean[] { true, false }));

        assertThat(count(rows.get(0), "s")).as("row 0 has no s").isZero();
        assertThat(count(rows.get(1).getGroup("s", 0), "phones")).as("row 1's phones is absent").isZero();
        assertThat(readIntList(rows.get(2).getGroup("s", 0), "phones")).as("row 2's phones is empty").isEmpty();
        assertThat(readIntList(rows.get(3).getGroup("s", 0), "phones")).containsExactly((Integer) null);
        assertThat(readIntList(rows.get(4).getGroup("s", 0), "phones")).containsExactly(42);
    }

    @Test
    void optionalStructEnclosingOptionalMap(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                                value -> value.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // Row 0: s null; row 1: s present, props null; row 2: props empty; row 3: props = {1:30}.
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .struct("s", Validity.ofNulls(new boolean[] { true, false, false, false }))
                .map("s.props", new int[] { 0, 0, 0, 0, 1 },
                        Validity.ofNulls(new boolean[] { true, true, false, false }))
                .ints("s.props.key_value.key", new int[] { 1 })
                .ints("s.props.key_value.value", new int[] { 30 }));

        assertThat(count(rows.get(0), "s")).as("row 0 has no s").isZero();
        assertThat(count(rows.get(1).getGroup("s", 0), "props")).as("row 1's props is absent").isZero();
        assertThat(intMap(rows.get(2).getGroup("s", 0), "props")).as("row 2's props is empty").isEmpty();
        assertThat(intMap(rows.get(3).getGroup("s", 0), "props")).isEqualTo(Map.of(1, 30));
    }

    // ==================== Boundaries ====================

    @Test
    void listsSurvivePageAndRowGroupBoundaries(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("v", RepetitionType.OPTIONAL,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        // Records cycle through empty, absent, single and three-element lists, so every level
        // combination recurs on both sides of every page and row-group boundary.
        int records = 6_000;
        int[] offsets = new int[records + 1];
        boolean[] listNulls = new boolean[records];
        List<Integer> flattened = new ArrayList<>();
        for (int r = 0; r < records; r++) {
            listNulls[r] = r % 4 == 2;
            int entries = switch (r % 4) {
                case 0 -> 0;
                case 1 -> 1;
                case 2 -> 0; // absent
                default -> 3;
            };
            for (int e = 0; e < entries; e++) {
                flattened.add(r + e);
            }
            offsets[r + 1] = flattened.size();
        }
        int[] elements = flattened.stream().mapToInt(Integer::intValue).toArray();
        boolean[] elementNulls = new boolean[elements.length];
        for (int e = 0; e < elements.length; e++) {
            elementNulls[e] = e % 7 == 0;
        }

        WriterConfig config = WriterConfig.builder().pageTargetBytes(1024).rowGroupBufferTargetBytes(4096).build();
        Path file = write(dir, schema, config, batch -> batch
                .list("v", offsets, Validity.ofNulls(listNulls))
                .ints("v.list.element", elements, elementNulls));

        ParquetMetadata footer = ParquetJavaReader.readFooter(file);
        assertThat(footer.getBlocks()).as("row groups").hasSizeGreaterThan(1);
        assertThat(ParquetJavaReader.readPages(file).dataPageCount()).as("data pages").isGreaterThan(1);

        List<Group> rows = ParquetJavaReader.readGroups(file);
        assertThat(rows).hasSize(records);
        for (int r = 0; r < records; r++) {
            List<Integer> actual = readIntList(rows.get(r), "v");
            if (listNulls[r]) {
                assertThat(actual).as("record %d is an absent list", r).isNull();
                continue;
            }
            List<Integer> expected = new ArrayList<>();
            for (int e = offsets[r]; e < offsets[r + 1]; e++) {
                expected.add(elementNulls[e] ? null : elements[e]);
            }
            assertThat(actual).as("record %d", r).isEqualTo(expected);
        }
    }

    // ==================== The row-oriented layer ====================

    /// The same nested shapes produced record by record rather than column by column. The row
    /// layer stages into a `ColumnBatch` and submits it through the columnar path, so what
    /// these add to the gate is the record-shaped entry point over each nesting shape: that
    /// the offsets, per-instance nulls and phantom leaf slots it derives from a filler are the
    /// ones parquet-java expects to see.

    @Test
    void rowWrittenStructsIncludingAbsentOnes(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("person", RepetitionType.OPTIONAL, s -> s
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
                        .addColumn("born", PhysicalType.INT32, RepetitionType.REQUIRED)
                        // A REQUIRED fixed-width leaf: the absent record gives it a slot the row
                        // layer fills with a placeholder, which must not reach the file.
                        .addColumn("key", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 4))
                .build();
        byte[] firstKey = { 1, 2, 3, 4 };
        byte[] lastKey = { 5, 6, 7, 8 };

        List<Group> rows = writeRowsAndRead(dir, schema, writer -> {
            writer.writeRow(row -> row.setInt("id", 1).setStruct("person",
                    person -> person.setBinary("name", utf8("ada")).setInt("born", 1815)
                            .setBinary("key", firstKey)));
            writer.writeRow(row -> row.setInt("id", 2));
            writer.writeRow(row -> row.setInt("id", 3).setStruct("person",
                    person -> person.setInt("born", 1912).setBinary("key", lastKey)));
        });

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getGroup("person", 0).getBinary("name", 0).getBytes()).isEqualTo(utf8("ada"));
        assertThat(rows.get(0).getGroup("person", 0).getInteger("born", 0)).isEqualTo(1815);
        assertThat(rows.get(0).getGroup("person", 0).getBinary("key", 0).getBytes()).isEqualTo(firstKey);
        assertThat(count(rows.get(1), "person")).as("record 1 has no person").isZero();
        assertThat(count(rows.get(2).getGroup("person", 0), "name")).as("record 2 has no name").isZero();
        assertThat(rows.get(2).getGroup("person", 0).getInteger("born", 0)).isEqualTo(1912);
        assertThat(rows.get(2).getGroup("person", 0).getBinary("key", 0).getBytes()).isEqualTo(lastKey);
    }

    @Test
    void rowWrittenListsIncludingEmptyAbsentAndNullEntries(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("v", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        List<Group> rows = writeRowsAndRead(dir, schema, writer -> {
            writer.writeRow(row -> row.setList("v", v -> v.addInt(1).addNull().addInt(3)));
            writer.writeRow(row -> row.setList("v", v -> { }));
            writer.writeRow(row -> { });
            writer.writeRow(row -> row.setList("v", v -> v.addInt(4)));
        });

        assertThat(readIntList(rows.get(0), "v")).containsExactly(1, null, 3);
        assertThat(readIntList(rows.get(1), "v")).as("record 1 is an empty list").isEmpty();
        assertThat(readIntList(rows.get(2), "v")).as("record 2 is an absent list").isNull();
        assertThat(readIntList(rows.get(3), "v")).containsExactly(4);
    }

    @Test
    void rowWrittenListOfStructsAndListOfLists(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .list("people", RepetitionType.REQUIRED, element -> element.struct(RepetitionType.REQUIRED,
                        person -> person.addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)))
                .list("grid", RepetitionType.REQUIRED, element -> element.list(RepetitionType.REQUIRED,
                        inner -> inner.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        List<Group> rows = writeRowsAndRead(dir, schema, writer -> {
            writer.writeRow(row -> row
                    .setList("people", people -> people
                            .addStruct(person -> person.setBinary("name", utf8("ada")))
                            .addStruct(person -> person.setBinary("name", utf8("alan"))))
                    .setList("grid", grid -> grid
                            .addList(inner -> inner.addInt(1).addInt(2))
                            .addList(inner -> inner.addInt(3))));
            writer.writeRow(row -> row
                    .setList("people", people -> people.addStruct(p -> p.setBinary("name", utf8("grace"))))
                    .setList("grid", grid -> { }));
        });

        Group people = rows.get(0).getGroup("people", 0);
        assertThat(count(people, "list")).isEqualTo(2);
        assertThat(people.getGroup("list", 0).getGroup("element", 0).getBinary("name", 0).getBytes())
                .isEqualTo(utf8("ada"));
        assertThat(people.getGroup("list", 1).getGroup("element", 0).getBinary("name", 0).getBytes())
                .isEqualTo(utf8("alan"));

        Group grid = rows.get(0).getGroup("grid", 0);
        assertThat(readIntList(grid.getGroup("list", 0), "element")).containsExactly(1, 2);
        assertThat(readIntList(grid.getGroup("list", 1), "element")).containsExactly(3);
        assertThat(count(rows.get(1).getGroup("grid", 0), "list")).as("record 1's grid is empty").isZero();
    }

    @Test
    void rowWrittenMaps(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();

        List<Group> rows = writeRowsAndRead(dir, schema, writer -> {
            writer.writeRow(row -> row.setMap("props", props -> props
                    .addEntry(entry -> entry.setString("key", "reads").setLong("value", 12))
                    .addEntry(entry -> entry.setString("key", "writes").setNull("value"))));
            writer.writeRow(row -> row.setMap("props", props -> { }));
            writer.writeRow(row -> { });
        });

        Group entries = rows.get(0).getGroup("props", 0);
        assertThat(count(entries, "key_value")).isEqualTo(2);
        assertThat(entries.getGroup("key_value", 0).getBinary("key", 0).getBytes()).isEqualTo(utf8("reads"));
        assertThat(entries.getGroup("key_value", 0).getLong("value", 0)).isEqualTo(12L);
        assertThat(entries.getGroup("key_value", 1).getBinary("key", 0).getBytes()).isEqualTo(utf8("writes"));
        assertThat(count(entries.getGroup("key_value", 1), "value")).as("the second value is null").isZero();
        assertThat(count(rows.get(1).getGroup("props", 0), "key_value")).as("record 1 is an empty map").isZero();
        assertThat(count(rows.get(2), "props")).as("record 2 has no map").isZero();
    }

    /// Records written through the row layer across several of its staged batches, so the
    /// per-batch reset of the nested staging — list offsets, per-instance null masks, the leaf
    /// counts that are not the record count — is exercised against an external reader.
    @Test
    void rowWrittenNestedRecordsCrossStagedBatches(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.builder("nested")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("person", RepetitionType.OPTIONAL, s -> s
                        .addColumn("born", PhysicalType.INT32, RepetitionType.REQUIRED))
                .list("v", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int records = 2_500;
        List<Group> rows = writeRowsAndRead(dir, schema, writer -> {
            for (int r = 0; r < records; r++) {
                int record = r;
                writer.writeRow(row -> {
                    row.setInt("id", record);
                    if (record % 3 != 0) {
                        row.setStruct("person", person -> person.setInt("born", 1900 + record % 100));
                    }
                    if (record % 5 != 0) {
                        row.setList("v", v -> {
                            for (int e = 0; e <= record % 4; e++) {
                                if (e == 2) {
                                    v.addNull();
                                }
                                else {
                                    v.addInt(record * 10 + e);
                                }
                            }
                        });
                    }
                });
            }
        });

        assertThat(rows).hasSize(records);
        for (int r = 0; r < records; r++) {
            Group row = rows.get(r);
            assertThat(row.getInteger("id", 0)).as("record %d id", r).isEqualTo(r);
            if (r % 3 == 0) {
                assertThat(count(row, "person")).as("record %d has no person", r).isZero();
            }
            else {
                assertThat(row.getGroup("person", 0).getInteger("born", 0)).as("record %d born", r)
                        .isEqualTo(1900 + r % 100);
            }
            if (r % 5 == 0) {
                assertThat(readIntList(row, "v")).as("record %d has no list", r).isNull();
                continue;
            }
            List<Integer> expected = new ArrayList<>();
            for (int e = 0; e <= r % 4; e++) {
                expected.add(e == 2 ? null : r * 10 + e);
            }
            assertThat(readIntList(row, "v")).as("record %d list", r).isEqualTo(expected);
        }
    }

    // ==================== Helpers ====================

    /// Writes records through the row-oriented layer, which may throw a checked [IOException].
    private interface RowWrite {
        void accept(RowWriter rows) throws IOException;
    }

    private List<Group> writeRowsAndRead(Path dir, FileSchema schema, RowWrite filler) throws IOException {
        Path file = dir.resolve("nested-rows.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            filler.accept(writer.rowWriter());
        }
        return ParquetJavaReader.readGroups(file);
    }

    private List<Group> writeAndRead(Path dir, FileSchema schema, Consumer<ColumnBatch> filler) throws IOException {
        return ParquetJavaReader.readGroups(write(dir, schema, WriterConfig.defaults(), filler));
    }

    private Path write(Path dir, FileSchema schema, WriterConfig config, Consumer<ColumnBatch> filler)
            throws IOException {
        Path file = dir.resolve("nested.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            writer.columnWriter().writeBatch(filler);
        }
        return file;
    }

    /// How many instances of a field a group holds; zero means the field is absent, which is how
    /// parquet-java represents a null leaf, a null `struct` instance and an absent list alike.
    /// A legacy two-level list, whose entry is the element rather than a group holding one.
    ///
    /// `FileSchema.Builder` will not declare this shape, so it reaches the writer only from a
    /// schema read back off an existing file — which is how a copy of a parquet-mr, Hive or Avro
    /// file reaches it. The writer produced one one-element list per row for it until #1035, and
    /// what says it produces a real list now is another implementation reading one back: our own
    /// reader accepts the shape by design and would agree with a file that only it can read.
    @Test
    void legacyTwoLevelListWithALeafEntry(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("nested", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.REPEATED)));

        // Row 0: [1,2]; row 1: []; row 2: [3,4,5].
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("items", new int[] { 0, 2, 2, 5 })
                .ints("items.element", new int[] { 1, 2, 3, 4, 5 }));

        assertThat(rows).hasSize(3);
        assertThat(twoLevelIntLists(rows, "items")).containsExactly(
                List.of(1, 2), List.of(), List.of(3, 4, 5));
    }

    /// The other legacy spelling, whose entry is a repeated group of fields. Both of the group's
    /// leaves repeat under the single layer the annotation supplies, so a reader that disagreed
    /// about where the entries begin would show them misaligned against each other.
    @Test
    void legacyTwoLevelListOfStructs(@TempDir Path dir) throws IOException {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("nested", 1),
                SchemaElement.group("items", RepetitionType.OPTIONAL, 1, LogicalType.list()),
                SchemaElement.group("element", RepetitionType.REPEATED, 2),
                SchemaElement.primitive("a", PhysicalType.INT32, RepetitionType.REQUIRED),
                SchemaElement.primitive("b", PhysicalType.INT32, RepetitionType.REQUIRED)));

        // Row 0: [{1,10},{2,20}]; row 1: [{3,30}].
        List<Group> rows = writeAndRead(dir, schema, batch -> batch
                .list("items", new int[] { 0, 2, 3 })
                .ints("items.element.a", new int[] { 1, 2, 3 })
                .ints("items.element.b", new int[] { 10, 20, 30 }));

        assertThat(rows).hasSize(2);
        assertThat(twoLevelFieldLists(rows, "items", "a")).containsExactly(List.of(1, 2), List.of(3));
        assertThat(twoLevelFieldLists(rows, "items", "b")).containsExactly(List.of(10, 20), List.of(30));
    }

    /// The entries of a legacy two-level `LIST` whose entry is a leaf, per row.
    private static List<List<Integer>> twoLevelIntLists(List<Group> rows, String field) {
        List<List<Integer>> lists = new ArrayList<>(rows.size());
        for (Group row : rows) {
            List<Integer> values = new ArrayList<>();
            if (count(row, field) > 0) {
                Group list = row.getGroup(field, 0);
                for (int i = 0; i < count(list, "element"); i++) {
                    values.add(list.getInteger("element", i));
                }
            }
            lists.add(values);
        }
        return lists;
    }

    /// One field of each entry of a legacy two-level `LIST` whose entry is a group, per row.
    private static List<List<Integer>> twoLevelFieldLists(List<Group> rows, String field, String leaf) {
        List<List<Integer>> lists = new ArrayList<>(rows.size());
        for (Group row : rows) {
            List<Integer> values = new ArrayList<>();
            if (count(row, field) > 0) {
                Group list = row.getGroup(field, 0);
                for (int i = 0; i < count(list, "element"); i++) {
                    values.add(list.getGroup("element", i).getInteger(leaf, 0));
                }
            }
            lists.add(values);
        }
        return lists;
    }

    private static int count(Group group, String field) {
        return group.getFieldRepetitionCount(field);
    }

    /// A row's `LIST` field as a nullable list of nullable `INT32`s, or `null` if the list itself
    /// is absent.
    private static List<Integer> readIntList(Group row, String field) {
        return count(row, field) == 0 ? null : intList(row.getGroup(field, 0));
    }

    /// The entries of a canonical 3-level `LIST` group, a `null` entry for an absent element.
    private static List<Integer> intList(Group list) {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < count(list, "list"); i++) {
            Group entry = list.getGroup("list", i);
            values.add(count(entry, "element") == 0 ? null : entry.getInteger("element", 0));
        }
        return values;
    }

    private static List<List<Integer>> intLists(List<Group> rows, String field) {
        List<List<Integer>> lists = new ArrayList<>(rows.size());
        for (Group row : rows) {
            lists.add(readIntList(row, field));
        }
        return lists;
    }

    /// A row's `MAP` field as an insertion-ordered map, a `null` mapped value for an absent one,
    /// or `null` if the map itself is absent.
    private static Map<Integer, Integer> intMap(Group row, String field) {
        if (count(row, field) == 0) {
            return null;
        }
        Group entries = row.getGroup(field, 0);
        Map<Integer, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < count(entries, "key_value"); i++) {
            Group entry = entries.getGroup("key_value", i);
            map.put(entry.getInteger("key", 0), count(entry, "value") == 0 ? null : entry.getInteger("value", 0));
        }
        return map;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
