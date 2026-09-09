/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.writer.WriterTestSupport.mapOf;
import static dev.hardwood.writer.WriterTestSupport.readListOfInts;
import static dev.hardwood.writer.WriterTestSupport.readMapOfInts;
import static dev.hardwood.writer.WriterTestSupport.readNullable;
import static org.assertj.core.api.Assertions.assertThat;

/// Round-trip tests for the nested shapes: structs, lists and maps, and the combinations of
/// them the shredder has to encode into repetition and definition levels.
///
/// The distinctions the levels carry — an empty list against an absent one, a null element
/// inside a present list, a null struct whose leaves still occupy a level — are invisible in
/// the values alone, so each shape is read back through the reader that reconstructs them.
class WriterNestedRoundTripTest {

    @Test
    void writesAndReadsBackStructWithRequiredAndOptionalLeaves() throws Exception {
        // optional group address { required int32 street; optional int32 zip }
        // record 0: address null (street/zip absent); 1: present, zip null; 2,3: fully present.
        FileSchema schema = FileSchema.builder("schema")
                .struct("address", RepetitionType.OPTIONAL, s -> s
                        .addColumn("street", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        Validity addressNulls = Validity.ofNulls(new boolean[] { true, false, false, false });
        int[] street = { 0, 10, 20, 30 };
        int[] zip = { 0, 0, 200, 300 };
        boolean[] zipNulls = { false, true, false, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("address", addressNulls)
                    .ints("address.street", street)
                    .ints("address.zip", zip, zipNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int streetIdx = reader.getFileSchema().getColumn("address.street").columnIndex();
            int zipIdx = reader.getFileSchema().getColumn("address.zip").columnIndex();

            // street is absent only where the struct is null; zip also where zip itself is null.
            assertThat(readNullable(reader, streetIdx)).containsExactly(null, 10, 20, 30);
            assertThat(readNullable(reader, zipIdx)).containsExactly(null, null, 200, 300);

            // The STRUCT layer distinguishes a null struct from a present struct with a null leaf.
            try (ColumnReader column = reader.columnReader(zipIdx)) {
                assertThat(column.nextBatch()).isTrue();
                assertThat(column.getLayerCount()).isEqualTo(1);
                assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
                Validity struct = column.getLayerValidity(0);
                assertThat(struct.isNull(0)).isTrue();
                assertThat(struct.isNull(1)).isFalse();
                assertThat(struct.isNull(2)).isFalse();
            }
        }
    }

    @Test
    void writesAndReadsBackNestedOptionalStructDepthTwo() throws Exception {
        // optional group a { optional int32 b } — definition levels span 0, 1 and 2.
        // record 0: a null (def 0); 1: a present, b null (def 1); 2: fully present (def 2).
        FileSchema schema = FileSchema.builder("schema")
                .struct("a", RepetitionType.OPTIONAL, s -> s
                        .addColumn("b", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        Validity aNulls = Validity.ofNulls(new boolean[] { true, false, false });
        int[] b = { 0, 0, 42 };
        boolean[] bNulls = { false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.struct("a", aNulls).ints("a.b", b, bNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int bIdx = reader.getFileSchema().getColumn("a.b").columnIndex();
            assertThat(reader.getFileSchema().getColumn(bIdx).maxDefinitionLevel()).isEqualTo(2);
            assertThat(readNullable(reader, bIdx)).containsExactly(null, null, 42);

            try (ColumnReader column = reader.columnReader(bIdx)) {
                assertThat(column.nextBatch()).isTrue();
                Validity struct = column.getLayerValidity(0);
                assertThat(struct.isNull(0)).isTrue();  // a null
                assertThat(struct.isNull(1)).isFalse(); // a present (b null)
                assertThat(struct.isNull(2)).isFalse();
            }
        }
    }

    @Test
    void writesAndReadsBackRequiredStruct() throws Exception {
        // required group g { optional int32 x } — no STRUCT layer; x behaves like a flat
        // optional column, but the group nesting must still round-trip through the footer.
        FileSchema schema = FileSchema.builder("schema")
                .struct("g", RepetitionType.REQUIRED, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] x = { 1, 0, 3 };
        boolean[] xNulls = { false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.ints("g.x", x, xNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int xIdx = reader.getFileSchema().getColumn("g.x").columnIndex();
            assertThat(readNullable(reader, xIdx)).containsExactly(1, null, 3);
        }
    }

    @Test
    void writesAndReadsBackListOfInts() throws Exception {
        // optional group phones (LIST) { repeated group list { optional int32 element } }
        // record 0: [1,2]; 1: [] (empty); 2: null (absent list); 3: [3, null, 5].
        FileSchema schema = FileSchema.builder("schema")
                .list("phones", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 5 };
        Validity listNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] elements = { 1, 2, 3, 0, 5 };
        boolean[] elementNulls = { false, false, false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("phones", offsets, listNulls)
                    .ints("phones.list.element", elements, elementNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int leaf = reader.getFileSchema().getColumn("phones.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf))
                    .containsExactly(List.of(1, 2), List.of(), null, Arrays.asList(3, null, 5));
        }
    }

    @Test
    void writesAndReadsBackListOfLists() throws Exception {
        // optional [[optional int]] — two repetition levels.
        // record 0: [[1,2],[3]]; 1: []; 2: null; 3: [[]] (one empty inner); 4: [null] (one null inner).
        FileSchema schema = FileSchema.builder("schema")
                .list("m", RepetitionType.OPTIONAL,
                        el -> el.list(RepetitionType.OPTIONAL, inner -> inner.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        int[] outerOffsets = { 0, 2, 2, 2, 3, 4 };
        Validity outerNulls = Validity.ofNulls(new boolean[] { false, false, true, false, false });
        int[] innerOffsets = { 0, 2, 3, 3, 3 };
        Validity innerNulls = Validity.ofNulls(new boolean[] { false, false, false, true });
        int[] elements = { 1, 2, 3 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("m", outerOffsets, outerNulls)
                    .list("m.list.element", innerOffsets, innerNulls)
                    .ints("m.list.element.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(
                        reader.getFileSchema().getColumn("m.list.element.list.element").columnIndex())) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getRecordCount()).isEqualTo(5);
            assertThat(column.getLayerCount()).isEqualTo(2);

            List<List<List<Integer>>> actual = new ArrayList<>();
            int[] outer = column.getLayerOffsets(0);
            int[] inner = column.getLayerOffsets(1);
            Validity outerV = column.getLayerValidity(0);
            Validity innerV = column.getLayerValidity(1);
            int[] values = column.getInts();
            for (int r = 0; r < column.getRecordCount(); r++) {
                if (outerV.isNull(r)) {
                    actual.add(null);
                    continue;
                }
                List<List<Integer>> lists = new ArrayList<>();
                for (int i = outer[r]; i < outer[r + 1]; i++) {
                    if (innerV.isNull(i)) {
                        lists.add(null);
                        continue;
                    }
                    List<Integer> ints = new ArrayList<>();
                    for (int e = inner[i]; e < inner[i + 1]; e++) {
                        ints.add(values[e]);
                    }
                    lists.add(ints);
                }
                actual.add(lists);
            }
            assertThat(actual).containsExactly(
                    List.of(List.of(1, 2), List.of(3)),
                    List.of(),
                    null,
                    List.of(List.of()),
                    Arrays.asList((List<Integer>) null));
        }
    }

    @Test
    void writesAndReadsBackListOfStructs() throws Exception {
        // optional [ { required int32 x; optional int32 y } ]
        FileSchema schema = FileSchema.builder("schema")
                .list("people", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("y", PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // record 0: [{1,10},{2,null}]; 1: [{3,30}].
        int[] offsets = { 0, 2, 3 };
        int[] x = { 1, 2, 3 };
        int[] y = { 10, 0, 30 };
        boolean[] yNulls = { false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("people", offsets)
                    .ints("people.list.element.x", x)
                    .ints("people.list.element.y", y, yNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int xIdx = reader.getFileSchema().getColumn("people.list.element.x").columnIndex();
            int yIdx = reader.getFileSchema().getColumn("people.list.element.y").columnIndex();
            try (ColumnReader xr = reader.columnReader(xIdx); ColumnReader yr = reader.columnReader(yIdx)) {
                assertThat(xr.nextBatch()).isTrue();
                assertThat(yr.nextBatch()).isTrue();
                assertThat(xr.getLayerOffsets(0)).containsExactly(0, 2, 3);
                assertThat(Arrays.copyOf(xr.getInts(), xr.getValueCount())).containsExactly(1, 2, 3);
                int[] ys = yr.getInts();
                Validity yv = yr.getLeafValidity();
                assertThat(yv.isNull(1)).isTrue();
                assertThat(ys[0]).isEqualTo(10);
                assertThat(ys[2]).isEqualTo(30);
            }
        }
    }

    @Test
    void writesAndReadsBackRequiredList() throws Exception {
        // required list<required int32> — the list itself is never null, so there is no outer
        // optional level; def levels only distinguish an empty list from a present element.
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int[] offsets = { 0, 2, 2, 3 }; // record 0: [1,2]; record 1: []; record 2: [3]
        int[] elements = { 1, 2, 3 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.list("v", offsets).ints("v.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int leaf = reader.getFileSchema().getColumn("v.list.element").columnIndex();
            assertThat(readListOfInts(reader, leaf)).containsExactly(List.of(1, 2), List.of(), List.of(3));
        }
    }

    @Test
    void writesAndReadsBackListOfStructWithNullStructElement() throws Exception {
        // optional list< optional struct { required int32 x } >, one record whose middle
        // element is a null struct — distinct from an absent list and from a null leaf.
        FileSchema schema = FileSchema.builder("schema")
                .list("people", RepetitionType.OPTIONAL, el -> el.struct(RepetitionType.OPTIONAL, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        int[] offsets = { 0, 3 };
        Validity structNulls = Validity.ofNulls(new boolean[] { false, true, false });
        int[] x = { 1, 0, 3 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("people", offsets)
                    .struct("people.list.element", structNulls)
                    .ints("people.list.element.x", x));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(
                        reader.getFileSchema().getColumn("people.list.element.x").columnIndex())) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getRecordCount()).isEqualTo(1);
            // Three struct-element slots, the middle one an absent struct; the leaf is not
            // compacted, so its slot survives and reads back null with the two present values.
            assertThat(column.getLayerOffsets(0)).containsExactly(0, 3);
            assertThat(column.getValueCount()).isEqualTo(3);
            Validity structValidity = column.getLayerValidity(1);
            assertThat(structValidity.isNull(0)).isFalse();
            assertThat(structValidity.isNull(1)).isTrue();
            assertThat(structValidity.isNull(2)).isFalse();
            Validity leafValidity = column.getLeafValidity();
            assertThat(leafValidity.isNull(1)).isTrue();
            int[] xs = column.getInts();
            assertThat(xs[0]).isEqualTo(1);
            assertThat(xs[2]).isEqualTo(3);
        }
    }

    @Test
    void writesAndReadsBackMapOfIntToInt() throws Exception {
        // optional map<int32, optional int32> props — key/value share one REPEATED layer.
        // record 0: {1:10, 2:null}; 1: {} (empty); 2: null (absent map); 3: {3:30}.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        int[] offsets = { 0, 2, 2, 2, 3 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] keys = { 1, 2, 3 };
        int[] values = { 10, 0, 30 };
        boolean[] valueNulls = { false, true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .map("props", offsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .ints("props.key_value.value", values, valueNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int valIdx = reader.getFileSchema().getColumn("props.key_value.value").columnIndex();
            assertThat(reader.getFileSchema().getColumn(valIdx).maxDefinitionLevel()).isEqualTo(3);
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader vr = reader.columnReader(valIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(vr.nextBatch()).isTrue();
                assertThat(vr.getRecordCount()).isEqualTo(4);
                assertThat(vr.getLayerCount()).isEqualTo(1);
                assertThat(vr.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
                assertThat(readMapOfInts(kr, vr)).containsExactly(
                        mapOf(1, 10, 2, null), Map.of(), null, mapOf(3, 30, null, null));
            }
        }
    }

    @Test
    void writesAndReadsBackRequiredMap() throws Exception {
        // required map<int32, required int32> — the map itself is never null, so only an
        // empty map (zero-delta) and a present entry are distinguished.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.REQUIRED, PhysicalType.INT32,
                        v -> v.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int[] offsets = { 0, 2, 2, 3 }; // record 0: {1:10, 2:20}; 1: {}; 2: {3:30}
        int[] keys = { 1, 2, 3 };
        int[] values = { 10, 20, 30 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .map("props", offsets)
                    .ints("props.key_value.key", keys)
                    .ints("props.key_value.value", values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int valIdx = reader.getFileSchema().getColumn("props.key_value.value").columnIndex();
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader vr = reader.columnReader(valIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(vr.nextBatch()).isTrue();
                assertThat(readMapOfInts(kr, vr)).containsExactly(
                        mapOf(1, 10, 2, 20), Map.of(), Map.of(3, 30));
            }
        }
    }

    @Test
    void writesAndReadsBackMapOfIntToListOfInts() throws Exception {
        // optional map<int32, required list<required int32>> — a REPEATED value layer nested
        // inside the MAP's REPEATED layer, two repetition levels driven by two offset arrays.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.list(RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        // record 0: {1:[10,20], 2:[30]}; 1: {} (empty); 2: null (absent map).
        int[] mapOffsets = { 0, 2, 2, 2 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true });
        int[] keys = { 1, 2 };
        int[] listOffsets = { 0, 2, 3 };
        int[] elements = { 10, 20, 30 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .map("props", mapOffsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .list("props.key_value.value", listOffsets)
                    .ints("props.key_value.value.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int elemIdx = reader.getFileSchema().getColumn("props.key_value.value.list.element").columnIndex();
            try (ColumnReader kr = reader.columnReader(keyIdx); ColumnReader er = reader.columnReader(elemIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(er.nextBatch()).isTrue();
                assertThat(er.getLayerCount()).isEqualTo(2);

                List<Map<Integer, List<Integer>>> actual = new ArrayList<>();
                int[] mapOffs = er.getLayerOffsets(0);
                Validity mapV = er.getLayerValidity(0);
                int[] listOffs = er.getLayerOffsets(1);
                int[] ks = kr.getInts();
                int[] vals = er.getInts();
                for (int r = 0; r < er.getRecordCount(); r++) {
                    if (mapV.isNull(r)) {
                        actual.add(null);
                        continue;
                    }
                    Map<Integer, List<Integer>> map = new LinkedHashMap<>();
                    for (int e = mapOffs[r]; e < mapOffs[r + 1]; e++) {
                        List<Integer> list = new ArrayList<>();
                        for (int i = listOffs[e]; i < listOffs[e + 1]; i++) {
                            list.add(vals[i]);
                        }
                        map.put(ks[e], list);
                    }
                    actual.add(map);
                }
                assertThat(actual).containsExactly(
                        Map.of(1, List.of(10, 20), 2, List.of(30)), Map.of(), null);
            }
        }
    }

    @Test
    void writesAndReadsBackOptionalStructEnclosingOptionalList() throws Exception {
        // optional group s { optional group phones (LIST) { repeated group list { optional int32 element } } }
        // record 0: s null; 1: s present, phones null; 2: phones empty; 3: phones = [null]; 4: phones = [42].
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .list("phones", RepetitionType.OPTIONAL,
                                el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        Validity sNulls = Validity.ofNulls(new boolean[] { true, false, false, false, false });
        int[] offsets = { 0, 0, 0, 0, 1, 2 };
        Validity phonesNulls = Validity.ofNulls(new boolean[] { true, true, false, false, false });
        int[] elements = { 0, 42 };
        boolean[] elementNulls = { true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("s", sNulls)
                    .list("s.phones", offsets, phonesNulls)
                    .ints("s.phones.list.element", elements, elementNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(
                        reader.getFileSchema().getColumn("s.phones.list.element").columnIndex())) {
            assertThat(reader.getFileSchema().getColumn("s.phones.list.element").maxDefinitionLevel()).isEqualTo(4);
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLayerCount()).isEqualTo(2);
            assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(column.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);

            Validity structV = column.getLayerValidity(0);
            Validity listV = column.getLayerValidity(1);
            int[] listOffsets = column.getLayerOffsets(1);
            Validity leafV = column.getLeafValidity();
            int[] values = column.getInts();

            List<List<Integer>> actual = new ArrayList<>();
            for (int r = 0; r < column.getRecordCount(); r++) {
                if (structV.isNull(r) || listV.isNull(r)) {
                    actual.add(null);
                    continue;
                }
                List<Integer> list = new ArrayList<>();
                for (int e = listOffsets[r]; e < listOffsets[r + 1]; e++) {
                    list.add(leafV.isNull(e) ? null : values[e]);
                }
                actual.add(list);
            }
            assertThat(actual).containsExactly(
                    null, null, List.of(), Arrays.asList((Integer) null), List.of(42));
        }
    }

    @Test
    void writesAndReadsBackOptionalStructEnclosingRequiredList() throws Exception {
        // optional group s { required group phones (LIST) { repeated group list { required int32 element } } }
        // A REQUIRED list contributes no definition level of its own, so an absent struct and an
        // empty list differ by one level rather than two, and the struct's Validity is the only
        // input saying a record has no entries — the list has no mask to say it.
        // record 0: s null; 1: s present, phones empty; 2: phones = [7, 8].
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .list("phones", RepetitionType.REQUIRED,
                                el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED)))
                .build();

        Validity sNulls = Validity.ofNulls(new boolean[] { true, false, false });
        int[] offsets = { 0, 0, 0, 2 };
        int[] elements = { 7, 8 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("s", sNulls)
                    .list("s.phones", offsets)
                    .ints("s.phones.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(
                        reader.getFileSchema().getColumn("s.phones.list.element").columnIndex())) {
            assertThat(reader.getFileSchema().getColumn("s.phones.list.element").maxDefinitionLevel()).isEqualTo(2);
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLayerCount()).isEqualTo(2);
            assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(column.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);

            Validity structV = column.getLayerValidity(0);
            int[] listOffsets = column.getLayerOffsets(1);
            int[] values = column.getInts();

            List<List<Integer>> actual = new ArrayList<>();
            for (int r = 0; r < column.getRecordCount(); r++) {
                if (structV.isNull(r)) {
                    actual.add(null);
                    continue;
                }
                List<Integer> list = new ArrayList<>();
                for (int e = listOffsets[r]; e < listOffsets[r + 1]; e++) {
                    list.add(values[e]);
                }
                actual.add(list);
            }
            assertThat(actual).containsExactly(null, List.of(), List.of(7, 8));
        }
    }

    @Test
    void writesAndReadsBackOptionalStructEnclosingOptionalListNestedInsideAList() throws Exception {
        // optional group chapters (LIST) { repeated group list { optional group element {
        //   optional group sections (LIST) { repeated group list { required int32 element } } } } }
        // — the same nullable-struct-enclosing-a-list shape as above, but as a list element
        // rather than the record root, so the leaf-to-root walk crosses two REPEATED layers.
        // book 0: chapters = [{sections: [10, 20]}, null]; book 1: chapters = []; book 2: chapters = null.
        FileSchema schema = FileSchema.builder("schema")
                .list("chapters", RepetitionType.OPTIONAL, chapter -> chapter.struct(RepetitionType.OPTIONAL,
                        element -> element.list("sections", RepetitionType.OPTIONAL,
                                section -> section.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))))
                .build();

        int[] chapterOffsets = { 0, 2, 2, 2 };
        Validity chaptersNulls = Validity.ofNulls(new boolean[] { false, false, true });
        Validity elementNulls = Validity.ofNulls(new boolean[] { false, true });
        int[] sectionOffsets = { 0, 2, 2 };
        Validity sectionsNulls = Validity.ofNulls(new boolean[] { false, true });
        int[] elements = { 10, 20 };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .list("chapters", chapterOffsets, chaptersNulls)
                    .struct("chapters.list.element", elementNulls)
                    .list("chapters.list.element.sections", sectionOffsets, sectionsNulls)
                    .ints("chapters.list.element.sections.list.element", elements));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader column = reader.columnReader(reader.getFileSchema()
                        .getColumn("chapters.list.element.sections.list.element").columnIndex())) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLayerCount()).isEqualTo(3);
            assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(column.getLayerKind(1)).isEqualTo(LayerKind.STRUCT);
            assertThat(column.getLayerKind(2)).isEqualTo(LayerKind.REPEATED);

            int[] chapterOffs = column.getLayerOffsets(0);
            Validity chaptersV = column.getLayerValidity(0);
            Validity elementV = column.getLayerValidity(1);
            int[] sectionOffs = column.getLayerOffsets(2);
            int[] values = column.getInts();

            List<List<List<Integer>>> actual = new ArrayList<>();
            for (int r = 0; r < column.getRecordCount(); r++) {
                if (chaptersV.isNull(r)) {
                    actual.add(null);
                    continue;
                }
                List<List<Integer>> chapters = new ArrayList<>();
                for (int c = chapterOffs[r]; c < chapterOffs[r + 1]; c++) {
                    if (elementV.isNull(c)) {
                        chapters.add(null);
                        continue;
                    }
                    List<Integer> sections = new ArrayList<>();
                    for (int e = sectionOffs[c]; e < sectionOffs[c + 1]; e++) {
                        sections.add(values[e]);
                    }
                    chapters.add(sections);
                }
                actual.add(chapters);
            }
            assertThat(actual).containsExactly(
                    Arrays.asList(List.of(10, 20), null), List.of(), null);
        }
    }

    @Test
    void writesAndReadsBackOptionalStructEnclosingOptionalMap() throws Exception {
        // optional group s { optional map props (MAP) { repeated group key_value { required int32 key;
        //   optional int32 value } } }
        // record 0: s null; 1: s present, props null; 2: props empty; 3: props = {1:null}; 4: props = {2:99}.
        FileSchema schema = FileSchema.builder("schema")
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                                v -> v.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        Validity sNulls = Validity.ofNulls(new boolean[] { true, false, false, false, false });
        int[] offsets = { 0, 0, 0, 0, 1, 2 };
        Validity propsNulls = Validity.ofNulls(new boolean[] { true, true, false, false, false });
        int[] keys = { 1, 2 };
        int[] values = { 0, 99 };
        boolean[] valueNulls = { true, false };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .struct("s", sNulls)
                    .map("s.props", offsets, propsNulls)
                    .ints("s.props.key_value.key", keys)
                    .ints("s.props.key_value.value", values, valueNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
                ColumnReader keyCol = reader.columnReader(
                        reader.getFileSchema().getColumn("s.props.key_value.key").columnIndex());
                ColumnReader valCol = reader.columnReader(
                        reader.getFileSchema().getColumn("s.props.key_value.value").columnIndex())) {
            assertThat(reader.getFileSchema().getColumn("s.props.key_value.value").maxDefinitionLevel()).isEqualTo(4);
            assertThat(keyCol.nextBatch()).isTrue();
            assertThat(valCol.nextBatch()).isTrue();
            assertThat(valCol.getLayerCount()).isEqualTo(2);
            assertThat(valCol.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(valCol.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);

            Validity structV = valCol.getLayerValidity(0);
            Validity mapV = valCol.getLayerValidity(1);
            int[] mapOffsets = valCol.getLayerOffsets(1);
            Validity valueLeafV = valCol.getLeafValidity();
            int[] ks = keyCol.getInts();
            int[] vs = valCol.getInts();

            List<Map<Integer, Integer>> actual = new ArrayList<>();
            for (int r = 0; r < valCol.getRecordCount(); r++) {
                if (structV.isNull(r) || mapV.isNull(r)) {
                    actual.add(null);
                    continue;
                }
                Map<Integer, Integer> map = new LinkedHashMap<>();
                for (int e = mapOffsets[r]; e < mapOffsets[r + 1]; e++) {
                    map.put(ks[e], valueLeafV.isNull(e) ? null : vs[e]);
                }
                actual.add(map);
            }
            Map<Integer, Integer> rec3 = new LinkedHashMap<>();
            rec3.put(1, null);
            assertThat(actual).containsExactly(null, null, Map.of(), rec3, Map.of(2, 99));
        }
    }

    @Test
    void writesAndReadsBackMapOfIntToStruct() throws Exception {
        // optional map<int32, optional struct { required int32 a, optional int32 b }> — a STRUCT
        // value layer nested inside the MAP's REPEATED layer. A null struct value is distinct
        // from an absent map, an empty map, and a null leaf inside a present struct.
        FileSchema schema = FileSchema.builder("schema")
                .map("props", RepetitionType.OPTIONAL, PhysicalType.INT32,
                        v -> v.struct(RepetitionType.OPTIONAL, s -> s
                                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                                .addColumn("b", PhysicalType.INT32, RepetitionType.OPTIONAL)))
                .build();

        // record 0: {1:{a:10,b:20}, 2:null}; 1: {} (empty); 2: null (absent map); 3: {3:{a:30,b:null}}.
        int[] offsets = { 0, 2, 2, 2, 3 };
        Validity mapNulls = Validity.ofNulls(new boolean[] { false, false, true, false });
        int[] keys = { 1, 2, 3 };
        Validity structNulls = Validity.ofNulls(new boolean[] { false, true, false });
        int[] a = { 10, 0, 30 }; // the null-struct slot (entry 1) is a phantom, its value ignored
        int[] b = { 20, 0, 0 };
        boolean[] bNulls = { false, false, true };

        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch
                    .map("props", offsets, mapNulls)
                    .ints("props.key_value.key", keys)
                    .struct("props.key_value.value", structNulls)
                    .ints("props.key_value.value.a", a)
                    .ints("props.key_value.value.b", b, bNulls));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int keyIdx = reader.getFileSchema().getColumn("props.key_value.key").columnIndex();
            int aIdx = reader.getFileSchema().getColumn("props.key_value.value.a").columnIndex();
            int bIdx = reader.getFileSchema().getColumn("props.key_value.value.b").columnIndex();
            try (ColumnReader kr = reader.columnReader(keyIdx);
                    ColumnReader ar = reader.columnReader(aIdx);
                    ColumnReader br = reader.columnReader(bIdx)) {
                assertThat(kr.nextBatch()).isTrue();
                assertThat(ar.nextBatch()).isTrue();
                assertThat(br.nextBatch()).isTrue();
                assertThat(ar.getLayerCount()).isEqualTo(2);
                assertThat(ar.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
                assertThat(ar.getLayerKind(1)).isEqualTo(LayerKind.STRUCT);

                // Reconstruct each entry's struct as [a, b] (b nullable); a null struct is a null
                // map value, an absent map a null record.
                int[] mapOffs = ar.getLayerOffsets(0);
                Validity mapV = ar.getLayerValidity(0);
                Validity structV = ar.getLayerValidity(1);
                int[] ks = kr.getInts();
                int[] as = ar.getInts();
                int[] bs = br.getInts();
                Validity bLeaf = br.getLeafValidity();
                List<Map<Integer, List<Integer>>> actual = new ArrayList<>();
                for (int r = 0; r < ar.getRecordCount(); r++) {
                    if (mapV.isNull(r)) {
                        actual.add(null);
                        continue;
                    }
                    Map<Integer, List<Integer>> map = new LinkedHashMap<>();
                    for (int e = mapOffs[r]; e < mapOffs[r + 1]; e++) {
                        map.put(ks[e], structV.isNull(e)
                                ? null
                                : Arrays.asList(as[e], bLeaf.isNull(e) ? null : bs[e]));
                    }
                    actual.add(map);
                }

                Map<Integer, List<Integer>> rec0 = new LinkedHashMap<>();
                rec0.put(1, List.of(10, 20));
                rec0.put(2, null);
                Map<Integer, List<Integer>> rec3 = new LinkedHashMap<>();
                rec3.put(3, Arrays.asList(30, null));
                assertThat(actual).containsExactly(rec0, Map.of(), null, rec3);
            }
        }
    }
}
