/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;

/// Per the Parquet format spec, a repeated field that is neither contained by a
/// `LIST`/`MAP`-annotated group nor itself `LIST`/`MAP`-annotated is a required
/// list of required elements whose element type is the type of the field
/// (https://parquet.apache.org/docs/file-format/types/logicaltypes/#nested-types).
///
/// The primitive fixtures encode a single row holding the two integers `[42, 7]`
/// under a field named `foo`; one uses a standard `LIST`-annotated three-level
/// group, the other an unannotated `REPEATED INT32`. The group fixtures encode
/// `foo = [{a:1,b:"x"}, {a:2,b:"y"}]`, exercising the element-is-the-field rule
/// for a repeated *group*: an annotated `list<struct>` and the unannotated
/// `REPEATED group { a; b }`. The reader must surface all of them as a list.
/// The empty-list fixture holds bare repeated groups, at the top level and below
/// an optional struct, whose lists are empty in some rows.
class UnannotatedRepeatedListTest {

    @Test
    void unannotatedRepeatedPrimitiveIsReadAsList() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_primitive_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            PqList foo = rowReader.getList("foo");
            assertThat(foo).isNotNull();
            assertThat(foo.size()).isEqualTo(2);

            PqIntList ints = foo.ints();
            assertThat(ints.get(0)).isEqualTo(42);
            assertThat(ints.get(1)).isEqualTo(7);

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void annotatedListMatchesUnannotatedRepeated() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_annotated_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            PqIntList ints = rowReader.getList("foo").ints();
            assertThat(ints.size()).isEqualTo(2);
            assertThat(ints.get(0)).isEqualTo(42);
            assertThat(ints.get(1)).isEqualTo(7);
        }
    }

    /// The annotated `required group (LIST)` is a required list of required
    /// elements (`maxDef == 1`), so it engages the fixed-size-list fast path. The
    /// reconstructed row must still be `[42, 7]`, proving the fast path's
    /// `maxDef == 1` reconstruction end to end.
    @Test
    void annotatedRequiredListReadsThroughFixedSizeListFastPath() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_annotated_list_test.parquet");
        ReaderConfig fastPathOn =
                ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "true").build();

        try (HardwoodContext context = HardwoodContext.create();
                ParquetFileReader fileReader =
                        ParquetFileReader.open(InputFile.of(file), context, fastPathOn);
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            PqIntList ints = rowReader.getList("foo").ints();
            assertThat(ints.size()).isEqualTo(2);
            assertThat(ints.get(0)).isEqualTo(42);
            assertThat(ints.get(1)).isEqualTo(7);
        }
    }

    @Test
    void unannotatedRepeatedGroupIsReadAsListOfStructs() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_group_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            assertListOfTwoStructs(rowReader.getList("foo"));

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void annotatedListOfStructsMatchesUnannotatedRepeatedGroup() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_group_annotated_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            assertListOfTwoStructs(rowReader.getList("foo"));
        }
    }

    /// `foo` is a top-level bare repeated group with rows `[{a:1}]`, `[]` and
    /// `[{a:2},{a:3}]`. An empty list is encoded at the group's max definition
    /// level minus one and must surface as a present, empty list.
    @Test
    void columnReaderReadsEmptyUnannotatedRepeatedGroupAsEmptyList() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_group_empty_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader col = fileReader.columnReader("foo.a")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getRecordCount()).isEqualTo(3);
            assertThat(col.getLayerCount()).isEqualTo(1);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(col.getLayerOffsets(0)).containsExactly(0, 1, 1, 3);
            assertThat(col.getLayerValidity(0).hasNulls()).isFalse();
            assertThat(col.getValueCount()).isEqualTo(3);
            assertThat(Arrays.copyOf(col.getInts(), 3)).containsExactly(1, 2, 3);
            assertThat(col.nextBatch()).isFalse();
        }
    }

    /// `s.bar` is a bare repeated group below an optional struct, with rows
    /// `{bar:[{a:4}]}`, `null` and `{bar:[]}`. The null struct and the empty list
    /// sit at distinct definition levels and must stay distinguishable.
    @Test
    void columnReaderReadsEmptyNestedUnannotatedRepeatedGroupAsEmptyList() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_group_empty_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader col = fileReader.columnReader("s.bar.a")) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getRecordCount()).isEqualTo(3);
            assertThat(col.getLayerCount()).isEqualTo(2);
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(col.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);

            Validity structValidity = col.getLayerValidity(0);
            assertThat(structValidity.isNotNull(0)).isTrue();
            assertThat(structValidity.isNull(1)).isTrue();
            assertThat(structValidity.isNotNull(2)).isTrue();

            assertThat(col.getLayerOffsets(1)).containsExactly(0, 1, 1, 1);
            Validity listValidity = col.getLayerValidity(1);
            assertThat(listValidity.isNotNull(0)).isTrue();
            assertThat(listValidity.isNotNull(2)).isTrue();

            assertThat(col.getValueCount()).isEqualTo(1);
            assertThat(col.getInts()[0]).isEqualTo(4);
            assertThat(col.nextBatch()).isFalse();
        }
    }

    /// The annotated `list<struct>` form of the empty-list fixture reads its lists
    /// through the `LIST` branch of the layer thresholds, the bare repeated group
    /// through the unannotated one. Both must yield the same layers, offsets,
    /// validity and values for `foo.a` (leaf 0) and `s.bar.a` (leaf 1).
    @Test
    void annotatedEmptyListOfStructsMatchesUnannotatedRepeatedGroup() throws Exception {
        Path annotated = Paths.get("src/test/resources/unannotated_repeated_group_empty_annotated_list_test.parquet");
        Path unannotated = Paths.get("src/test/resources/unannotated_repeated_group_empty_test.parquet");

        try (ParquetFileReader annotatedReader = ParquetFileReader.open(InputFile.of(annotated));
                ParquetFileReader unannotatedReader = ParquetFileReader.open(InputFile.of(unannotated))) {
            for (int leaf = 0; leaf < 2; leaf++) {
                try (ColumnReader expected = annotatedReader.columnReader(leaf);
                        ColumnReader actual = unannotatedReader.columnReader(leaf)) {
                    assertSameBatch(expected, actual);
                }
            }
        }
    }

    @Test
    void rowReaderReadsEmptyUnannotatedRepeatedGroupAsEmptyList() throws Exception {
        Path file = Paths.get("src/test/resources/unannotated_repeated_group_empty_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getList("foo").size()).isEqualTo(1);
            assertThat(rowReader.getList("foo").structs().get(0).getInt("a")).isEqualTo(1);
            assertThat(rowReader.getStruct("s").getList("bar").structs().get(0).getInt("a")).isEqualTo(4);

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getList("foo").size()).isEqualTo(0);
            assertThat(rowReader.getStruct("s")).isNull();

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getList("foo").size()).isEqualTo(2);
            assertThat(rowReader.getStruct("s").getList("bar").size()).isEqualTo(0);

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    /// Asserts that `actual` reads one batch identical to `expected`'s: the same
    /// record count, layer kinds, per-layer offsets and validity, and leaf values.
    private static void assertSameBatch(ColumnReader expected, ColumnReader actual) throws IOException {
        assertThat(expected.nextBatch()).isTrue();
        assertThat(actual.nextBatch()).isTrue();
        assertThat(actual.getRecordCount()).isEqualTo(expected.getRecordCount());
        assertThat(actual.getLayerCount()).isEqualTo(expected.getLayerCount());

        int items = expected.getRecordCount();
        for (int layer = 0; layer < expected.getLayerCount(); layer++) {
            assertThat(actual.getLayerKind(layer)).as("kind of layer %d", layer)
                    .isEqualTo(expected.getLayerKind(layer));
            Validity expectedValidity = expected.getLayerValidity(layer);
            Validity actualValidity = actual.getLayerValidity(layer);
            for (int i = 0; i < items; i++) {
                assertThat(actualValidity.isNull(i)).as("null at layer %d, item %d", layer, i)
                        .isEqualTo(expectedValidity.isNull(i));
            }
            if (expected.getLayerKind(layer) == LayerKind.REPEATED) {
                int[] expectedOffsets = Arrays.copyOf(expected.getLayerOffsets(layer), items + 1);
                assertThat(Arrays.copyOf(actual.getLayerOffsets(layer), items + 1))
                        .as("offsets of layer %d", layer)
                        .containsExactly(expectedOffsets);
                items = expectedOffsets[items];
            }
        }

        assertThat(actual.getValueCount()).isEqualTo(expected.getValueCount());
        int values = expected.getValueCount();
        assertThat(Arrays.copyOf(actual.getInts(), values))
                .containsExactly(Arrays.copyOf(expected.getInts(), values));
        assertThat(expected.nextBatch()).isFalse();
        assertThat(actual.nextBatch()).isFalse();
    }

    private static void assertListOfTwoStructs(PqList foo) {
        assertThat(foo).isNotNull();
        assertThat(foo.size()).isEqualTo(2);

        PqStruct first = foo.structs().get(0);
        assertThat(first.getInt("a")).isEqualTo(1);
        assertThat(first.getString("b")).isEqualTo("x");

        PqStruct second = foo.structs().get(1);
        assertThat(second.getInt("a")).isEqualTo(2);
        assertThat(second.getString("b")).isEqualTo("y");
    }
}
