/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.internal.predicate.CapturedWarnings;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A repeated group outside a `LIST` or `MAP` group is a list of its own elements. An
/// annotation it carries there is dropped with a warning, and the group reads exactly as the
/// unannotated one; a `VARIANT` annotation, which makes it a list of variants, is kept, and a
/// read of the group is not supported.
///
/// `annotated_repeated_group_test.parquet` holds the bare repeated group `foo` unannotated and
/// the same group annotated `MAP_KEY_VALUE` (`foo_mkv`), `LIST` (`foo_list`; `foo_list_lt` the
/// logical type only) and `MAP` (`foo_map`; `foo_map_ct` the converted type only), with rows
/// `[{a:1,b:x},{a:2,b:y}]`, `[]` and `[{a:3,b:null}]`, and `s.bar` annotated `LIST` below an
/// optional struct, with rows `{bar:[{a:4}]}`, `null` and `{bar:[]}`.
class AnnotatedBareRepeatedGroupTest {

    private static final Path ANNOTATED = Path.of("src/test/resources/annotated_repeated_group_test.parquet");
    private static final Path LEGACY_MAP = Path.of("src/test/resources/repeated_legacy_map_test.parquet");
    private static final Path VARIANT = Path.of("src/test/resources/repeated_variant_group_test.parquet");
    private static final Path VARIANT_PLAIN = Path.of("src/test/resources/repeated_variant_group_plain_test.parquet");
    private static final Path ANNOTATED_ELEMENT = Path.of("src/test/resources/annotated_list_element_group_test.parquet");
    private static final String VARIANT_MESSAGE = "[repeated_variant_group_test.parquet] Repeated group 'foo' is"
            + " annotated VARIANT(1) outside a LIST or MAP group; a list of variants in this form is not supported";

    @RegisterExtension
    final CapturedWarnings warnings = new CapturedWarnings();

    @ParameterizedTest
    @ValueSource(strings = { "foo", "foo_mkv", "foo_list", "foo_list_lt", "foo_map", "foo_map_ct" })
    void rowReaderReadsTheGroupAsAListOfStructs(String name) throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(ANNOTATED));
                RowReader rows = fileReader.rowReader()) {
            assertThat(fileReader.getFileSchema().getField(name))
                    .isInstanceOfSatisfying(SchemaNode.GroupNode.class, group -> assertThat(group.isStruct()).isTrue());

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            List<PqStruct> first = rows.getList(name).structs();
            assertThat(first).hasSize(2);
            assertThat(first.get(0).getInt("a")).isEqualTo(1);
            assertThat(first.get(0).getString("b")).isEqualTo("x");
            assertThat(first.get(1).getInt("a")).isEqualTo(2);
            assertThat(first.get(1).getString("b")).isEqualTo("y");

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getList(name).isEmpty()).isTrue();

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            List<PqStruct> third = rows.getList(name).structs();
            assertThat(third).hasSize(1);
            assertThat(third.get(0).getInt("a")).isEqualTo(3);
            assertThat(third.get(0).isNull("b")).isTrue();

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo", "foo_mkv", "foo_list", "foo_list_lt", "foo_map", "foo_map_ct" })
    void columnReaderExposesOneRepeatedLayer(String name) throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(ANNOTATED));
                ColumnReader column = fileReader.columnReader(name + ".a")) {
            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getRecordCount()).isEqualTo(3);
            assertThat(column.getLayerCount()).isEqualTo(1);
            assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(Arrays.copyOf(column.getLayerOffsets(0), 4)).containsExactly(0, 2, 2, 3);
            assertThat(column.getValueCount()).isEqualTo(3);
            assertThat(Arrays.copyOf(column.getInts(), 3)).containsExactly(1, 2, 3);
            assertThat(column.nextBatch()).isFalse();
        }
    }

    @Test
    void nestedGroupReadsAsAListOfStructs() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(ANNOTATED));
                RowReader rows = fileReader.rowReader();
                ColumnReader column = fileReader.columnReader("s.bar.a")) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            List<PqStruct> bar = rows.getStruct("s").getList("bar").structs();
            assertThat(bar).hasSize(1);
            assertThat(bar.get(0).getInt("a")).isEqualTo(4);
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getStruct("s")).isNull();
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getStruct("s").getList("bar").isEmpty()).isTrue();
            assertThat(rows.hasNext()).isFalse();

            assertThat(column.nextBatch()).isTrue();
            assertThat(column.getLayerCount()).isEqualTo(2);
            assertThat(column.getLayerKind(0)).isEqualTo(LayerKind.STRUCT);
            assertThat(column.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);
            assertThat(column.getLayerValidity(0).isNull(1)).isTrue();
            assertThat(Arrays.copyOf(column.getLayerOffsets(1), 4)).containsExactly(0, 1, 1, 1);
            assertThat(column.getValueCount()).isEqualTo(1);
            assertThat(column.getInts()[0]).isEqualTo(4);
        }
    }

    @Test
    void droppedAnnotationsAreReportedOncePerFile() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(ANNOTATED))) {
            assertThat(fileReader.getFileSchema().getColumnCount()).isEqualTo(13);
        }
        assertThat(warnings.messages()).containsExactly(
                "Ignoring 6 annotation(s) on repeated groups outside a LIST or MAP group; those groups are read"
                        + " as though unannotated: foo_mkv (MAP_KEY_VALUE); foo_list (LIST); foo_list_lt (LIST);"
                        + " foo_map (MAP); foo_map_ct (MAP); s.bar (LIST)");
    }

    /// `attrs` is an unannotated bare repeated group whose only child is a `MAP_KEY_VALUE` group,
    /// so it is a list whose element is a legacy map: `[{a:1,b:2},{c:3}]` and `[]`. `attrs_map`
    /// is the same group annotated `MAP`; the annotation is dropped, and the `key_value` child
    /// makes the unannotated group a map again.
    @ParameterizedTest
    @ValueSource(strings = { "attrs", "attrs_map" })
    void repeatedLegacyMapReadsAsAListOfMaps(String name) throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(LEGACY_MAP));
                RowReader rows = fileReader.rowReader();
                ColumnReader keys = fileReader.columnReader(name + ".key_value.key");
                ColumnReader values = fileReader.columnReader(name + ".key_value.value")) {
            assertThat(fileReader.getFileSchema().getField(name))
                    .isInstanceOfSatisfying(SchemaNode.GroupNode.class, group -> assertThat(group.isMap()).isTrue());

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            List<PqMap> maps = rows.getList(name).maps();
            assertThat(maps).hasSize(2);
            assertThat(maps.get(0).size()).isEqualTo(2);
            assertThat(maps.get(0).getValue("a")).isEqualTo(1);
            assertThat(maps.get(0).getValue("b")).isEqualTo(2);
            assertThat(maps.get(1).size()).isEqualTo(1);
            assertThat(maps.get(1).getValue("c")).isEqualTo(3);

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(2);
            assertThat(rows.getList(name).isEmpty()).isTrue();
            assertThat(rows.hasNext()).isFalse();

            assertThat(keys.nextBatch()).isTrue();
            assertThat(keys.getLayerCount()).isEqualTo(2);
            assertThat(keys.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            assertThat(keys.getLayerKind(1)).isEqualTo(LayerKind.REPEATED);
            assertThat(Arrays.copyOf(keys.getLayerOffsets(0), 3)).containsExactly(0, 2, 2);
            assertThat(Arrays.copyOf(keys.getLayerOffsets(1), 3)).containsExactly(0, 2, 3);
            assertThat(keys.getStrings()).containsExactly("a", "b", "c");

            assertThat(values.nextBatch()).isTrue();
            assertThat(Arrays.copyOf(values.getInts(), 3)).containsExactly(1, 2, 3);
        }
        assertThat(warnings.messages()).containsExactly(
                "Ignoring 1 annotation(s) on repeated groups outside a LIST or MAP group; those groups are read"
                        + " as though unannotated: attrs_map (MAP)");
    }

    /// Every file of a multi-file read has its annotations dropped, the first when the reader
    /// opens and each later one when its footer is loaded, and each file warns once.
    @Test
    void laterFileHasItsAnnotationsDroppedToo() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.openAll(
                List.of(InputFile.of(ANNOTATED), InputFile.of(ANNOTATED)));
                RowReader rows = fileReader.rowReader()) {
            int rowCount = 0;
            while (rows.hasNext()) {
                rows.next();
                rowCount++;
                if (rowCount == 4) {
                    List<PqStruct> first = rows.getList("foo_mkv").structs();
                    assertThat(first).hasSize(2);
                    assertThat(first.get(1).getInt("a")).isEqualTo(2);
                    assertThat(first.get(1).getString("b")).isEqualTo("y");
                }
            }
            assertThat(rowCount).isEqualTo(6);
        }
        String warning = "Ignoring 6 annotation(s) on repeated groups outside a LIST or MAP group; those groups are"
                + " read as though unannotated: foo_mkv (MAP_KEY_VALUE); foo_list (LIST); foo_list_lt (LIST);"
                + " foo_map (MAP); foo_map_ct (MAP); s.bar (LIST)";
        assertThat(warnings.messages()).containsExactly(warning, warning);
    }

    @Test
    void repeatedVariantGroupOpensAndReportsItsAnnotation() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VARIANT))) {
            assertThat(fileReader.getFileSchema().getField("foo"))
                    .isInstanceOfSatisfying(SchemaNode.GroupNode.class, group -> {
                        assertThat(group.isVariant()).isTrue();
                        assertThat(group.repetitionType()).isEqualTo(RepetitionType.REPEATED);
                    });
        }
        assertThat(warnings.messages()).isEmpty();
    }

    @Test
    void readsNotTouchingTheRepeatedVariantGroupSucceed() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VARIANT));
                RowReader rows = fileReader.buildRowReader()
                        .projection(ColumnProjection.columns("id"))
                        .filter(FilterPredicate.eq("id", 1))
                        .build();
                ColumnReader id = fileReader.columnReader("id")) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            assertThat(rows.hasNext()).isFalse();

            assertThat(id.nextBatch()).isTrue();
            assertThat(id.getValueCount()).isEqualTo(1);
            assertThat(id.getInts()[0]).isEqualTo(1);
        }
    }

    @Test
    void readsTouchingTheRepeatedVariantGroupAreUnsupported() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VARIANT))) {
            List<ThrowingCallable> reads = List.of(
                    fileReader::rowReader,
                    () -> fileReader.buildRowReader().filter(FilterPredicate.eq("id", 1)).build(),
                    () -> fileReader.columnReader("foo.value"),
                    () -> fileReader.columnReaders(ColumnProjection.columns("id", "foo.metadata")));
            for (ThrowingCallable read : reads) {
                assertThatThrownBy(read)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessage(VARIANT_MESSAGE);
            }
            // The refused reads leave the file reader usable.
            try (RowReader rows = fileReader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
                assertThat(rows.hasNext()).isTrue();
            }
        }
    }

    /// A filter on the group or one of its leaves is refused while the filter is resolved, before
    /// any column is planned, by the rules for any column below a repeated path or a `VARIANT`
    /// group: the caller asked a question with no single answer per row.
    @Test
    void filtersOnTheRepeatedVariantGroupAreRejected() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(VARIANT))) {
            assertThatThrownBy(() -> fileReader.buildRowReader()
                    .projection(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.eq("foo.value", new byte[]{ 0x0c, 0x2a }))
                    .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column 'foo.value' is a leaf of the VARIANT group 'foo', which holds an encoded"
                            + " variant; it takes isNull and isNotNull predicates only");
            assertThatThrownBy(() -> fileReader.buildRowReader()
                    .projection(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.isNull("foo.metadata"))
                    .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Filter predicates do not support repeated columns. Column 'foo.metadata' is repeated.");
            assertThatThrownBy(() -> fileReader.buildColumnReader("id")
                    .filter(FilterPredicate.isNotNull("foo"))
                    .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Filter predicates do not support repeated columns. Column 'foo' is repeated.");
        }
    }

    /// The file holding the repeated `VARIANT` group is the second of the read, after one whose
    /// `foo` is an unannotated list of structs with the same leaves.
    @Test
    void laterFileTouchingTheRepeatedVariantGroupIsUnsupported() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.openAll(
                List.of(InputFile.of(VARIANT_PLAIN), InputFile.of(VARIANT)));
                RowReader rows = fileReader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getList("foo").structs()).hasSize(2);
            assertThatThrownBy(() -> {
                while (rows.hasNext()) {
                    rows.next();
                }
            })
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage(VARIANT_MESSAGE);
        }
        try (ParquetFileReader fileReader = ParquetFileReader.openAll(
                List.of(InputFile.of(VARIANT_PLAIN), InputFile.of(VARIANT)));
                RowReader rows = fileReader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
            int rowCount = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getInt("id")).isEqualTo(1);
                rowCount++;
            }
            assertThat(rowCount).isEqualTo(2);
        }
    }

    /// A `LIST` whose element group carries `MAP_KEY_VALUE`, an annotation a list element cannot
    /// use, reads its elements as structs.
    @Test
    void listElementGroupWithAnUnusableAnnotationReadsAsAStruct() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(ANNOTATED_ELEMENT));
                RowReader rows = fileReader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            PqList first = rows.getList("l");
            assertThat(first.get(0)).isInstanceOf(PqStruct.class);
            List<PqStruct> structs = first.structs();
            assertThat(structs).hasSize(2);
            assertThat(structs.get(0).getInt("a")).isEqualTo(1);
            assertThat(structs.get(0).getString("b")).isEqualTo("x");
            assertThat(structs.get(1).getInt("a")).isEqualTo(2);
            assertThat(structs.get(1).getString("b")).isEqualTo("y");

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getList("l")).isNull();

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            List<PqStruct> third = rows.getList("l").structs();
            assertThat(third).hasSize(1);
            assertThat(third.get(0).getInt("a")).isEqualTo(3);
            assertThat(third.get(0).isNull("b")).isTrue();
            assertThat(rows.hasNext()).isFalse();
        }
    }
}
