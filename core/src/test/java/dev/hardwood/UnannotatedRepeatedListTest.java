/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
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
