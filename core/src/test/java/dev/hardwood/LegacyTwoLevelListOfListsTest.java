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

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// Legacy two-level `LIST` encoding whose element is itself a list, i.e. a
/// repeated group with a single field that is *itself* repeated. The Parquet
/// [backward-compatibility rules](https://parquet.apache.org/docs/file-format/types/logicaltypes/#backward-compatibility-rules)
/// say the repeated group is the element (it is a genuine element, not a
/// synthetic single-field wrapper to be unwrapped), so
/// `mylist (LIST) { repeated group bag { repeated int32 num } }` reads as
/// `list<struct{num: list<int>}>`.
///
/// The fixture shares its on-disk leaf data with a plain `list<list<int>>`
/// (`list_of_lists_modern_test`); the two differ only in schema interpretation.
class LegacyTwoLevelListOfListsTest {

    private static final Path LEGACY =
            Paths.get("src/test/resources/list_of_lists_legacy_two_level_test.parquet");
    private static final Path MODERN =
            Paths.get("src/test/resources/list_of_lists_modern_test.parquet");

    @Test
    void getListElementResolvesTheRepeatedGroupNotItsChild() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LEGACY))) {
            SchemaNode.GroupNode mylist =
                    (SchemaNode.GroupNode) reader.getFileSchema().getRootNode().children().get(0);
            assertThat(mylist.isList()).isTrue();

            // Rule 3: the repeated group 'bag' is the element, not its repeated child 'num'.
            SchemaNode element = mylist.getListElement();
            assertThat(element).isInstanceOf(SchemaNode.GroupNode.class);
            assertThat(element.name()).isEqualTo("bag");
        }
    }

    @Test
    void legacyTwoLevelReadsAsListOfStructsEachHoldingAList() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(LEGACY));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            PqList mylist = rowReader.getList("mylist");
            assertThat(mylist).isNotNull();
            assertThat(mylist.size()).isEqualTo(2);

            PqStruct bag0 = mylist.structs().get(0);
            assertThat(bag0.getList("num").ints().get(0)).isEqualTo(1);
            assertThat(bag0.getList("num").ints().get(1)).isEqualTo(2);

            PqStruct bag1 = mylist.structs().get(1);
            assertThat(bag1.getList("num").ints().get(0)).isEqualTo(3);

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void modernListOfListsReadsAsNestedLists() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(MODERN));
                RowReader rowReader = fileReader.rowReader()) {

            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            PqList mylist = rowReader.getList("mylist");
            assertThat(mylist.size()).isEqualTo(2);
            assertThat(mylist.lists().get(0).ints().get(0)).isEqualTo(1);
            assertThat(mylist.lists().get(0).ints().get(1)).isEqualTo(2);
            assertThat(mylist.lists().get(1).ints().get(0)).isEqualTo(3);
        }
    }
}
