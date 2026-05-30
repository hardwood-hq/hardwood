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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Guard test for #525.
///
/// Fixture schema: `id INT, point STRUCT{x, y}, tag STRING, score INT`.
///
///   leaf-column index:     id=0, point.x=1, point.y=2, tag=3, score=4
///   top-level field index: id=0, point=1,             tag=2, score=3
///
/// `score` is a top-level primitive whose FIELD index (3) differs from its
/// LEAF-column index (4), because the `point` struct contributes two extra
/// leaf columns in front of it.
///
/// A reader that addresses columns by index has two index spaces: the public
/// accessors (`getInt(int)`, `isNull(int)`) take a top-level FIELD index, while
/// value arrays are addressed by projected LEAF-column index. They coincide for
/// pure-flat schemas but diverge for a top-level primitive after a struct. This
/// test pins the behaviour of filtering on such a primitive through the
/// record-side path, so that if struct-of-primitives reading is ever moved to a
/// reader that confuses the two spaces, it fails loudly here instead of
/// returning wrong rows.
class StructThenPrimitiveFilterTest {

    private static final Path FILE =
            Paths.get("src/test/resources/struct_then_primitive_test.parquet");

    /// Sanity: unfiltered read of the struct-bearing schema returns every row
    /// and navigates the struct correctly.
    @Test
    void readsAllRowsWithoutFilter() throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.buildRowReader().build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getInt("id"));
                // point.y == id*10 + 1 — proves struct navigation is correct
                assertThat(rows.getStruct("point").getInt("y"))
                        .isEqualTo(rows.getInt("id") * 10 + 1);
            }
        }
        assertThat(ids).containsExactly(1, 2, 3, 4);
    }

    /// The guard: filter on the top-level primitive `score`, which sits after
    /// the `point` struct (so its field index 3 differs from leaf index 4).
    ///
    /// The predicate must be evaluated on the record-side path
    /// (`RecordFilterCompiler` → `FilteredRowReader`), where columns are
    /// addressed through the reader's indexed accessors. To force that path
    /// reliably we AND in a predicate on the `tag` string column: a `BYTE_ARRAY`
    /// predicate is not drain-side eligible (`BatchFilterCompiler` returns null
    /// for it), and one non-eligible leaf makes the whole query fall back. (An
    /// `OR` is NOT sufficient — `OR` of column-local leaves is drain-side
    /// eligible on current `main`.)
    ///
    /// `tag` is "keep" for every row, so the result is exactly `score > 55` →
    /// ids 2, 3, 4. A reader that read `score` at leaf index 4 instead of field
    /// index 3 would throw or return the wrong rows.
    @Test
    void filterOnPrimitiveAfterStruct() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.eq("tag", "keep"),
                FilterPredicate.gt("score", 55));

        List<Integer> matchedIds = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                matchedIds.add(rows.getInt("id"));
            }
        }

        assertThat(matchedIds).containsExactly(2, 3, 4);
    }
}
