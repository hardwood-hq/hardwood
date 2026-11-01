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

import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.reader.row.FlatRowReader;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

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
/// pure-flat schemas but diverge for a top-level primitive after a struct. `point`
/// repeats nothing, so the file is read by `FlatRowReader`, which addresses the
/// fields and the batch matchers the columns. This test pins filtering on such a
/// primitive through both filter paths, so that a reader that confuses the two
/// spaces fails loudly here instead of returning wrong rows.
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
    /// Both leaves compile to batch matchers, which address the decoded columns.
    /// `tag` is "keep" for every row, so the result is exactly `score > 55` →
    /// ids 2, 3, 4. A reader that read `score` at leaf index 4 instead of field
    /// index 3 would throw or return the wrong rows.
    @Test
    void filterOnPrimitiveAfterStruct() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.eq("tag", "keep"),
                FilterPredicate.gt("score", 55));

        assertThat(matchedIds(filter)).containsExactly(2, 3, 4);
    }

    /// The same rows through the record-side path (`RecordFilterCompiler` → the
    /// row reader's record matcher): `score` and `tag` each appear in both arms of
    /// the `OR`, which no batch matcher can hold, and the second arm matches nothing.
    @Test
    void filterOnPrimitiveAfterStructOnTheRecordPath() throws Exception {
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.and(FilterPredicate.eq("tag", "keep"), FilterPredicate.gt("score", 55)),
                FilterPredicate.and(FilterPredicate.eq("tag", "never"), FilterPredicate.lt("score", 0)));
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());
            assertThat(BatchFilterCompiler.tryCompile(FilterPredicateResolver.resolve(filter, schema), schema,
                    projected::toProjectedIndex))
                    .as("filter %s must take the record path", filter)
                    .isNull();
        }

        assertThat(matchedIds(filter)).containsExactly(2, 3, 4);
    }

    private static List<Integer> matchedIds(FilterPredicate filter) throws Exception {
        List<Integer> matchedIds = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            while (rows.hasNext()) {
                rows.next();
                matchedIds.add(rows.getInt("id"));
            }
        }
        return matchedIds;
    }
}
