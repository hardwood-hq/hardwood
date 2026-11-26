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
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// `isNull` / `isNotNull` on a `VARIANT` group and on the leaves below it, evaluated per record
/// with only `id` projected, so the predicate columns are decoded for the filter alone.
class VariantNullPredicateTest {

    /// ids 1..4; `var` unshredded and present in every row.
    private static final Path UNSHREDDED = Paths.get("src/test/resources/variant_test.parquet");
    /// ids 1..3; `var` unshredded and null in row 2.
    private static final Path NULL_VARIANT = Paths.get("src/test/resources/convert_variant_null_test.parquet");
    /// ids 1..4; `var` shredded and present in every row. `value` is set in row 2 only,
    /// `typed_value` in rows 1 and 4.
    private static final Path SHREDDED = Paths.get("src/test/resources/variant_shredded_test.parquet");
    /// ids 1..4; `s.v` shredded and null in row 4. `value` is set in row 2 only,
    /// `typed_value` in row 1 only.
    private static final Path SHREDDED_IN_STRUCT = Paths.get("src/test/resources/variant_shredded_in_struct_test.parquet");

    /// The filter decodes `var.value` alone, without `var.metadata`: the predicate reads the
    /// leaf's nulls and never assembles the variant.
    @Test
    void leafOfUnshreddedVariant() throws Exception {
        assertThat(ids(UNSHREDDED, FilterPredicate.isNotNull("var.value"))).containsExactly(1, 2, 3, 4);
    }

    @Test
    void variantGroupWithNullRows() throws Exception {
        assertThat(ids(NULL_VARIANT, FilterPredicate.isNull("var"))).containsExactly(2);
        assertThat(ids(NULL_VARIANT, FilterPredicate.isNotNull("var"))).containsExactly(1, 3);
        assertThat(ids(NULL_VARIANT, FilterPredicate.isNotNull("var.metadata"))).containsExactly(1, 3);
        assertThat(ids(NULL_VARIANT, FilterPredicate.isNull("var.value"))).containsExactly(2);
    }

    @Test
    void leavesOfShreddedVariant() throws Exception {
        assertThat(ids(SHREDDED, FilterPredicate.isNotNull("var.value"))).containsExactly(2);
        assertThat(ids(SHREDDED, FilterPredicate.isNull("var.value"))).containsExactly(1, 3, 4);
        assertThat(ids(SHREDDED, FilterPredicate.isNotNull("var.typed_value"))).containsExactly(1, 4);
        assertThat(ids(SHREDDED, FilterPredicate.isNull("var.typed_value"))).containsExactly(2, 3);
    }

    @Test
    void shreddedVariantInStruct() throws Exception {
        assertThat(ids(SHREDDED_IN_STRUCT, FilterPredicate.isNull("s.v"))).containsExactly(4);
        assertThat(ids(SHREDDED_IN_STRUCT, FilterPredicate.isNull("s.v.metadata"))).containsExactly(4);
        assertThat(ids(SHREDDED_IN_STRUCT, FilterPredicate.isNotNull("s.v.value"))).containsExactly(2);
        assertThat(ids(SHREDDED_IN_STRUCT, FilterPredicate.isNull("s.v.typed_value"))).containsExactly(2, 3, 4);
    }

    private static List<Integer> ids(Path file, FilterPredicate filter) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(filter)
                     .build()) {
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getInt("id"));
            }
        }
        return ids;
    }
}
