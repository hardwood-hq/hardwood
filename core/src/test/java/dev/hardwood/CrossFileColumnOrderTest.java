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
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests that a multi-file reader resolves each projected column against the
/// leaf ordering of the file it is reading, rather than reusing the first
/// file's leaf ordinals (issue #903).
///
/// Every fixture pair here passes the physical/logical/repetition-type checks
/// in `RowGroupIterator.validateSchemaCompatibility`, because the columns
/// differ only in where they sit in the flattened leaf list — the case that
/// path-based validation cannot see but ordinal-based reading depends on.
class CrossFileColumnOrderTest {

    private static final Path ORDER_AB = Paths.get("src/test/resources/compat_order_ab.parquet");
    private static final Path ORDER_BA = Paths.get("src/test/resources/compat_order_ba.parquet");
    private static final Path ORDER_AB_MIXED = Paths.get("src/test/resources/compat_order_ab_mixed.parquet");
    private static final Path ORDER_BA_MIXED = Paths.get("src/test/resources/compat_order_ba_mixed.parquet");
    private static final Path ORDER_X_AB = Paths.get("src/test/resources/compat_order_x_ab.parquet");
    private static final Path ORDER_B_ONLY = Paths.get("src/test/resources/compat_order_b_only.parquet");
    private static final Path ORDER_AB_DICT = Paths.get("src/test/resources/compat_order_ab_dict.parquet");
    private static final Path ORDER_BA_DICT = Paths.get("src/test/resources/compat_order_ba_dict.parquet");
    private static final Path ORDER_NESTED_AB = Paths.get("src/test/resources/compat_order_nested_ab.parquet");
    private static final Path ORDER_NESTED_BA = Paths.get("src/test/resources/compat_order_nested_ba.parquet");
    private static final Path CHUNK_PATH_SWAPPED =
            Paths.get("src/test/resources/compat_chunk_path_swapped.parquet");

    /// `compat_order_ab` has leaves (a, b); `compat_order_ba` has (b, a) with
    /// identical types. Reading by first-file ordinal decodes the second file's
    /// `b` chunk into the `a` slot and vice versa — a correct decode filed under
    /// the wrong name, so nothing downstream can detect it.
    @Test
    void readsReorderedLeavesByPathNotOrdinal() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB, ORDER_BA));
             RowReader reader = parquet.rowReader()) {

            List<String> rows = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                rows.add("a=" + reader.getLong("a") + " b=" + reader.getLong("b"));
            }

            assertThat(rows).containsExactly(
                    "a=1 b=101",
                    "a=2 b=102",
                    "a=3 b=103",
                    "a=4 b=104",
                    "a=5 b=105",
                    "a=6 b=106");
        }
    }

    /// Same reordering with differing physical types. Reading by first-file
    /// ordinal hands the second file's DOUBLE chunk to a worker built for
    /// INT64, which surfaces as a `ClassCastException` from a decode thread
    /// after the first file's rows have already been emitted.
    @Test
    void readsReorderedLeavesOfDifferentPhysicalTypes() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB_MIXED, ORDER_BA_MIXED));
             RowReader reader = parquet.rowReader()) {

            List<String> rows = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                rows.add("a=" + reader.getLong("a") + " b=" + reader.getDouble("b"));
            }

            assertThat(rows).containsExactly(
                    "a=1 b=1.5",
                    "a=2 b=2.5",
                    "a=3 b=3.5",
                    "a=4 b=4.5",
                    "a=5 b=5.5",
                    "a=6 b=6.5");
        }
    }

    /// A column the projection never asks for, inserted ahead of one it does,
    /// shifts every later leaf by one ordinal. Non-projected columns are
    /// documented as unconstrained, so this pair must read correctly.
    @Test
    void readsProjectedColumnPastAnExtraLeadingColumn() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB, ORDER_X_AB));
             RowReader reader = parquet.buildRowReader()
                     .projection(ColumnProjection.columns("a"))
                     .build()) {

            List<Long> values = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                values.add(reader.getLong("a"));
            }

            assertThat(values).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        }
    }

    /// Dropping a leading column the projection does not ask for leaves the
    /// projected column at a lower ordinal than the first file's. Non-projected
    /// columns are documented as unconstrained, so this pair must read
    /// correctly rather than failing on the first file's out-of-range ordinal.
    @Test
    void readsWhenFileOmitsANonProjectedLeadingColumn() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB, ORDER_B_ONLY));
             RowReader reader = parquet.buildRowReader()
                     .projection(ColumnProjection.columns("b"))
                     .build()) {

            List<Long> values = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                values.add(reader.getLong("b"));
            }

            assertThat(values).containsExactly(101L, 102L, 103L, 104L, 105L, 106L);
        }
    }

    /// A *projected* column absent from a later file is a genuine
    /// incompatibility, and must be reported by name with the file identified.
    @Test
    void rejectsFileMissingAProjectedColumn() {
        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB, ORDER_B_ONLY));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_order_b_only.parquet] column a — Not present in this file's schema");
        }
    }

    /// Row-group pruning resolves predicate column indices once against the
    /// first file, then applies them to every file's row groups. With reordered
    /// leaves the second file is pruned against the wrong column's statistics:
    /// `a < 10` is tested against `b`'s min/max of 104..106 and drops the whole
    /// row group.
    @Test
    void prunesRowGroupsAgainstTheCorrectColumnStatistics() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB, ORDER_BA));
             RowReader reader = parquet.buildRowReader()
                     .filter(FilterPredicate.lt("a", 10L))
                     .build()) {

            List<Long> values = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                values.add(reader.getLong("a"));
            }

            assertThat(values).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        }
    }

    /// A predicate on a column that is not part of the projection still has to
    /// be evaluated against the right leaf in each file.
    @Test
    void prunesOnANonProjectedColumnAgainstTheCorrectStatistics() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB, ORDER_BA));
             RowReader reader = parquet.buildRowReader()
                     .projection(ColumnProjection.columns("a"))
                     .filter(FilterPredicate.lt("b", 1000L))
                     .build()) {

            List<Long> values = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                values.add(reader.getLong("a"));
            }

            assertThat(values).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        }
    }

    /// The `_dict` pair carries dictionary pages, bloom filters and a page index,
    /// so the dictionary, bloom and page-index pruning sources all address the
    /// file's chunks by ordinal — none of which the plain fixtures exercise. The
    /// two files' `a` statistics overlap (1..7 and 2..8), so `a = 6` can only be
    /// decided per file by the dictionary or bloom filter, and reading `b`'s in
    /// either file's place answers "absent" and yields nothing at all.
    @Test
    void prunesReorderedLeavesByDictionaryBloomAndPageIndex() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(ORDER_AB_DICT, ORDER_BA_DICT));
             RowReader reader = parquet.buildRowReader()
                     .filter(FilterPredicate.eq("a", 6L))
                     .build()) {

            List<Long> values = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                values.add(reader.getLong("a"));
            }

            assertThat(values).containsExactly(6L);
        }
    }

    /// Reordered leaves that are repeated and more than one path element deep, so
    /// the leaf ordinal is resolved for the per-page mask gate as well (it
    /// short-circuits on `maxRepetitionLevel == 0`, which every flat fixture hits).
    @Test
    void readsReorderedNestedLeaves() throws Exception {
        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader parquet = hardwood.openAll(
                     InputFile.ofPaths(ORDER_NESTED_AB, ORDER_NESTED_BA));
             RowReader reader = parquet.rowReader()) {

            List<String> rows = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                rows.add("l1=" + reader.getList("l1").values()
                        + " l2=" + reader.getList("l2").values());
            }

            assertThat(rows).containsExactly(
                    "l1=[1, 2] l2=[101]",
                    "l1=[3] l2=[102, 103]",
                    "l1=[4] l2=[104, 105]",
                    "l1=[5, 6] l2=[106]");
        }
    }

    /// A footer whose column chunks disagree with the schema about which leaf they
    /// hold is internally inconsistent: the two are independent statements of the
    /// same positional alignment. Either statement could be the wrong one, so the
    /// read refuses rather than picking one and decoding under the other's schema.
    @Test
    void rejectsFooterWhoseChunkPathsDisagreeWithTheSchema() {
        try (Hardwood hardwood = Hardwood.create()) {
            assertThatThrownBy(() -> {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(CHUNK_PATH_SWAPPED));
                     RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                    }
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[compat_chunk_path_swapped.parquet] row group 0, column a — The row group"
                            + " lists column 'b' at this position");
        }
    }
}
