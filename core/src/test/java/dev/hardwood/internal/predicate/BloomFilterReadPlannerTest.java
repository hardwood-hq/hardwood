/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The bloom read planner's candidate sets, against `bloom_filter_test.parquet` (one row group;
/// columns `id` INT64(0), `value` INT64(1) without a filter, `name` STRING(2), `code` INT32(3),
/// `price` FLOAT(4), `ratio` DOUBLE(5), `dec`(6), `ts`(7), `sparse` INT64(8); bloom filters on
/// all but `value`) and synthetic metadata for the shapes the fixture cannot express.
///
/// The planner derives eligibility by running [RowGroupFilterEvaluator#decideRowGroup] with a
/// recording source, so these tests pin the mirror: candidate sets must equal the columns whose
/// bloom check the evaluator actually reaches, and must be a superset of the columns a real
/// (filter-returning) evaluation consults.
class BloomFilterReadPlannerTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/bloom_filter_test.parquet");

    // Column order in the schema / row group: id(0), value(1), name(2), code(3), price(4),
    // ratio(5), dec(6), ts(7), sparse(8).
    private static final int ID_COLUMN = 0;
    private static final int VALUE_COLUMN = 1;
    private static final int NAME_COLUMN = 2;
    private static final int CODE_COLUMN = 3;
    private static final int PRICE_COLUMN = 4;
    private static final int SPARSE_COLUMN = 8;

    private static ParquetFileReader reader;
    private static InputFile inputFile;
    private static RowGroup rowGroup;
    private static FileSchema schema;

    @BeforeAll
    static void open() throws Exception {
        inputFile = InputFile.of(FIXTURE);
        reader = ParquetFileReader.open(inputFile);
        rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        schema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema());
    }

    @AfterAll
    static void close() throws Exception {
        reader.close();
    }

    @Test
    void equalityLeafPlansItsColumn() {
        BloomFilterReadPlanner.BloomFilterReadPlan plan =
                plan(FilterPredicate.eq("code", 1));
        assertThat(plan.candidatesFor(0))
                .extracting(BloomFilterReadPlanner.BloomCandidate::columnIndex)
                .containsExactly(CODE_COLUMN);
        BloomFilterReadPlanner.BloomCandidate candidate = plan.candidatesFor(0).getFirst();
        ColumnMetaData metaData = rowGroup.columns().get(CODE_COLUMN).metaData();
        assertThat(candidate.offset()).isEqualTo(metaData.bloomFilterOffset());
        assertThat(candidate.length()).isEqualTo(metaData.bloomFilterLength());
        assertThat(candidate.rowGroupIndex()).isZero();
        assertThat(candidate.rowGroup()).isSameAs(rowGroup);
    }

    @Test
    void inListLeafPlansItsColumnLikeEquality() {
        assertThat(plan(FilterPredicate.in("code", 1, 2)).candidatesFor(0))
                .extracting(BloomFilterReadPlanner.BloomCandidate::columnIndex)
                .containsExactly(CODE_COLUMN);
    }

    @Test
    void columnWithoutBloomFilterIsNotPlanned() {
        // `value`'s chunk carries no bloom_filter_offset, so the consult is left to the lazy path
        // (which answers "no filter") and no fetch is planned.
        assertThat(plan(FilterPredicate.eq("value", 5L))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void rangePredicatePlansNothingEvenWhenItKeepsTheGroup() {
        // `lt` that statistics keep: the evaluator consults blooms only for EQ / IN leaves, so an
        // in-range range predicate plans nothing.
        assertThat(plan(FilterPredicate.lt("code", 5))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void statisticsOutOfRangePredicatePlansNothing() {
        assertThat(plan(FilterPredicate.gt("code", 1000))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void nullPredicatePlansNothing() {
        assertThat(BloomFilterReadPlanner.plan(null, List.of(rowGroup))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void conjunctionPlansEveryBloomEligibleLeaf() {
        assertThat(plan(FilterPredicate.and(
                FilterPredicate.eq("code", 1), FilterPredicate.eq("name", "w"))).candidatesFor(0))
                .extracting(BloomFilterReadPlanner.BloomCandidate::columnIndex)
                .containsExactly(CODE_COLUMN, NAME_COLUMN);
    }

    @Test
    void conjunctionShortCircuitedByStatisticsPlansNothing() {
        // `value` = 10 000 is out of range, so the first child proves CANNOT_MATCH from statistics
        // alone and the second child's bloom check is never reached.
        assertThat(plan(FilterPredicate.and(
                FilterPredicate.eq("value", 10_000L), FilterPredicate.eq("code", 1)))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void nanProbePlansNothing() {
        // The evaluator refuses NaN probes before touching the source; the planner mirrors that
        // automatically instead of re-implementing the carve-out.
        assertThat(plan(FilterPredicate.eq("price", Float.NaN))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void signedZeroProbePlansItsColumn() {
        assertThat(plan(FilterPredicate.eq("price", -0.0f)).candidatesFor(0))
                .extracting(BloomFilterReadPlanner.BloomCandidate::columnIndex)
                .containsExactly(PRICE_COLUMN);
    }

    @Test
    void float16ProbePlansNothing() {
        // The FLOAT16 arm of the evaluator never consults bloom filters (hashing the 2-byte stored
        // form would be lossy), so a FLOAT16 leaf plans nothing even against a bloom-bearing
        // column. The fixture has no FLOAT16 column; the resolved predicate is built directly.
        ResolvedPredicate float16 = new ResolvedPredicate.Float16Predicate(
                CODE_COLUMN, FilterPredicate.Operator.EQ, 1.0f, false);
        assertThat(BloomFilterReadPlanner.plan(float16, List.of(rowGroup))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void nonPositiveOffsetIsLeftToTheLazyPath() {
        // A present but non-positive bloom_filter_offset cannot name a real filter; the lazy path
        // warns and keeps the row group, so the planner must not prefetch it.
        RowGroup patched = patchColumn(CODE_COLUMN, md -> withBloom(md, 0L, md.bloomFilterLength()));
        assertThat(BloomFilterReadPlanner.plan(resolved(FilterPredicate.eq("code", 1)),
                List.of(patched))).isEqualTo(BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void negativeLengthIsRejected() {
        // The footer reader rejects a negative bloom_filter_length (readNonNegativeI32), so this
        // shape is only reachable through synthetic metadata; the planner fails early rather than
        // planning a malformed fetch.
        RowGroup patched = patchColumn(CODE_COLUMN, md -> withBloom(md, 100L, -5));
        assertThatThrownBy(() -> BloomFilterReadPlanner.plan(resolved(FilterPredicate.eq("code", 1)),
                List.of(patched)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bloom_filter_length");
    }

    @Test
    void zeroLengthIsLeftToTheLazyPath() {
        // readNonNegativeI32 admits a zero bloom_filter_length, so a real footer can carry one.
        // An empty region cannot hold a filter and the lazy path fails on it with its usual
        // error, so the planner plans nothing rather than prefetch an empty filter.
        RowGroup patched = patchColumn(CODE_COLUMN, md -> withBloom(md, 100L, 0));
        assertThat(BloomFilterReadPlanner.plan(resolved(FilterPredicate.eq("code", 1)),
                List.of(patched))).isEqualTo(BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void chunkInAnotherFileIsLeftToTheLazyPath() {
        // A chunk whose file_path names another file makes the lazy path throw with its own
        // exception context; the planner excludes it so that timing and message stay unchanged.
        RowGroup patched = patchColumn(CODE_COLUMN,
                md -> withBloom(md, md.bloomFilterOffset(), md.bloomFilterLength()), "other.parquet");
        assertThat(BloomFilterReadPlanner.plan(resolved(FilterPredicate.eq("code", 1)),
                List.of(patched))).isEqualTo(BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void outOfBoundsColumnIndexIsConservativeNoFilter() {
        // Mirrors RowGroupBloomFilterSource.forColumn's bounds guard: a narrower file reached via
        // a predicate resolved against the reference schema yields "no filter", not an exception.
        ResolvedPredicate pastTheEnd =
                new ResolvedPredicate.LongPredicate(99, FilterPredicate.Operator.EQ, 1L);
        assertThat(BloomFilterReadPlanner.plan(pastTheEnd, List.of(rowGroup))).isEqualTo(
                BloomFilterReadPlanner.BloomFilterReadPlan.EMPTY);
    }

    @Test
    void candidatesAreComputedPerRowGroup() {
        List<RowGroup> rowGroups = List.of(rowGroup, rowGroup);
        BloomFilterReadPlanner.BloomFilterReadPlan plan =
                BloomFilterReadPlanner.plan(resolved(FilterPredicate.eq("code", 1)), rowGroups);
        assertThat(plan.candidates()).hasSize(2);
        assertThat(plan.candidatesFor(0))
                .extracting(BloomFilterReadPlanner.BloomCandidate::columnIndex)
                .containsExactly(CODE_COLUMN);
        assertThat(plan.candidatesFor(1))
                .extracting(BloomFilterReadPlanner.BloomCandidate::columnIndex)
                .containsExactly(CODE_COLUMN);
        assertThat(plan.candidatesFor(2)).isEmpty();
    }

    @Test
    void candidatesSupersetOfRealConsultsIncludingSiblingShortCircuit() {
        // The over-approximation bound: the planner may plan a filter the real evaluation skips.
        // With `and(eq(code,1), eq(sparse,1))` the real run's first bloom proof (code=1 absent)
        // short-circuits the AND, so `sparse` is never consulted — yet the planner plans both,
        // which is the prefetched-but-unused excess allowed by the invariant. The direction that
        // matters: the planner never plans *fewer* columns than the real run consults.
        FilterPredicate and = FilterPredicate.and(
                FilterPredicate.eq("code", 1), FilterPredicate.eq("sparse", 1L));
        Set<Integer> realConsults = new LinkedHashSet<>();
        BloomFilterSource real = columnIndex -> {
            realConsults.add(columnIndex);
            return new RowGroupBloomFilterSource(inputFile, rowGroup).forColumn(columnIndex);
        };
        RowGroupFilterEvaluator.decideRowGroup(resolved(and), rowGroup, real, null);

        BloomFilterReadPlanner.BloomFilterReadPlan plan =
                BloomFilterReadPlanner.plan(resolved(and), List.of(rowGroup));
        Set<Integer> planned = new LinkedHashSet<>(plan.candidatesFor(0).stream()
                .map(BloomFilterReadPlanner.BloomCandidate::columnIndex).toList());

        assertThat(planned).containsAll(realConsults);
        assertThat(planned).containsExactlyInAnyOrder(CODE_COLUMN, SPARSE_COLUMN);
        assertThat(realConsults).containsExactly(CODE_COLUMN);
    }

    @Test
    void candidatesSupersetOfRealConsultsAcrossOperators() {
        // eq, IN, and multi-leaf predicates over several bloom-bearing columns: whatever the real
        // evaluation consults, the planner plans.
        List<FilterPredicate> predicates = List.of(
                FilterPredicate.eq("id", 5L),
                FilterPredicate.eq("sparse", 1L),
                FilterPredicate.in("code", 1, 2),
                FilterPredicate.inStrings("name", "w", "v"),
                FilterPredicate.eq("dec", new BigDecimal(1)),
                FilterPredicate.eq("ts", Instant.parse("2024-01-01T00:00:01Z")),
                FilterPredicate.or(FilterPredicate.eq("code", 1), FilterPredicate.eq("name", "w")));
        for (FilterPredicate predicate : predicates) {
            Set<Integer> realConsults = new LinkedHashSet<>();
            BloomFilterSource real = columnIndex -> {
                realConsults.add(columnIndex);
                return new RowGroupBloomFilterSource(inputFile, rowGroup).forColumn(columnIndex);
            };
            RowGroupFilterEvaluator.decideRowGroup(resolved(predicate), rowGroup, real, null);

            Set<Integer> planned = new LinkedHashSet<>(
                    BloomFilterReadPlanner.plan(resolved(predicate), List.of(rowGroup))
                            .candidatesFor(0).stream()
                            .map(BloomFilterReadPlanner.BloomCandidate::columnIndex).toList());
            assertThat(planned).as("planned vs consulted for %s", predicate)
                    .containsAll(realConsults);
        }
    }

    private static BloomFilterReadPlanner.BloomFilterReadPlan plan(FilterPredicate filter) {
        return BloomFilterReadPlanner.plan(resolved(filter), List.of(rowGroup));
    }

    private static ResolvedPredicate resolved(FilterPredicate filter) {
        return FilterPredicateResolver.resolve(filter, schema);
    }

    /// A copy of the row group with one column's metadata edited and its chunk file path kept.
    private static RowGroup patchColumn(int columnIndex,
                                        java.util.function.UnaryOperator<ColumnMetaData> edit) {
        return patchColumn(columnIndex, edit, "");
    }

    private static RowGroup patchColumn(int columnIndex,
                                        java.util.function.UnaryOperator<ColumnMetaData> edit,
                                        String filePath) {
        ColumnChunk original = rowGroup.columns().get(columnIndex);
        ColumnChunk patched = new ColumnChunk(edit.apply(original.metaData()),
                original.offsetIndexOffset(), original.offsetIndexLength(),
                original.columnIndexOffset(), original.columnIndexLength(), filePath);
        List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
        columns.set(columnIndex, patched);
        return new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());
    }

    private static ColumnMetaData withBloom(ColumnMetaData md, Long offset, Integer length) {
        return new ColumnMetaData(
                md.type(), md.encodings(), md.pathInSchema(), md.codec(),
                md.numValues(), md.totalUncompressedSize(), md.totalCompressedSize(),
                md.keyValueMetadata(), md.dataPageOffset(), md.dictionaryPageOffset(),
                md.statistics(), md.geospatialStatistics(), offset, length, md.encodingStats(),
                md.sizeStatistics());
    }
}
