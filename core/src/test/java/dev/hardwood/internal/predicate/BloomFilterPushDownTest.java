/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.XxHash64;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Bloom-filter row-group pruning against `bloom_filter_test.parquet` (one row group, 64 rows;
/// bloom filters on `id` INT64 `0..63`, `code` INT32 `0,3,…,189`, `name` STRING `""`…`"x"*63`,
/// `price` FLOAT `0,2,…,126`, `ratio` DOUBLE `0,0.5,…,31.5`, `dec` DECIMAL(38,0)/FLBA `0,2,…,126`,
/// `ts` TIMESTAMP(us,UTC) at even-second offsets from `2024-01-01`, `sparse` INT64 `0,1000,…,63000`;
/// `value` INT64 `0,10,…,630` carries none).
///
/// The discriminating cases use values that fall *inside* the column's statistics min/max range —
/// so statistics alone keep the row group — but were never written, so only the bloom filter can
/// prove their absence.
class BloomFilterPushDownTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/bloom_filter_test.parquet");

    // Column order in the schema / row group: id(0), value(1), name(2), code(3).
    private static final int VALUE_COLUMN = 1;
    private static final int NAME_COLUMN = 2;

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
    void int32EqualityInsideRangeButAbsentIsDroppedOnlyByBloom() {
        // `code` holds multiples of 3; 1 is in [0, 189] (statistics keep) but never written.
        FilterPredicate absent = FilterPredicate.eq("code", 1);
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        // A stored value is kept by both.
        FilterPredicate present = FilterPredicate.eq("code", 3);
        assertThat(statisticsDrop(present)).isFalse();
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void stringEqualityInsideRangeButAbsentIsDroppedByBloom() {
        // "w" sorts within ["", "x"*63] (statistics keep) but is not one of the stored names.
        FilterPredicate absent = FilterPredicate.eq("name", "w");
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        FilterPredicate present = FilterPredicate.eq("name", "xx");
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void floatEqualityInsideRangeButAbsentIsDroppedOnlyByBloom() {
        // `price` holds even multiples of 2; 1.0 is in [0.0, 126.0] (statistics keep) but never
        // written, so only the bloom filter proves its absence.
        FilterPredicate absent = FilterPredicate.eq("price", 1.0f);
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        // A stored value is kept by both.
        FilterPredicate present = FilterPredicate.eq("price", 2.0f);
        assertThat(statisticsDrop(present)).isFalse();
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void doubleEqualityInsideRangeButAbsentIsDroppedOnlyByBloom() {
        // `ratio` holds multiples of 0.5; 0.25 is in [0.0, 31.5] (statistics keep) but never written.
        FilterPredicate absent = FilterPredicate.eq("ratio", 0.25);
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        FilterPredicate present = FilterPredicate.eq("ratio", 0.5);
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void negativeZeroFloatIsNeverPrunedByBloom() {
        // `price` stores +0.0; a query for -0.0f must still match it (-0.0f == +0.0f) even though the
        // two have different IEEE-754 bits and therefore different bloom hashes. The ±0 carve-out
        // keeps the row group rather than pruning on the raw-bit hash.
        FilterPredicate negZero = FilterPredicate.eq("price", -0.0f);
        assertThat(statisticsDrop(negZero)).isFalse();
        assertThat(bloomDrop(negZero)).isFalse();
    }

    @Test
    void fixedLenByteArrayDecimalEqualityInsideRangeButAbsentIsDroppedByBloom() {
        // `dec` is DECIMAL(38,0) stored as FIXED_LEN_BYTE_ARRAY(16) and holds even values; 1 is in
        // [0, 126] (statistics keep) but never written. Exercises the FLBA equality path with real
        // fixed-length bytes and cross-checks that the FLBA decimal encoding matches the writer's.
        FilterPredicate absent = FilterPredicate.eq("dec", new BigDecimal(1));
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        FilterPredicate present = FilterPredicate.eq("dec", new BigDecimal(2));
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void timestampLogicalTypeEqualityUsesBloomOnThePhysicalLong() {
        // `ts` is TIMESTAMP(us, UTC) — physically INT64 — at even-second offsets from 2024-01-01.
        // An Instant predicate resolves to the physical long (epoch micros), so an odd-second
        // instant lands in range but was never written: only the bloom filter, keyed on that
        // INT64, can prove it absent. Confirms logical predicate types prune via their physical value.
        FilterPredicate absent = FilterPredicate.eq("ts", Instant.parse("2024-01-01T00:00:01Z"));
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        FilterPredicate present = FilterPredicate.eq("ts", Instant.parse("2024-01-01T00:00:02Z"));
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void columnWithoutBloomFilterFallsBackToStatistics() {
        // `value` holds multiples of 10 and carries no bloom filter; an absent in-range value
        // cannot be dropped, so the row group is kept.
        FilterPredicate absentInRange = FilterPredicate.eq("value", 5L);
        assertThat(bloomDrop(absentInRange)).isFalse();

        // Out of range is still dropped by statistics alone.
        assertThat(bloomDrop(FilterPredicate.eq("value", 10_000L))).isTrue();
    }

    @Test
    void inListIsDroppedOnlyWhenEveryValueIsAbsent() {
        assertThat(bloomDrop(FilterPredicate.in("code", 1, 2))).isTrue();
        assertThat(bloomDrop(FilterPredicate.in("code", 1, 3))).isFalse();
    }

    @Test
    void int64EqualityInsideRangeButAbsentIsDroppedOnlyByBloom() {
        // `sparse` holds multiples of 1000; 1 is in [0, 63000] (statistics keep) but never written,
        // so only the bloom filter proves its absence. (`id` is contiguous 0..63 — no in-range gap.)
        FilterPredicate absent = FilterPredicate.eq("sparse", 1L);
        assertThat(statisticsDrop(absent)).isFalse();
        assertThat(bloomDrop(absent)).isTrue();

        FilterPredicate present = FilterPredicate.eq("sparse", 1000L);
        assertThat(bloomDrop(present)).isFalse();
    }

    @Test
    void int64InListIsDroppedOnlyWhenEveryValueIsAbsent() {
        assertThat(bloomDrop(FilterPredicate.in("sparse", 1L, 2L))).isTrue();
        assertThat(bloomDrop(FilterPredicate.in("sparse", 1L, 1000L))).isFalse();
    }

    @Test
    void binaryInListIsDroppedOnlyWhenEveryValueIsAbsent() {
        // `name` holds only runs of 'x' ("" … "x"*63). "w"/"v" are not stored, yet sort inside the
        // min/max range ["", "x"*63] ('v','w' < 'x'), so statistics keep but the bloom filter drops.
        assertThat(bloomDrop(FilterPredicate.inStrings("name", "w", "v"))).isTrue();
        assertThat(bloomDrop(FilterPredicate.inStrings("name", "w", "xx"))).isFalse();
    }

    @Test
    void andDropsWhenAnyBloomEligibleLeafIsAbsent() {
        // code=1 is absent -> the conjunction matches nothing.
        FilterPredicate and = FilterPredicate.and(
                FilterPredicate.eq("code", 1), FilterPredicate.eq("id", 5L));
        assertThat(bloomDrop(and)).isTrue();
    }

    @Test
    void orDropsOnlyWhenEveryBranchIsAbsent() {
        FilterPredicate bothAbsent = FilterPredicate.or(
                FilterPredicate.eq("code", 1), FilterPredicate.eq("code", 2));
        assertThat(bloomDrop(bothAbsent)).isTrue();

        FilterPredicate onePresent = FilterPredicate.or(
                FilterPredicate.eq("code", 1), FilterPredicate.eq("id", 5L));
        assertThat(bloomDrop(onePresent)).isFalse();
    }

    @Test
    void readsBloomFilterWhenLengthIsAbsent() {
        // Legacy writers omit bloom_filter_length, forcing the header-probe path. Blank the length
        // on the `name` chunk (keeping its offset) and confirm the filter is reconstructed from the
        // probe alone — a stored value must still report present (no false negatives).
        ColumnChunk original = rowGroup.columns().get(NAME_COLUMN);
        ColumnMetaData md = original.metaData();
        ColumnMetaData withoutLength = new ColumnMetaData(
                md.type(), md.encodings(), md.pathInSchema(), md.codec(),
                md.numValues(), md.totalUncompressedSize(), md.totalCompressedSize(),
                md.keyValueMetadata(), md.dataPageOffset(), md.dictionaryPageOffset(),
                md.statistics(), md.geospatialStatistics(), md.bloomFilterOffset(), null);
        ColumnChunk patched = new ColumnChunk(withoutLength, original.offsetIndexOffset(),
                original.offsetIndexLength(), original.columnIndexOffset(), original.columnIndexLength());

        List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
        columns.set(NAME_COLUMN, patched);
        RowGroup patchedRowGroup = new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());

        BloomFilter filter = new RowGroupBloomFilterSource(inputFile, patchedRowGroup).forColumn(NAME_COLUMN);
        assertThat(filter).isNotNull();
        assertThat(filter.mightContain(XxHash64.hash("x".getBytes(StandardCharsets.UTF_8)))).isTrue();
    }

    @Test
    void rowGroupBloomFilterSourceExposesFiltersPerColumn() {
        RowGroupBloomFilterSource source = new RowGroupBloomFilterSource(inputFile, rowGroup);
        assertThat(source.forColumn(NAME_COLUMN)).isNotNull();
        assertThat(source.forColumn(VALUE_COLUMN)).isNull();
        // Out-of-bounds index (e.g. a narrower file in a multi-file scan) is conservatively
        // treated as "no filter", not an exception.
        assertThat(source.forColumn(rowGroup.columns().size())).isNull();
    }

    private static boolean bloomDrop(FilterPredicate filter) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.canDropRowGroup(resolved, rowGroup,
                new RowGroupBloomFilterSource(inputFile, rowGroup));
    }

    private static boolean statisticsDrop(FilterPredicate filter) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.canDropRowGroup(resolved, rowGroup);
    }
}
