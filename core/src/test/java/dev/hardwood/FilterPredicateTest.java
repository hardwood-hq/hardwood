/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterPredicateTest {

    // ==================== Predicate Factory Tests ====================

    @Test
    void testIntPredicateCreation() {
        FilterPredicate p = FilterPredicate.eq("id", 42);
        assertThat(p).isInstanceOf(FilterPredicate.IntColumnPredicate.class);
        FilterPredicate.IntColumnPredicate ip = (FilterPredicate.IntColumnPredicate) p;
        assertThat(ip.column()).isEqualTo("id");
        assertThat(ip.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(ip.value()).isEqualTo(42);
    }

    @Test
    void testLongPredicateCreation() {
        FilterPredicate p = FilterPredicate.gt("value", 100L);
        assertThat(p).isInstanceOf(FilterPredicate.LongColumnPredicate.class);
        FilterPredicate.LongColumnPredicate lp = (FilterPredicate.LongColumnPredicate) p;
        assertThat(lp.column()).isEqualTo("value");
        assertThat(lp.op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(lp.value()).isEqualTo(100L);
    }

    @Test
    void testFloatPredicateCreation() {
        FilterPredicate p = FilterPredicate.lt("rating", 3.5f);
        assertThat(p).isInstanceOf(FilterPredicate.FloatColumnPredicate.class);
    }

    @Test
    void testDoublePredicateCreation() {
        FilterPredicate p = FilterPredicate.gtEq("price", 50.0);
        assertThat(p).isInstanceOf(FilterPredicate.DoubleColumnPredicate.class);
    }

    @Test
    void testBooleanPredicateCreation() {
        FilterPredicate p = FilterPredicate.eq("active", true);
        assertThat(p).isInstanceOf(FilterPredicate.BooleanColumnPredicate.class);
    }

    @Test
    void testStringPredicateCreation() {
        FilterPredicate p = FilterPredicate.eq("name", "hello");
        assertThat(p).isInstanceOf(FilterPredicate.BinaryColumnPredicate.class);
    }

    @Test
    void testAndComposition() {
        FilterPredicate p = FilterPredicate.and(
                FilterPredicate.gt("id", 10),
                FilterPredicate.lt("id", 20)
        );
        assertThat(p).isInstanceOf(FilterPredicate.And.class);
        FilterPredicate.And and = (FilterPredicate.And) p;
        assertThat(and.filters()).hasSize(2);
        assertThat(and.filters().get(0)).isInstanceOf(FilterPredicate.IntColumnPredicate.class);
        assertThat(and.filters().get(1)).isInstanceOf(FilterPredicate.IntColumnPredicate.class);
    }

    @Test
    void testOrComposition() {
        FilterPredicate p = FilterPredicate.or(
                FilterPredicate.eq("status", "active"),
                FilterPredicate.eq("status", "pending")
        );
        assertThat(p).isInstanceOf(FilterPredicate.Or.class);
    }

    // ==================== Row Group Filter Evaluation Tests ====================

    @Test
    void testCanDropWithEq() {
        // Row group with int values min=10, max=20
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // EQ 15 (in range) -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 15), rg, schema)).isFalse();

        // EQ 5 (below min) -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 5), rg, schema)).isTrue();

        // EQ 25 (above max) -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 25), rg, schema)).isTrue();

        // EQ 10 (equals min) -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 10), rg, schema)).isFalse();

        // EQ 20 (equals max) -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 20), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithNotEq() {
        // Row group with single value: min=max=42
        RowGroup rg = createIntRowGroup(42, 42);
        FileSchema schema = createIntSchema();

        // NOT_EQ 42 when min==max==42 -> can drop (all values are 42)
        assertThat(canDropRowGroup(
                FilterPredicate.notEq("col", 42), rg, schema)).isTrue();

        // NOT_EQ 10 when min==max==42 -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.notEq("col", 10), rg, schema)).isFalse();

        // NOT_EQ with range min=10, max=20 -> cannot drop
        RowGroup range = createIntRowGroup(10, 20);
        assertThat(canDropRowGroup(
                FilterPredicate.notEq("col", 15), range, schema)).isFalse();
    }

    @Test
    void testCanDropWithLt() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // LT 5 -> all values >= 10 so none < 5 -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.lt("col", 5), rg, schema)).isTrue();

        // LT 10 -> all values >= 10, none < 10 -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.lt("col", 10), rg, schema)).isTrue();

        // LT 15 -> some values < 15 -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.lt("col", 15), rg, schema)).isFalse();

        // LT 25 -> all values < 25 possible -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.lt("col", 25), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithLtEq() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // LT_EQ 9 -> all values >= 10 -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.ltEq("col", 9), rg, schema)).isTrue();

        // LT_EQ 10 -> min == 10 -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.ltEq("col", 10), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithGt() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // GT 25 -> max is 20 -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 25), rg, schema)).isTrue();

        // GT 20 -> max is 20, none > 20 -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 20), rg, schema)).isTrue();

        // GT 15 -> some values > 15 -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 15), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithGtEq() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // GT_EQ 21 -> max is 20 -> can drop
        assertThat(canDropRowGroup(
                FilterPredicate.gtEq("col", 21), rg, schema)).isTrue();

        // GT_EQ 20 -> max is 20 -> cannot drop
        assertThat(canDropRowGroup(
                FilterPredicate.gtEq("col", 20), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithLongPredicate() {
        RowGroup rg = createLongRowGroup(100L, 200L);
        FileSchema schema = createLongSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 200L), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.lt("col", 100L), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 150L), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithDoublePredicate() {
        RowGroup rg = createDoubleRowGroup(1.0, 10.0);
        FileSchema schema = createDoubleSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 10.0), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.lt("col", 1.0), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 5.0), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithFloatPredicate() {
        RowGroup rg = createFloatRowGroup(1.0f, 10.0f);
        FileSchema schema = createFloatSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 10.0f), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.ltEq("col", 0.5f), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 5.0f), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithBooleanPredicate() {
        // Row group with all true: min=true, max=true
        RowGroup allTrue = createBooleanRowGroup(true, true);
        FileSchema schema = createBooleanSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", false), allTrue, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", true), allTrue, schema)).isFalse();

        // Row group with mixed: min=false, max=true
        RowGroup mixed = createBooleanRowGroup(false, true);
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", false), mixed, schema)).isFalse();
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", true), mixed, schema)).isFalse();
    }

    @Test
    void testCanDropWithBinaryPredicate() {
        // Row group with strings "banana" to "date"
        RowGroup rg = createBinaryRowGroup("banana".getBytes(), "date".getBytes());
        FileSchema schema = createBinarySchema();

        // "apple" < "banana" -> EQ cannot match
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", "apple"), rg, schema)).isTrue();

        // "elderberry" > "date" -> EQ cannot match
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", "elderberry"), rg, schema)).isTrue();

        // "cherry" in range -> EQ might match
        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", "cherry"), rg, schema)).isFalse();
    }

    @Test
    void testAndEvaluation() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // AND where left can drop -> can drop
        FilterPredicate andLeftDrop = FilterPredicate.and(
                FilterPredicate.gt("col", 25),  // can drop (max=20)
                FilterPredicate.lt("col", 30)   // cannot drop
        );
        assertThat(canDropRowGroup(andLeftDrop, rg, schema)).isTrue();

        // AND where right can drop -> can drop
        FilterPredicate andRightDrop = FilterPredicate.and(
                FilterPredicate.lt("col", 30),   // cannot drop
                FilterPredicate.gt("col", 25)    // can drop
        );
        assertThat(canDropRowGroup(andRightDrop, rg, schema)).isTrue();

        // AND where neither can drop -> cannot drop
        FilterPredicate andNeitherDrop = FilterPredicate.and(
                FilterPredicate.gt("col", 5),
                FilterPredicate.lt("col", 25)
        );
        assertThat(canDropRowGroup(andNeitherDrop, rg, schema)).isFalse();
    }

    @Test
    void testOrEvaluation() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // OR where both can drop -> can drop
        FilterPredicate orBothDrop = FilterPredicate.or(
                FilterPredicate.lt("col", 5),    // can drop
                FilterPredicate.gt("col", 25)    // can drop
        );
        assertThat(canDropRowGroup(orBothDrop, rg, schema)).isTrue();

        // OR where only left can drop -> cannot drop
        FilterPredicate orLeftOnly = FilterPredicate.or(
                FilterPredicate.lt("col", 5),    // can drop
                FilterPredicate.gt("col", 15)    // cannot drop
        );
        assertThat(canDropRowGroup(orLeftOnly, rg, schema)).isFalse();

        // OR where neither can drop -> cannot drop
        FilterPredicate orNeitherDrop = FilterPredicate.or(
                FilterPredicate.gt("col", 5),
                FilterPredicate.lt("col", 25)
        );
        assertThat(canDropRowGroup(orNeitherDrop, rg, schema)).isFalse();
    }

    @Test
    void testMissingStatisticsNeverDrop() {
        // Row group with no statistics
        RowGroup rg = createRowGroupWithoutStatistics();
        FileSchema schema = createIntSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.eq("col", 42), rg, schema)).isFalse();
        assertThat(canDropRowGroup(
                FilterPredicate.gt("col", 100), rg, schema)).isFalse();
    }

    @Test
    void testUnknownColumnThrowsAtResolve() {
        FileSchema schema = createIntSchema();

        // Filter on a column that doesn't exist -> throws at resolve time
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("nonexistent", 42), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void testIntInPredicateCreation() {
        FilterPredicate p = FilterPredicate.in("id", 1, 5, 10);
        assertThat(p).isInstanceOf(FilterPredicate.IntInPredicate.class);
        FilterPredicate.IntInPredicate ip = (FilterPredicate.IntInPredicate) p;
        assertThat(ip.column()).isEqualTo("id");
        assertThat(ip.values()).containsExactly(1, 5, 10);
    }

    @Test
    void testLongInPredicateCreation() {
        FilterPredicate p = FilterPredicate.in("ts", 100L, 200L);
        assertThat(p).isInstanceOf(FilterPredicate.LongInPredicate.class);
    }

    @Test
    void testStringInPredicateCreation() {
        FilterPredicate p = FilterPredicate.inStrings("city", "NYC", "LA");
        assertThat(p).isInstanceOf(FilterPredicate.BinaryInPredicate.class);
    }

    @Test
    void testCanDropWithIntIn() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 1, 5, 8), rg, schema)).isTrue();

        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 25, 30), rg, schema)).isTrue();

        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 5, 15, 25), rg, schema)).isFalse();

        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 1, 10), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithLongIn() {
        RowGroup rg = createLongRowGroup(100L, 200L);
        FileSchema schema = createLongSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 50L, 80L), rg, schema)).isTrue();
        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 50L, 150L), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithStringIn() {
        RowGroup rg = createBinaryRowGroup("banana".getBytes(), "date".getBytes());
        FileSchema schema = createBinarySchema();

        assertThat(canDropRowGroup(
                FilterPredicate.inStrings("col", "apple", "elderberry"), rg, schema)).isTrue();

        assertThat(canDropRowGroup(
                FilterPredicate.inStrings("col", "apple", "cherry"), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithInMissingStatistics() {
        RowGroup rg = createRowGroupWithoutStatistics();
        FileSchema schema = createIntSchema();

        assertThat(canDropRowGroup(
                FilterPredicate.in("col", 1, 2, 3), rg, schema)).isFalse();
    }

    // ==================== IS NULL / IS NOT NULL Factory Tests ====================

    @Test
    void testIsNullPredicateCreation() {
        FilterPredicate p = FilterPredicate.isNull("name");
        assertThat(p).isInstanceOf(FilterPredicate.IsNullPredicate.class);
        FilterPredicate.IsNullPredicate np = (FilterPredicate.IsNullPredicate) p;
        assertThat(np.column()).isEqualTo("name");
    }

    @Test
    void testIsNotNullPredicateCreation() {
        FilterPredicate p = FilterPredicate.isNotNull("name");
        assertThat(p).isInstanceOf(FilterPredicate.IsNotNullPredicate.class);
        FilterPredicate.IsNotNullPredicate np = (FilterPredicate.IsNotNullPredicate) p;
        assertThat(np.column()).isEqualTo("name");
    }

    // ==================== IS NULL / IS NOT NULL Row Group Evaluation Tests ====================

    @Test
    void testCanDropWithIsNull() {
        FileSchema schema = createIntSchema();

        // nullCount=0 -> can drop IS NULL (no nulls in this row group)
        RowGroup rgNoNulls = createRowGroupWithNullCount(PhysicalType.INT32, 0L, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNull("col"), rgNoNulls, schema)).isTrue();

        // nullCount=50 -> cannot drop IS NULL (some nulls exist)
        RowGroup rgSomeNulls = createRowGroupWithNullCount(PhysicalType.INT32, 50L, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNull("col"), rgSomeNulls, schema)).isFalse();

        // nullCount=100 (all null) -> cannot drop IS NULL
        RowGroup rgAllNulls = createRowGroupWithNullCount(PhysicalType.INT32, 100L, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNull("col"), rgAllNulls, schema)).isFalse();

        // nullCount unknown -> cannot drop (conservative)
        RowGroup rgUnknown = createRowGroupWithNullCount(PhysicalType.INT32, null, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNull("col"), rgUnknown, schema)).isFalse();
    }

    @Test
    void testCanDropWithIsNotNull() {
        FileSchema schema = createIntSchema();

        // nullCount=0 -> cannot drop IS NOT NULL (all values are non-null)
        RowGroup rgNoNulls = createRowGroupWithNullCount(PhysicalType.INT32, 0L, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNotNull("col"), rgNoNulls, schema)).isFalse();

        // nullCount=50 -> cannot drop IS NOT NULL (some non-nulls exist)
        RowGroup rgSomeNulls = createRowGroupWithNullCount(PhysicalType.INT32, 50L, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNotNull("col"), rgSomeNulls, schema)).isFalse();

        // nullCount=100 (all null) -> can drop IS NOT NULL
        RowGroup rgAllNulls = createRowGroupWithNullCount(PhysicalType.INT32, 100L, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNotNull("col"), rgAllNulls, schema)).isTrue();

        // nullCount unknown -> cannot drop (conservative)
        RowGroup rgUnknown = createRowGroupWithNullCount(PhysicalType.INT32, null, 100);
        assertThat(canDropRowGroup(FilterPredicate.isNotNull("col"), rgUnknown, schema)).isFalse();
    }

    @Test
    void testIsNullWorksOnAnyColumnType() {
        // IS NULL / IS NOT NULL should work on any physical type without type validation errors
        for (PhysicalType type : new PhysicalType[] {
                PhysicalType.INT32, PhysicalType.INT64, PhysicalType.FLOAT,
                PhysicalType.DOUBLE, PhysicalType.BOOLEAN, PhysicalType.BYTE_ARRAY }) {
            FileSchema schema = createSchemaForType(type);
            RowGroup rg = createRowGroupWithNullCount(type, 0L, 100);

            // Should not throw
            canDropRowGroup(FilterPredicate.isNull("col"), rg, schema);
            canDropRowGroup(FilterPredicate.isNotNull("col"), rg, schema);
        }
    }

    // ==================== LocalDate Factory Tests ====================

    @Test
    void testLocalDatePredicateCreation() {
        LocalDate date = LocalDate.of(2024, 6, 15);
        FilterPredicate p = FilterPredicate.eq("dt", date);
        assertThat(p).isInstanceOf(FilterPredicate.DateColumnPredicate.class);
        FilterPredicate.DateColumnPredicate dp = (FilterPredicate.DateColumnPredicate) p;
        assertThat(dp.column()).isEqualTo("dt");
        assertThat(dp.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(dp.value()).isEqualTo(date);
    }

    @Test
    void testLocalDateAllOperators() {
        LocalDate date = LocalDate.of(2024, 1, 1);
        assertThat(((FilterPredicate.DateColumnPredicate) FilterPredicate.notEq("d", date)).op()).isEqualTo(FilterPredicate.Operator.NOT_EQ);
        assertThat(((FilterPredicate.DateColumnPredicate) FilterPredicate.lt("d", date)).op()).isEqualTo(FilterPredicate.Operator.LT);
        assertThat(((FilterPredicate.DateColumnPredicate) FilterPredicate.ltEq("d", date)).op()).isEqualTo(FilterPredicate.Operator.LT_EQ);
        assertThat(((FilterPredicate.DateColumnPredicate) FilterPredicate.gt("d", date)).op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(((FilterPredicate.DateColumnPredicate) FilterPredicate.gtEq("d", date)).op()).isEqualTo(FilterPredicate.Operator.GT_EQ);
    }

    // ==================== Instant Factory Tests ====================

    @Test
    void testInstantPredicateCreation() {
        Instant instant = Instant.parse("2024-06-15T12:30:00Z");
        FilterPredicate p = FilterPredicate.eq("ts", instant);
        assertThat(p).isInstanceOf(FilterPredicate.InstantColumnPredicate.class);
        FilterPredicate.InstantColumnPredicate ip = (FilterPredicate.InstantColumnPredicate) p;
        assertThat(ip.column()).isEqualTo("ts");
        assertThat(ip.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(ip.value()).isEqualTo(instant);
    }

    @Test
    void testInstantAllOperators() {
        Instant instant = Instant.parse("2024-01-01T00:00:00Z");
        assertThat(((FilterPredicate.InstantColumnPredicate) FilterPredicate.notEq("ts", instant)).op()).isEqualTo(FilterPredicate.Operator.NOT_EQ);
        assertThat(((FilterPredicate.InstantColumnPredicate) FilterPredicate.lt("ts", instant)).op()).isEqualTo(FilterPredicate.Operator.LT);
        assertThat(((FilterPredicate.InstantColumnPredicate) FilterPredicate.ltEq("ts", instant)).op()).isEqualTo(FilterPredicate.Operator.LT_EQ);
        assertThat(((FilterPredicate.InstantColumnPredicate) FilterPredicate.gt("ts", instant)).op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(((FilterPredicate.InstantColumnPredicate) FilterPredicate.gtEq("ts", instant)).op()).isEqualTo(FilterPredicate.Operator.GT_EQ);
    }

    // ==================== LocalTime Factory Tests ====================

    @Test
    void testLocalTimePredicateCreation() {
        LocalTime time = LocalTime.of(12, 30, 45);
        FilterPredicate p = FilterPredicate.eq("t", time);
        assertThat(p).isInstanceOf(FilterPredicate.TimeColumnPredicate.class);
        FilterPredicate.TimeColumnPredicate tp = (FilterPredicate.TimeColumnPredicate) p;
        assertThat(tp.column()).isEqualTo("t");
        assertThat(tp.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(tp.value()).isEqualTo(time);
    }

    @Test
    void testLocalTimeAllOperators() {
        LocalTime time = LocalTime.of(10, 0);
        assertThat(((FilterPredicate.TimeColumnPredicate) FilterPredicate.notEq("t", time)).op()).isEqualTo(FilterPredicate.Operator.NOT_EQ);
        assertThat(((FilterPredicate.TimeColumnPredicate) FilterPredicate.lt("t", time)).op()).isEqualTo(FilterPredicate.Operator.LT);
        assertThat(((FilterPredicate.TimeColumnPredicate) FilterPredicate.ltEq("t", time)).op()).isEqualTo(FilterPredicate.Operator.LT_EQ);
        assertThat(((FilterPredicate.TimeColumnPredicate) FilterPredicate.gt("t", time)).op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(((FilterPredicate.TimeColumnPredicate) FilterPredicate.gtEq("t", time)).op()).isEqualTo(FilterPredicate.Operator.GT_EQ);
    }

    // ==================== BigDecimal Factory Tests ====================

    @Test
    void testDecimalPredicateCreation() {
        BigDecimal value = new BigDecimal("99.99");
        FilterPredicate p = FilterPredicate.eq("amount", value);
        assertThat(p).isInstanceOf(FilterPredicate.DecimalColumnPredicate.class);
        FilterPredicate.DecimalColumnPredicate dp = (FilterPredicate.DecimalColumnPredicate) p;
        assertThat(dp.column()).isEqualTo("amount");
        assertThat(dp.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(dp.value()).isEqualTo(value);
    }

    @Test
    void testDecimalAllOperators() {
        BigDecimal value = new BigDecimal("100.00");
        assertThat(((FilterPredicate.DecimalColumnPredicate) FilterPredicate.notEq("a", value)).op()).isEqualTo(FilterPredicate.Operator.NOT_EQ);
        assertThat(((FilterPredicate.DecimalColumnPredicate) FilterPredicate.lt("a", value)).op()).isEqualTo(FilterPredicate.Operator.LT);
        assertThat(((FilterPredicate.DecimalColumnPredicate) FilterPredicate.ltEq("a", value)).op()).isEqualTo(FilterPredicate.Operator.LT_EQ);
        assertThat(((FilterPredicate.DecimalColumnPredicate) FilterPredicate.gt("a", value)).op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(((FilterPredicate.DecimalColumnPredicate) FilterPredicate.gtEq("a", value)).op()).isEqualTo(FilterPredicate.Operator.GT_EQ);
    }

    // ==================== UUID Factory Tests ====================

    @Test
    void testUuidPredicateCreation() {
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        FilterPredicate p = FilterPredicate.eq("request_id", uuid);
        assertThat(p).isInstanceOf(FilterPredicate.UUIDColumnPredicate.class);
        FilterPredicate.UUIDColumnPredicate bp = (FilterPredicate.UUIDColumnPredicate) p;
        assertThat(bp.column()).isEqualTo("request_id");
        assertThat(bp.op()).isEqualTo(FilterPredicate.Operator.EQ);

        ByteBuffer expected = ByteBuffer.allocate(16);
        expected.putLong(uuid.getMostSignificantBits());
        expected.putLong(uuid.getLeastSignificantBits());
        assertThat(bp.value()).isEqualTo(expected.array());
    }

    @Test
    void testUuidAllOperators() {
        UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000001");
        assertThat(((FilterPredicate.UUIDColumnPredicate) FilterPredicate.eq("u", uuid)).op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(((FilterPredicate.UUIDColumnPredicate) FilterPredicate.notEq("u", uuid)).op()).isEqualTo(FilterPredicate.Operator.NOT_EQ);
        assertThat(((FilterPredicate.UUIDColumnPredicate) FilterPredicate.lt("u", uuid)).op()).isEqualTo(FilterPredicate.Operator.LT);
        assertThat(((FilterPredicate.UUIDColumnPredicate) FilterPredicate.ltEq("u", uuid)).op()).isEqualTo(FilterPredicate.Operator.LT_EQ);
        assertThat(((FilterPredicate.UUIDColumnPredicate) FilterPredicate.gt("u", uuid)).op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(((FilterPredicate.UUIDColumnPredicate) FilterPredicate.gtEq("u", uuid)).op()).isEqualTo(FilterPredicate.Operator.GT_EQ);
    }

    @Test
    void testUuidNilValue() {
        UUID nil = new UUID(0L, 0L);
        FilterPredicate.UUIDColumnPredicate bp =
                (FilterPredicate.UUIDColumnPredicate) FilterPredicate.eq("u", nil);
        assertThat(bp.value()).isEqualTo(new byte[16]);
    }

    @Test
    void uuidPredicateOnNonUuidColumnThrows() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement("col", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                RepetitionType.REQUIRED, null, null, null, null, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, col));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", UUID.randomUUID()), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UuidType");
    }

    @Test
    void testCanDropWithUuidPredicate() {
        UUID low   = new UUID(0L, 1L);
        UUID mid   = new UUID(0L, 50L);
        UUID high  = new UUID(0L, 100L);
        UUID above = new UUID(0L, 200L);

        RowGroup rg = createUuidRowGroup(low, high);
        FileSchema schema = createUuidSchema();

        // EQ mid: in range → cannot drop
        assertThat(canDropRowGroup(FilterPredicate.eq("col", mid), rg, schema)).isFalse();
        // EQ above max → can drop
        assertThat(canDropRowGroup(FilterPredicate.eq("col", above), rg, schema)).isTrue();
        // GT high: max is high, none > high → can drop
        assertThat(canDropRowGroup(FilterPredicate.gt("col", high), rg, schema)).isTrue();
        // LT low: min is low, none < low → can drop
        assertThat(canDropRowGroup(FilterPredicate.lt("col", low), rg, schema)).isTrue();
        // GT_EQ low: max >= low → cannot drop
        assertThat(canDropRowGroup(FilterPredicate.gtEq("col", low), rg, schema)).isFalse();
    }

    // ==================== Float/Double Edge Cases ====================

    @Test
    void testCanDropWithDoubleNaN() {
        // NaN is ordered after +Infinity by Double.compare
        RowGroup rg = createDoubleRowGroup(1.0, 10.0);
        FileSchema schema = createDoubleSchema();
        // EQ NaN: NaN > max(10.0), so can drop
        assertThat(canDropRowGroup(FilterPredicate.eq("col", Double.NaN), rg, schema)).isTrue();
        // GT NaN: max(10.0) < NaN, so can drop
        assertThat(canDropRowGroup(FilterPredicate.gt("col", Double.NaN), rg, schema)).isTrue();
        // LT NaN: min(1.0) < NaN, so cannot drop (some values < NaN)
        assertThat(canDropRowGroup(FilterPredicate.lt("col", Double.NaN), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithDoubleNaNInStatistics() {
        // Row group where max is NaN (can happen with some writers)
        RowGroup rg = createDoubleRowGroup(1.0, Double.NaN);
        FileSchema schema = createDoubleSchema();
        // EQ 5.0: 5.0 < NaN (max), so cannot drop
        assertThat(canDropRowGroup(FilterPredicate.eq("col", 5.0), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithDoubleNegativeZero() {
        // -0.0 compares less than +0.0 via Double.compare
        RowGroup rg = createDoubleRowGroup(-0.0, 0.0);
        FileSchema schema = createDoubleSchema();
        // EQ -0.0: in range, cannot drop
        assertThat(canDropRowGroup(FilterPredicate.eq("col", -0.0), rg, schema)).isFalse();
        // EQ +0.0: in range, cannot drop
        assertThat(canDropRowGroup(FilterPredicate.eq("col", 0.0), rg, schema)).isFalse();
        // LT -0.0: min is -0.0, min >= value, can drop
        assertThat(canDropRowGroup(FilterPredicate.lt("col", -0.0), rg, schema)).isTrue();
    }

    @Test
    void testCanDropWithDoubleInfinity() {
        RowGroup rg = createDoubleRowGroup(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        FileSchema schema = createDoubleSchema();
        // Any finite value is in range
        assertThat(canDropRowGroup(FilterPredicate.eq("col", 0.0), rg, schema)).isFalse();
        // GT +Infinity: max is +Inf, +Inf <= +Inf, can drop
        assertThat(canDropRowGroup(FilterPredicate.gt("col", Double.POSITIVE_INFINITY), rg, schema)).isTrue();
        // LT -Infinity: min is -Inf, -Inf >= -Inf, can drop
        assertThat(canDropRowGroup(FilterPredicate.lt("col", Double.NEGATIVE_INFINITY), rg, schema)).isTrue();
    }

    @Test
    void testCanDropWithFloatNaN() {
        RowGroup rg = createFloatRowGroup(1.0f, 10.0f);
        FileSchema schema = createFloatSchema();
        assertThat(canDropRowGroup(FilterPredicate.eq("col", Float.NaN), rg, schema)).isTrue();
        assertThat(canDropRowGroup(FilterPredicate.gt("col", Float.NaN), rg, schema)).isTrue();
        assertThat(canDropRowGroup(FilterPredicate.lt("col", Float.NaN), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithFloatNegativeZero() {
        RowGroup rg = createFloatRowGroup(-0.0f, 0.0f);
        FileSchema schema = createFloatSchema();
        assertThat(canDropRowGroup(FilterPredicate.eq("col", -0.0f), rg, schema)).isFalse();
        assertThat(canDropRowGroup(FilterPredicate.eq("col", 0.0f), rg, schema)).isFalse();
        assertThat(canDropRowGroup(FilterPredicate.lt("col", -0.0f), rg, schema)).isTrue();
    }

    // ==================== Compound NOT Tests ====================

    @Test
    void testNotWrappingAndIsConservative() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();
        // NOT(AND(GT 25, LT 5)) — both children would drop, but NOT is conservative
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.and(
                FilterPredicate.gt("col", 25),
                FilterPredicate.lt("col", 5)));
        assertThat(canDropRowGroup(filter, rg, schema)).isFalse();
    }

    @Test
    void testNotWrappingOrIsConservative() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();
        // NOT(OR(EQ 5, EQ 25)) — both children would drop, OR drops, but NOT is conservative
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.or(
                FilterPredicate.eq("col", 5),
                FilterPredicate.eq("col", 25)));
        assertThat(canDropRowGroup(filter, rg, schema)).isFalse();
    }

    @Test
    void testNotWrappingLeafIsConservative() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();
        // NOT(GT 25) should ideally push down as LT_EQ 25, but currently conservative
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.gt("col", 25));
        assertThat(canDropRowGroup(filter, rg, schema)).isFalse();
    }

    // ==================== Type Mismatch Tests ====================

    @Test
    void intPredicateOnStringColumnThrows() {
        RowGroup rg = createBinaryRowGroup(new byte[]{0x41}, new byte[]{0x5A});
        FileSchema schema = createBinarySchema();
        assertThatThrownBy(() -> canDropRowGroup(
                FilterPredicate.eq("col", 42), rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type BYTE_ARRAY"
                        + "; given filter predicate type INT32 is incompatible");
    }

    @Test
    void longPredicateOnIntColumnThrows() {
        RowGroup rg = createIntRowGroup(0, 100);
        FileSchema schema = createIntSchema();
        assertThatThrownBy(() -> canDropRowGroup(
                FilterPredicate.gt("col", 50L), rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type INT32"
                        + "; given filter predicate type INT64 is incompatible");
    }

    @Test
    void floatPredicateOnDoubleColumnThrows() {
        RowGroup rg = createDoubleRowGroup(0.0, 100.0);
        FileSchema schema = createDoubleSchema();
        assertThatThrownBy(() -> canDropRowGroup(
                FilterPredicate.gt("col", 50.0f), rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type DOUBLE"
                        + "; given filter predicate type FLOAT is incompatible");
    }

    @Test
    void stringPredicateOnIntColumnThrows() {
        RowGroup rg = createIntRowGroup(0, 100);
        FileSchema schema = createIntSchema();
        assertThatThrownBy(() -> canDropRowGroup(
                FilterPredicate.eq("col", "hello"), rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type INT32"
                        + "; given filter predicate type BYTE_ARRAY is incompatible");
    }

    @Test
    void booleanPredicateOnLongColumnThrows() {
        RowGroup rg = createLongRowGroup(0, 100);
        FileSchema schema = createLongSchema();
        assertThatThrownBy(() -> canDropRowGroup(
                FilterPredicate.eq("col", true), rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type INT64"
                        + "; given filter predicate type BOOLEAN is incompatible");
    }

    @Test
    void intInPredicateOnStringColumnThrows() {
        RowGroup rg = createBinaryRowGroup(new byte[]{0x41}, new byte[]{0x5A});
        FileSchema schema = createBinarySchema();
        assertThatThrownBy(() -> canDropRowGroup(
                FilterPredicate.in("col", 1, 2, 3), rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type BYTE_ARRAY"
                        + "; given filter predicate type INT32 is incompatible");
    }

    @Test
    void typeMismatchInAndThrows() {
        RowGroup rg = createBinaryRowGroup(new byte[]{0x41}, new byte[]{0x7A});
        FileSchema schema = createBinarySchema();
        // First predicate matches (cannot drop), so And evaluates the second — which has wrong type
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.eq("col", "M"),
                FilterPredicate.eq("col", 42));
        assertThatThrownBy(() -> canDropRowGroup(filter, rg, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type BYTE_ARRAY"
                        + "; given filter predicate type INT32 is incompatible");
    }

    @Test
    void predicateOnUnknownColumnThrowsAtResolve() {
        FileSchema schema = createIntSchema();
        // Unknown column => throws at resolve time
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("nonexistent", 42), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    // ==================== Operator.invert() ====================

    @ParameterizedTest(name = "{0}.invert() = {1}")
    @MethodSource
    void testOperatorInvert(FilterPredicate.Operator op, FilterPredicate.Operator expected) {
        assertThat(op.invert()).isEqualTo(expected);
    }

    static Stream<Arguments> testOperatorInvert() {
        return Stream.of(
                Arguments.of(FilterPredicate.Operator.EQ,     FilterPredicate.Operator.NOT_EQ),
                Arguments.of(FilterPredicate.Operator.NOT_EQ, FilterPredicate.Operator.EQ),
                Arguments.of(FilterPredicate.Operator.LT,     FilterPredicate.Operator.GT_EQ),
                Arguments.of(FilterPredicate.Operator.LT_EQ,  FilterPredicate.Operator.GT),
                Arguments.of(FilterPredicate.Operator.GT,     FilterPredicate.Operator.LT_EQ),
                Arguments.of(FilterPredicate.Operator.GT_EQ,  FilterPredicate.Operator.LT)
        );
    }

    @ParameterizedTest(name = "{0}.invert().invert() = {0}")
    @EnumSource(FilterPredicate.Operator.class)
    void testOperatorDoubleInvertIsIdentity(FilterPredicate.Operator op) {
        assertThat(op.invert().invert()).isEqualTo(op);
    }

    // ==================== NOT pushdown via inversion ====================

    @ParameterizedTest(name = "NOT({0} col {1}) on int [{2},{3}] → canDrop={4}")
    @MethodSource
    void testNotInversionOnIntRowGroup(FilterPredicate.Operator op, int value, int min, int max, boolean canDrop) {
        RowGroup rg = createIntRowGroup(min, max);
        FileSchema schema = createIntSchema();

        FilterPredicate inner = switch (op) {
            case EQ -> FilterPredicate.eq("col", value);
            case NOT_EQ -> FilterPredicate.notEq("col", value);
            case LT -> FilterPredicate.lt("col", value);
            case LT_EQ -> FilterPredicate.ltEq("col", value);
            case GT -> FilterPredicate.gt("col", value);
            case GT_EQ -> FilterPredicate.gtEq("col", value);
        };

        FilterPredicate notPredicate = FilterPredicate.not(inner);
        assertThat(canDropRowGroup(notPredicate, rg, schema)).isEqualTo(canDrop);
    }

    static Stream<Arguments> testNotInversionOnIntRowGroup() {
        // Row group: min=10, max=20
        return Stream.of(
                // NOT(GT(25)) → LT_EQ(25): min(10) > 25? no → can't drop
                Arguments.of(FilterPredicate.Operator.GT, 25, 10, 20, false),
                // NOT(GT(5)) → LT_EQ(5): min(10) > 5? yes → can drop
                Arguments.of(FilterPredicate.Operator.GT, 5, 10, 20, true),
                // NOT(LT(5)) → GT_EQ(5): max(20) < 5? no → can't drop
                Arguments.of(FilterPredicate.Operator.LT, 5, 10, 20, false),
                // NOT(LT(25)) → GT_EQ(25): max(20) < 25? yes → can drop
                Arguments.of(FilterPredicate.Operator.LT, 25, 10, 20, true),
                // NOT(EQ(15)) → NOT_EQ(15): min==max==15? no (10≠20) → can't drop
                Arguments.of(FilterPredicate.Operator.EQ, 15, 10, 20, false),
                // NOT(EQ(15)) on single-value range → NOT_EQ(15): min==max==15? yes → can drop
                Arguments.of(FilterPredicate.Operator.EQ, 15, 15, 15, true),
                // NOT(NOT_EQ(15)) → EQ(15): 15 < 10? no, 15 > 20? no → can't drop
                Arguments.of(FilterPredicate.Operator.NOT_EQ, 15, 10, 20, false),
                // NOT(NOT_EQ(5)) → EQ(5): 5 < 10? yes → can drop
                Arguments.of(FilterPredicate.Operator.NOT_EQ, 5, 10, 20, true),
                // NOT(GT_EQ(25)) → LT(25): min(10) >= 25? no → can't drop
                Arguments.of(FilterPredicate.Operator.GT_EQ, 25, 10, 20, false),
                // NOT(GT_EQ(5)) → LT(5): min(10) >= 5? yes → can drop
                Arguments.of(FilterPredicate.Operator.GT_EQ, 5, 10, 20, true),
                // NOT(LT_EQ(5)) → GT(5): max(20) <= 5? no → can't drop
                Arguments.of(FilterPredicate.Operator.LT_EQ, 5, 10, 20, false),
                // NOT(LT_EQ(25)) → GT(25): max(20) <= 25? yes → can drop
                Arguments.of(FilterPredicate.Operator.LT_EQ, 25, 10, 20, true)
        );
    }

    @Test
    void testNotOnCompoundPredicateIsConservative() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // NOT(AND(GT(5), LT(25))) → OR(LT_EQ(5), GT_EQ(25)) via De Morgan
        // LT_EQ(5): min(10) > 5 → can drop; GT_EQ(25): max(20) < 25 → can drop
        // OR: both drop → can drop
        FilterPredicate notAnd = FilterPredicate.not(FilterPredicate.and(
                FilterPredicate.gt("col", 5),
                FilterPredicate.lt("col", 25)));
        assertThat(canDropRowGroup(notAnd, rg, schema)).isTrue();

        // NOT(OR(LT(5), GT(25))) → AND(GT_EQ(5), LT_EQ(25)) via De Morgan
        // GT_EQ(5): max(20) < 5? no; LT_EQ(25): min(10) > 25? no
        // AND: neither drops → can't drop
        FilterPredicate notOr = FilterPredicate.not(FilterPredicate.or(
                FilterPredicate.lt("col", 5),
                FilterPredicate.gt("col", 25)));
        assertThat(canDropRowGroup(notOr, rg, schema)).isFalse();

        // NOT(OR(LT(5), GT(15))) → AND(GT_EQ(5), LT_EQ(15))
        // GT_EQ(5): max(20) < 5? no; LT_EQ(15): min(10) > 15? no
        // AND: neither drops → can't drop (correct: range [10,20] has values both ≥5 and ≤15)
        FilterPredicate notOr2 = FilterPredicate.not(FilterPredicate.or(
                FilterPredicate.lt("col", 5),
                FilterPredicate.gt("col", 15)));
        assertThat(canDropRowGroup(notOr2, rg, schema)).isFalse();
    }

    @Test
    void testNotOnInPredicateExpandsToAndNotEq() {
        FileSchema schema = createIntSchema();

        // NOT(IN(1, 2, 3)) → AND(NOT_EQ(1), NOT_EQ(2), NOT_EQ(3))
        // Row group [10, 20]: none of 1, 2, 3 have min==max==value, so can't drop
        RowGroup rg = createIntRowGroup(10, 20);
        FilterPredicate notIn = FilterPredicate.not(FilterPredicate.in("col", 1, 2, 3));
        assertThat(canDropRowGroup(notIn, rg, schema)).isFalse();

        // Row group [5, 5] (single value): NOT_EQ(5) drops when min==max==5
        // NOT(IN(5, 10)) → AND(NOT_EQ(5), NOT_EQ(10))
        // NOT_EQ(5): min==max==5 → can drop. AND short-circuits.
        RowGroup rgSingle = createIntRowGroup(5, 5);
        FilterPredicate notInSingle = FilterPredicate.not(FilterPredicate.in("col", 5, 10));
        assertThat(canDropRowGroup(notInSingle, rgSingle, schema)).isTrue();
    }


    @Test
    void testNotOnIsNullInvertsToIsNotNull() {
        FileSchema schema = createIntSchema();

        // All rows are null → NOT(isNull) → isNotNull → can drop (all are null)
        RowGroup rgAllNulls = createRowGroupWithNullCount(PhysicalType.INT32, 100L, 100);
        assertThat(canDropRowGroup(
                FilterPredicate.not(FilterPredicate.isNull("col")), rgAllNulls, schema)).isTrue();

        // No nulls → NOT(isNull) → isNotNull → cannot drop (all are non-null)
        RowGroup rgNoNulls = createRowGroupWithNullCount(PhysicalType.INT32, 0L, 100);
        assertThat(canDropRowGroup(
                FilterPredicate.not(FilterPredicate.isNull("col")), rgNoNulls, schema)).isFalse();
    }

    @Test
    void testNotOnIsNotNullInvertsToIsNull() {
        FileSchema schema = createIntSchema();

        // No nulls → NOT(isNotNull) → isNull → can drop (no nulls exist)
        RowGroup rgNoNulls = createRowGroupWithNullCount(PhysicalType.INT32, 0L, 100);
        assertThat(canDropRowGroup(
                FilterPredicate.not(FilterPredicate.isNotNull("col")), rgNoNulls, schema)).isTrue();

        // All nulls → NOT(isNotNull) → isNull → cannot drop (all are null)
        RowGroup rgAllNulls = createRowGroupWithNullCount(PhysicalType.INT32, 100L, 100);
        assertThat(canDropRowGroup(
                FilterPredicate.not(FilterPredicate.isNotNull("col")), rgAllNulls, schema)).isFalse();
    }

    @Test
    void testDoubleNotIsEquivalentToOriginal() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // NOT(NOT(GT(25))) should behave like GT(25)
        FilterPredicate gt25 = FilterPredicate.gt("col", 25);
        FilterPredicate doubleNot = FilterPredicate.not(FilterPredicate.not(gt25));

        assertThat(canDropRowGroup(gt25, rg, schema))
                .isEqualTo(canDropRowGroup(doubleNot, rg, schema));
    }

    @Test
    void testDeeplyNestedNotWithDeMorgan() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // NOT(AND(OR(GT(5), LT(2)), NOT(EQ(3))))
        // De Morgan on AND → OR(NOT(OR(GT(5), LT(2))), NOT(NOT(EQ(3))))
        //   NOT(OR(GT(5), LT(2))) → AND(LT_EQ(5), GT_EQ(2))
        //     LT_EQ(5): min(10) > 5 → can drop. AND short-circuits.
        //   NOT(NOT(EQ(3))) → EQ(3): 3 < 10 → can drop.
        // OR: both drop → can drop
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.and(
                FilterPredicate.or(
                        FilterPredicate.gt("col", 5),
                        FilterPredicate.lt("col", 2)),
                FilterPredicate.not(FilterPredicate.eq("col", 3))));
        assertThat(canDropRowGroup(filter, rg, schema)).isTrue();
    }

    @Test
    void testNotAndWithInChildFallsBackToConservative() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // NOT(AND(GT(25), IN(1,2,3))) → OR(LT_EQ(25), NOT(IN(1,2,3)))
        // NOT(IN) returns null → entire OR returns null → conservative (false)
        FilterPredicate filter = FilterPredicate.not(FilterPredicate.and(
                FilterPredicate.gt("col", 25),
                FilterPredicate.in("col", 1, 2, 3)));
        assertThat(canDropRowGroup(filter, rg, schema)).isFalse();
    }

    @Test
    void testNotOrEquivalenceWithDeMorgan() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // NOT(OR(GT(25), LT(5))) should behave the same as AND(LT_EQ(25), GT_EQ(5))
        FilterPredicate notOr = FilterPredicate.not(FilterPredicate.or(
                FilterPredicate.gt("col", 25),
                FilterPredicate.lt("col", 5)));
        FilterPredicate deMorganEquivalent = FilterPredicate.and(
                FilterPredicate.ltEq("col", 25),
                FilterPredicate.gtEq("col", 5));

        assertThat(canDropRowGroup(notOr, rg, schema))
                .isEqualTo(canDropRowGroup(deMorganEquivalent, rg, schema));
    }

    @Test
    void testRowGroupIsNotDroppedWithIntersectingBoundingBox() {
        FileSchema schema = createGeometrySchema();
        RowGroup rg = createGeospatialRowGroup(2.0, 10.0, -4.0, 6.0);

        // query bbox totally covered by chunk bbox
        FilterPredicate totallyCovered = FilterPredicate.intersects("col", 3.0, 0.0, 8.0, 4.0);
        assertThat(canDropRowGroup(totallyCovered, rg, schema)).isFalse();

        // query bbox totally covering chunk bbox
        FilterPredicate totallyCovering = FilterPredicate.intersects("col", 0.0, -10.0, 15.0, 10.0);
        assertThat(canDropRowGroup(totallyCovering, rg, schema)).isFalse();

        // query bbox partially covering chunk bbox on left
        FilterPredicate partialLeft = FilterPredicate.intersects("col", -5.0, 0.0, 5.0, 3.0);
        assertThat(canDropRowGroup(partialLeft, rg, schema)).isFalse();

        // query bbox partially covering chunk bbox on right
        FilterPredicate partialRight = FilterPredicate.intersects("col", 8.0, 0.0, 15.0, 3.0);
        assertThat(canDropRowGroup(partialRight, rg, schema)).isFalse();

        // query bbox partially covering chunk bbox from below
        FilterPredicate partialBelow = FilterPredicate.intersects("col", 4.0, -10.0, 8.0, 0.0);
        assertThat(canDropRowGroup(partialBelow, rg, schema)).isFalse();

        // query bbox partially covering chunk bbox from above
        FilterPredicate partialAbove =  FilterPredicate.intersects("col", 4.0, 3.0, 8.0, 10.0);
        assertThat(canDropRowGroup(partialAbove, rg, schema)).isFalse();

        // query bbox partially covering chunk bbox on a corner
        FilterPredicate  corner = FilterPredicate.intersects("col", 8.0, 4.0, 15.0, 10.0);
        assertThat(canDropRowGroup(corner, rg, schema)).isFalse();
    }

    @Test
    void testRowGroupIsNotDroppedWhenGeospatialstatsAreNotAvailable() {
        FileSchema schema = createGeometrySchema();
        FilterPredicate query = FilterPredicate.intersects("col", 3.0, 0.0, 8.0, 4.0);

        // geospatial stats metadata mot available
        RowGroup rgWithNoGeostats = createRowGroupWithNoGeoStats();
        assertThat(canDropRowGroup(query, rgWithNoGeostats, schema)).isFalse();

        // bounding box absent in metadata
        RowGroup rgWithNoBbox = createRowGroupWithNullBbox();
        assertThat(canDropRowGroup(query, rgWithNoBbox, schema)).isFalse();
    }

    @Test
    void testRowGroupIsDroppedWithNonIntersectingBoundingBox() {
        FileSchema schema = createGeometrySchema();
        RowGroup rg = createGeospatialRowGroup(2.0, 10.0, -4.0, 6.0);

        // query bbox below chunk bbox
        FilterPredicate below = FilterPredicate.intersects("col",
                2.5, -10.0, 9.3, -6.0);
        assertThat(canDropRowGroup(below, rg, schema)).isTrue();

        // query bbox to left of chunk bbox
        FilterPredicate left = FilterPredicate.intersects("col",
                -5.0, -1.0, 0.0, 3.0);
        assertThat(canDropRowGroup(left, rg, schema)).isTrue();

        // query bbox above chunk bbox
        FilterPredicate above = FilterPredicate.intersects("col",
                4.0, 7.0, 8.0, 10.0);
        assertThat(canDropRowGroup(above, rg, schema)).isTrue();

        // query bbox to right of chunk bbox
        FilterPredicate right = FilterPredicate.intersects("col",
                11.0, -1.0, 15.0, 3.0);
        assertThat(canDropRowGroup(right, rg, schema)).isTrue();
    }

    @Test
    void testAntimeridianWrapping() {
        FileSchema schema = createGeometrySchema();

        // chunk wraps, query doesn't with it's end to right of chunk's start
        RowGroup rgWrapping = createGeospatialRowGroup(170.0, -170.0, -10.0, 10.0);
        FilterPredicate notWrapping = FilterPredicate.intersects("col", 160.0, -5.0, 180.0, 5.0);
        assertThat(canDropRowGroup(notWrapping, rgWrapping, schema)).isFalse();

        // query wraps, chunk doesn't with it's end to right of query's start
        RowGroup rgNotWrapping = createGeospatialRowGroup(160.0, 180.0, -10.0, 10.0);
        FilterPredicate wrapping = FilterPredicate.intersects("col", 170.0, -5.0, -170.0, 5.0);
        assertThat(canDropRowGroup(wrapping, rgNotWrapping, schema)).isFalse();

        // both chunk and query wrap, with query's x-axis expanse entirely within chunk's x-axis expanse
        FilterPredicate wrappingWithinChunk = FilterPredicate.intersects("col", 160.0, -5.0, -160.0, 5.0);
        assertThat(canDropRowGroup(wrappingWithinChunk, rgWrapping, schema)).isFalse();

        // chunk wraps, query doesn't with it's start and end both to left of chunk's start and to right of chunk's end
        FilterPredicate outsideChunk = FilterPredicate.intersects("col", 10.0, -5.0, 20.0, 5.0);
        assertThat(canDropRowGroup(outsideChunk, rgWrapping, schema)).isTrue();
    }

    @Test
    void testIntersectingBoundingBoxIsNotDroppedForGeography() {
        RowGroup rg = createGeospatialRowGroup(2.0, 10.0, -4.0, 6.0);
        FileSchema schema = createGeographySchema();

        FilterPredicate filter = FilterPredicate.intersects("col", 3.0, 0.0, 8.0, 4.0);
        assertThat(canDropRowGroup(filter, rg, schema)).isFalse();
    }

    @Test
    void testIntersectsComposedWithAnd() {
        RowGroup rg = createGeospatialRowGroup(2.0, 10.0, -4.0, 6.0);
        FileSchema schema = createGeographySchema();

        // one of the spatial predicate doesn't intersect, so drop
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.intersects("col", 11.0, -1.0, 15.0, 3.0),  // non-intersecting
                FilterPredicate.intersects("col", 3.0, 0.0, 8.0, 4.0)       // intersecting
        );
        assertThat(canDropRowGroup(filter, rg, schema)).isTrue();
    }

    @Test
    void testIntersectsComposedWithOr() {
        RowGroup rg = createGeospatialRowGroup(2.0, 10.0, -4.0, 6.0);
        FileSchema schema = createGeometrySchema();

        // only one intersects, don't drop
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.intersects("col", 11.0, -1.0, 15.0, 3.0),  // non-intersecting
                FilterPredicate.intersects("col", 3.0, 0.0, 8.0, 4.0)       // intersecting
        );
        assertThat(canDropRowGroup(filter, rg, schema)).isFalse();
    }

    // ==================== Helpers ====================

    private static RowGroup createIntRowGroup(int min, int max) {
        byte[] minBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(min).array();
        byte[] maxBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(max).array();
        return createRowGroupWithStats(PhysicalType.INT32, minBytes, maxBytes);
    }

    private static RowGroup createLongRowGroup(long min, long max) {
        byte[] minBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(min).array();
        byte[] maxBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(max).array();
        return createRowGroupWithStats(PhysicalType.INT64, minBytes, maxBytes);
    }

    private static RowGroup createFloatRowGroup(float min, float max) {
        byte[] minBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(min).array();
        byte[] maxBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(max).array();
        return createRowGroupWithStats(PhysicalType.FLOAT, minBytes, maxBytes);
    }

    private static RowGroup createDoubleRowGroup(double min, double max) {
        byte[] minBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(min).array();
        byte[] maxBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(max).array();
        return createRowGroupWithStats(PhysicalType.DOUBLE, minBytes, maxBytes);
    }

    private static RowGroup createBooleanRowGroup(boolean min, boolean max) {
        byte[] minBytes = {(byte) (min ? 1 : 0)};
        byte[] maxBytes = {(byte) (max ? 1 : 0)};
        return createRowGroupWithStats(PhysicalType.BOOLEAN, minBytes, maxBytes);
    }

    private static RowGroup createBinaryRowGroup(byte[] min, byte[] max) {
        return createRowGroupWithStats(PhysicalType.BYTE_ARRAY, min, max);
    }

    private static RowGroup createUuidRowGroup(UUID min, UUID max) {
        return createRowGroupWithStats(PhysicalType.FIXED_LEN_BYTE_ARRAY,
                uuidBytes(min), uuidBytes(max));
    }

    private static byte[] uuidBytes(UUID uuid) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    private static RowGroup createRowGroupWithStats(PhysicalType type, byte[] min, byte[] max) {
        Statistics stats = new Statistics(min, max, 0L, null, false);
        ColumnMetaData cmd = new ColumnMetaData(
                type, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(), 0, null, stats, null);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 1000, 100);
    }

    private static RowGroup createRowGroupWithNullCount(PhysicalType type, Long nullCount, long numRows) {
        Statistics stats = new Statistics(null, null, nullCount, null, false);
        ColumnMetaData cmd = new ColumnMetaData(
                type, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(), 0, null, stats, null);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 1000, numRows);
    }

    private static RowGroup createRowGroupWithoutStatistics() {
        ColumnMetaData cmd = new ColumnMetaData(
                PhysicalType.INT32, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(), 0, null, null, null);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 1000, 100);
    }

    private static RowGroup createGeospatialRowGroup(double xmin, double xmax, double ymin, double ymax) {
        BoundingBox bbox = new BoundingBox(xmin, xmax, ymin, ymax, null, null, null, null);
        GeospatialStatistics geospatialStatistics = new GeospatialStatistics(bbox, List.of(1));
        return new RowGroup(List.of(createGeostatsColumnChunk(geospatialStatistics)), 1000, 100);
    }

    private static RowGroup createRowGroupWithNoGeoStats() {
        return new RowGroup(List.of(createGeostatsColumnChunk(null)), 1000, 100);
    }

    private static RowGroup createRowGroupWithNullBbox() {
        GeospatialStatistics geospatialStatistics = new GeospatialStatistics(null, List.of(1));
        return new RowGroup(List.of(createGeostatsColumnChunk(geospatialStatistics)), 1000, 100);
    }

    private static ColumnChunk createGeostatsColumnChunk(GeospatialStatistics geospatialStatistics) {
        ColumnMetaData cmd = new ColumnMetaData(
                PhysicalType.BYTE_ARRAY, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(), 0, null, null, geospatialStatistics);
        return new ColumnChunk(cmd, null, null, null, null);
    }

    private static FileSchema createIntSchema() {
        return createSchemaForType(PhysicalType.INT32);
    }

    private static FileSchema createLongSchema() {
        return createSchemaForType(PhysicalType.INT64);
    }

    private static FileSchema createFloatSchema() {
        return createSchemaForType(PhysicalType.FLOAT);
    }

    private static FileSchema createDoubleSchema() {
        return createSchemaForType(PhysicalType.DOUBLE);
    }

    private static FileSchema createBooleanSchema() {
        return createSchemaForType(PhysicalType.BOOLEAN);
    }

    private static FileSchema createBinarySchema() {
        return createSchemaForType(PhysicalType.BYTE_ARRAY);
    }

    private static FileSchema createUuidSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement("col", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                RepetitionType.REQUIRED, null, null, null, null, null, new LogicalType.UuidType());
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema createGeometrySchema() {
        return createSchemaForType(PhysicalType.BYTE_ARRAY, new LogicalType.GeometryType("OGC:CRS84"));
    }

    private static FileSchema createGeographySchema() {
        return createSchemaForType(PhysicalType.BYTE_ARRAY, new LogicalType.GeographyType("OGC:CRS84", LogicalType.EdgeInterpolationAlgorithm.SPHERICAL));
    }

    private static FileSchema createSchemaForType(PhysicalType type) {
        // Root element + one column
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement("col", type, null, RepetitionType.REQUIRED, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema createSchemaForType(PhysicalType type, LogicalType logicalType) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement("col", type, null, RepetitionType.REQUIRED, null, null, null, null, null, logicalType);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    /// Helper that resolves a FilterPredicate and evaluates it against a row group.
    /// This mirrors the production code path: resolve first, then evaluate.
    private static boolean canDropRowGroup(FilterPredicate filter, RowGroup rg, FileSchema schema) {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        return RowGroupFilterEvaluator.canDropRowGroup(resolved, rg);
    }
}
