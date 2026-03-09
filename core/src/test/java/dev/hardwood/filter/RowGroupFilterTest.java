/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.filter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RowGroupFilter}, {@link StatisticsConverter}, and {@link Statistics}.
 */
class RowGroupFilterTest {

    @Nested
    class IntFilterTests {

        @Test
        void eqDropsWhenValueBelowRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 5).canDrop(rg, schema)).isTrue();
        }

        @Test
        void eqKeepsWhenValueInRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 50).canDrop(rg, schema)).isFalse();
        }

        @Test
        void eqDropsWhenValueAboveRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 200).canDrop(rg, schema)).isTrue();
        }

        @Test
        void eqKeepsWhenValueIsMinBoundary() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 10).canDrop(rg, schema)).isFalse();
        }

        @Test
        void eqKeepsWhenValueIsMaxBoundary() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 100).canDrop(rg, schema)).isFalse();
        }

        @Test
        void eqKeepsWhenSingleValueMatch() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(42), intBytes(42));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 42).canDrop(rg, schema)).isFalse();
        }

        @Test
        void gtDropsWhenMaxEqualsValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gt("col", 100).canDrop(rg, schema)).isTrue();
        }

        @Test
        void gtDropsWhenMaxBelowValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gt("col", 200).canDrop(rg, schema)).isTrue();
        }

        @Test
        void gtKeepsWhenMaxAboveValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gt("col", 50).canDrop(rg, schema)).isFalse();
        }

        @Test
        void gtKeepsWhenMaxAboveValueByOne() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gt("col", 99).canDrop(rg, schema)).isFalse();
        }

        @Test
        void gtEqDropsWhenMaxBelowValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gtEq("col", 101).canDrop(rg, schema)).isTrue();
        }

        @Test
        void gtEqKeepsWhenMaxEqualsValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gtEq("col", 100).canDrop(rg, schema)).isFalse();
        }

        @Test
        void ltDropsWhenMinEqualsValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.lt("col", 10).canDrop(rg, schema)).isTrue();
        }

        @Test
        void ltDropsWhenMinAboveValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.lt("col", 5).canDrop(rg, schema)).isTrue();
        }

        @Test
        void ltKeepsWhenMinBelowValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.lt("col", 50).canDrop(rg, schema)).isFalse();
        }

        @Test
        void ltEqDropsWhenMinAboveValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.ltEq("col", 9).canDrop(rg, schema)).isTrue();
        }

        @Test
        void ltEqKeepsWhenMinEqualsValue() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.ltEq("col", 10).canDrop(rg, schema)).isFalse();
        }

        @Test
        void notEqDropsWhenSingleValueMatch() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(42), intBytes(42));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.notEq("col", 42).canDrop(rg, schema)).isTrue();
        }

        @Test
        void notEqKeepsWhenRangeContainsOtherValues() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.notEq("col", 42).canDrop(rg, schema)).isFalse();
        }

        @Test
        void notEqKeepsWhenValueOutOfRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.notEq("col", 200).canDrop(rg, schema)).isFalse();
        }
    }

    @Nested
    class NegativeValueTests {

        @Test
        void eqWithNegativeRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(-100), intBytes(-10));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", -50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("col", 0).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.eq("col", -200).canDrop(rg, schema)).isTrue();
        }

        @Test
        void gtWithNegativeRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(-100), intBytes(-10));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.gt("col", -10).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.gt("col", -50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gt("col", -110).canDrop(rg, schema)).isFalse();
        }

        @Test
        void ltWithNegativeRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(-100), intBytes(-10));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.lt("col", -100).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.lt("col", -50).canDrop(rg, schema)).isFalse();
        }

        @Test
        void rangeSpanningZero() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(-50), intBytes(50));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 0).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gt("col", -60).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.lt("col", 60).canDrop(rg, schema)).isFalse();
        }
    }

    @Nested
    class Int64FilterTests {

        @Test
        void eqInt64() {
            RowGroup rg = makeRowGroup(PhysicalType.INT64, longBytes(1000000L), longBytes(9999999L));
            FileSchema schema = makeSchema("ts", PhysicalType.INT64, null);

            assertThat(RowGroupFilter.eq("ts", 5000000L).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("ts", 100L).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.eq("ts", 99999999L).canDrop(rg, schema)).isTrue();
        }

        @Test
        void gtInt64() {
            RowGroup rg = makeRowGroup(PhysicalType.INT64, longBytes(1000000L), longBytes(9999999L));
            FileSchema schema = makeSchema("ts", PhysicalType.INT64, null);

            assertThat(RowGroupFilter.gt("ts", 9999999L).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.gt("ts", 5000000L).canDrop(rg, schema)).isFalse();
        }

        @Test
        void ltInt64WithLargeValues() {
            RowGroup rg = makeRowGroup(PhysicalType.INT64, longBytes(Long.MAX_VALUE - 100), longBytes(Long.MAX_VALUE));
            FileSchema schema = makeSchema("ts", PhysicalType.INT64, null);

            assertThat(RowGroupFilter.lt("ts", Long.MAX_VALUE - 100).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.lt("ts", Long.MAX_VALUE).canDrop(rg, schema)).isFalse();
        }

        @Test
        void int64NegativeRange() {
            RowGroup rg = makeRowGroup(PhysicalType.INT64, longBytes(-1000000L), longBytes(-100L));
            FileSchema schema = makeSchema("ts", PhysicalType.INT64, null);

            assertThat(RowGroupFilter.eq("ts", -500L).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gt("ts", -100L).canDrop(rg, schema)).isTrue();
        }
    }

    @Nested
    class DoubleFilterTests {

        @Test
        void eqDropsWhenOutOfRange() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1.5), doubleBytes(9.9));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.eq("fare", 15.0).canDrop(rg, schema)).isTrue();
        }

        @Test
        void eqKeepsWhenInRange() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1.5), doubleBytes(9.9));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.eq("fare", 5.0).canDrop(rg, schema)).isFalse();
        }

        @Test
        void gtDouble() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1.5), doubleBytes(9.9));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.gt("fare", 9.9).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.gt("fare", 5.0).canDrop(rg, schema)).isFalse();
        }

        @Test
        void ltDouble() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1.5), doubleBytes(9.9));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.lt("fare", 1.5).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.lt("fare", 5.0).canDrop(rg, schema)).isFalse();
        }

        @Test
        void gtEqDouble() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1.5), doubleBytes(9.9));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.gtEq("fare", 10.0).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.gtEq("fare", 9.9).canDrop(rg, schema)).isFalse();
        }

        @Test
        void ltEqDouble() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1.5), doubleBytes(9.9));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.ltEq("fare", 1.4).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.ltEq("fare", 1.5).canDrop(rg, schema)).isFalse();
        }

        @Test
        void notEqDouble() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(3.14), doubleBytes(3.14));
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.notEq("fare", 3.14).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.notEq("fare", 2.71).canDrop(rg, schema)).isFalse();
        }

        @Test
        void verySmallDoubleRange() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(1e-10), doubleBytes(1e-9));
            FileSchema schema = makeDoubleSchema("tiny");

            assertThat(RowGroupFilter.eq("tiny", 5e-10).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("tiny", 1.0).canDrop(rg, schema)).isTrue();
        }

        @Test
        void negativeDoubleRange() {
            RowGroup rg = makeRowGroup(PhysicalType.DOUBLE, doubleBytes(-99.9), doubleBytes(-0.1));
            FileSchema schema = makeDoubleSchema("temp");

            assertThat(RowGroupFilter.eq("temp", -50.0).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("temp", 0.0).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.gt("temp", -0.1).canDrop(rg, schema)).isTrue();
        }
    }

    @Nested
    class FloatFilterTests {

        @Test
        void floatEqFilter() {
            RowGroup rg = makeRowGroup(PhysicalType.FLOAT, floatBytes(1.0f), floatBytes(10.0f));
            FileSchema schema = makeSchema("f", PhysicalType.FLOAT, null);

            assertThat(RowGroupFilter.eq("f", 5.0).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("f", 20.0).canDrop(rg, schema)).isTrue();
        }

        @Test
        void floatGtFilter() {
            RowGroup rg = makeRowGroup(PhysicalType.FLOAT, floatBytes(1.0f), floatBytes(10.0f));
            FileSchema schema = makeSchema("f", PhysicalType.FLOAT, null);

            assertThat(RowGroupFilter.gt("f", 10.0).canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.gt("f", 5.0).canDrop(rg, schema)).isFalse();
        }
    }

    @Nested
    class StringFilterTests {

        @Test
        void eqDropsOutOfRange() {
            RowGroup rg = makeStringRowGroup("apple", "mango");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "peach").canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.eq("name", "banana").canDrop(rg, schema)).isFalse();
        }

        @Test
        void eqDropsBelowRange() {
            RowGroup rg = makeStringRowGroup("mango", "peach");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "apple").canDrop(rg, schema)).isTrue();
        }

        @Test
        void eqKeepsAtBoundaries() {
            RowGroup rg = makeStringRowGroup("apple", "mango");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "apple").canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("name", "mango").canDrop(rg, schema)).isFalse();
        }

        @Test
        void gtString() {
            RowGroup rg = makeStringRowGroup("apple", "mango");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "peach").canDrop(rg, schema)).isTrue();
        }

        @Test
        void ltString() {
            RowGroup rg = makeStringRowGroup("mango", "peach");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "apple").canDrop(rg, schema)).isTrue();
        }

        @Test
        void stringWithPrefixComparison() {
            RowGroup rg = makeStringRowGroup("abc", "abcdef");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "abcd").canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("name", "abb").canDrop(rg, schema)).isTrue();
            assertThat(RowGroupFilter.eq("name", "abd").canDrop(rg, schema)).isTrue();
        }

        @Test
        void emptyStringBounds() {
            RowGroup rg = makeStringRowGroup("", "zzz");
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "anything").canDrop(rg, schema)).isFalse();
        }
    }

    @Nested
    class CompoundFilterTests {

        @Test
        void andDropsWhenAnyChildDrops() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            RowGroupFilter filter = RowGroupFilter.and(
                    RowGroupFilter.eq("col", 5),
                    RowGroupFilter.gt("col", 50));
            assertThat(filter.canDrop(rg, schema)).isTrue();
        }

        @Test
        void andKeepsWhenNoChildDrops() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            RowGroupFilter filter = RowGroupFilter.and(
                    RowGroupFilter.gt("col", 5),
                    RowGroupFilter.lt("col", 200));
            assertThat(filter.canDrop(rg, schema)).isFalse();
        }

        @Test
        void andWithThreeFilters() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            RowGroupFilter filter = RowGroupFilter.and(
                    RowGroupFilter.gt("col", 5),
                    RowGroupFilter.lt("col", 200),
                    RowGroupFilter.eq("col", 50));
            assertThat(filter.canDrop(rg, schema)).isFalse();
        }

        @Test
        void orDropsOnlyWhenAllChildrenDrop() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            RowGroupFilter filter = RowGroupFilter.or(
                    RowGroupFilter.eq("col", 5),
                    RowGroupFilter.eq("col", 200));
            assertThat(filter.canDrop(rg, schema)).isTrue();
        }

        @Test
        void orKeepsWhenAnyChildKeeps() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            RowGroupFilter filter = RowGroupFilter.or(
                    RowGroupFilter.eq("col", 5),
                    RowGroupFilter.eq("col", 50));
            assertThat(filter.canDrop(rg, schema)).isFalse();
        }

        @Test
        void orWithThreeFilters() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            RowGroupFilter filter = RowGroupFilter.or(
                    RowGroupFilter.eq("col", 5),
                    RowGroupFilter.eq("col", 200),
                    RowGroupFilter.eq("col", 999));
            assertThat(filter.canDrop(rg, schema)).isTrue();
        }

        @Test
        void nestedAndOr() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            // AND(OR(eq(5), eq(50)), gt(5)) -> OR keeps because eq(50) keeps -> AND checks gt(5) which keeps
            RowGroupFilter filter = RowGroupFilter.and(
                    RowGroupFilter.or(RowGroupFilter.eq("col", 5), RowGroupFilter.eq("col", 50)),
                    RowGroupFilter.gt("col", 5));
            assertThat(filter.canDrop(rg, schema)).isFalse();
        }

        @Test
        void nestedOrWithAllDropping() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            // AND(eq(5), OR(eq(200), eq(300))) -> eq(5) drops -> AND drops
            RowGroupFilter filter = RowGroupFilter.and(
                    RowGroupFilter.eq("col", 5),
                    RowGroupFilter.or(RowGroupFilter.eq("col", 200), RowGroupFilter.eq("col", 300)));
            assertThat(filter.canDrop(rg, schema)).isTrue();
        }

        @Test
        void notNeverDropsConservatively() {
            RowGroup rg = makeRowGroup(PhysicalType.INT32, intBytes(10), intBytes(100));
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.not(RowGroupFilter.gt("col", 50)).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.not(RowGroupFilter.eq("col", 50)).canDrop(rg, schema)).isFalse();
        }

        @Test
        void compoundColumnName() {
            RowGroupFilter and = RowGroupFilter.and(RowGroupFilter.eq("a", 1), RowGroupFilter.eq("b", 2));
            RowGroupFilter or = RowGroupFilter.or(RowGroupFilter.eq("a", 1));
            RowGroupFilter not = RowGroupFilter.not(RowGroupFilter.eq("c", 3));

            assertThat(and.columnName()).isNull();
            assertThat(or.columnName()).isNull();
            assertThat(not.columnName()).isEqualTo("c");
        }
    }

    @Nested
    class NoStatisticsTests {

        @Test
        void noStatisticsNeverDropsAnyOperator() {
            Statistics noStats = new Statistics(null, null, -1, -1, false);
            RowGroup rg = makeRowGroupWithStats(PhysicalType.INT32, noStats);
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.notEq("col", 50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gt("col", 50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gtEq("col", 50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.lt("col", 50).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.ltEq("col", 50).canDrop(rg, schema)).isFalse();
        }

        @Test
        void nullStatisticsNeverDrops() {
            RowGroup rg = makeRowGroupWithStats(PhysicalType.INT32, null);
            FileSchema schema = makeIntSchema("col");

            assertThat(RowGroupFilter.eq("col", 50).canDrop(rg, schema)).isFalse();
        }

        @Test
        void noStatisticsDoubleNeverDrops() {
            Statistics noStats = new Statistics(null, null, -1, -1, false);
            RowGroup rg = makeRowGroupWithStats(PhysicalType.DOUBLE, noStats);
            FileSchema schema = makeDoubleSchema("fare");

            assertThat(RowGroupFilter.eq("fare", 50.0).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gt("fare", 50.0).canDrop(rg, schema)).isFalse();
        }

        @Test
        void noStatisticsStringNeverDrops() {
            Statistics noStats = new Statistics(null, null, -1, -1, false);
            RowGroup rg = makeRowGroupWithStats(PhysicalType.BYTE_ARRAY, noStats);
            FileSchema schema = makeStringSchema("name");

            assertThat(RowGroupFilter.eq("name", "test").canDrop(rg, schema)).isFalse();
        }
    }

    @Nested
    class PageFilterTests {

        @Test
        void pageFilterGtInt() {
            RowGroupFilter filter = RowGroupFilter.gt("col", 100);
            assertThat(filter.canDropPage(intBytes(10), intBytes(50), PhysicalType.INT32)).isTrue();
            assertThat(filter.canDropPage(intBytes(10), intBytes(200), PhysicalType.INT32)).isFalse();
        }

        @Test
        void pageFilterEqInt() {
            RowGroupFilter filter = RowGroupFilter.eq("col", 50);
            assertThat(filter.canDropPage(intBytes(60), intBytes(100), PhysicalType.INT32)).isTrue();
            assertThat(filter.canDropPage(intBytes(10), intBytes(100), PhysicalType.INT32)).isFalse();
        }

        @Test
        void pageFilterLtInt() {
            RowGroupFilter filter = RowGroupFilter.lt("col", 10);
            assertThat(filter.canDropPage(intBytes(10), intBytes(100), PhysicalType.INT32)).isTrue();
            assertThat(filter.canDropPage(intBytes(5), intBytes(100), PhysicalType.INT32)).isFalse();
        }

        @Test
        void pageFilterGtEqInt() {
            RowGroupFilter filter = RowGroupFilter.gtEq("col", 101);
            assertThat(filter.canDropPage(intBytes(10), intBytes(100), PhysicalType.INT32)).isTrue();
            assertThat(filter.canDropPage(intBytes(10), intBytes(101), PhysicalType.INT32)).isFalse();
        }

        @Test
        void pageFilterLtEqInt() {
            RowGroupFilter filter = RowGroupFilter.ltEq("col", 9);
            assertThat(filter.canDropPage(intBytes(10), intBytes(100), PhysicalType.INT32)).isTrue();
            assertThat(filter.canDropPage(intBytes(9), intBytes(100), PhysicalType.INT32)).isFalse();
        }

        @Test
        void pageFilterNotEqInt() {
            RowGroupFilter filter = RowGroupFilter.notEq("col", 42);
            assertThat(filter.canDropPage(intBytes(42), intBytes(42), PhysicalType.INT32)).isTrue();
            assertThat(filter.canDropPage(intBytes(10), intBytes(100), PhysicalType.INT32)).isFalse();
        }

        @Test
        void pageFilterDouble() {
            RowGroupFilter filter = RowGroupFilter.gt("f", 10.0);
            assertThat(filter.canDropPage(doubleBytes(1.0), doubleBytes(9.0), PhysicalType.DOUBLE)).isTrue();
            assertThat(filter.canDropPage(doubleBytes(1.0), doubleBytes(20.0), PhysicalType.DOUBLE)).isFalse();
        }

        @Test
        void pageFilterString() {
            RowGroupFilter filter = RowGroupFilter.eq("s", "hello");
            byte[] helloBytes = "hello".getBytes(StandardCharsets.UTF_8);
            byte[] abcBytes = "abc".getBytes(StandardCharsets.UTF_8);
            byte[] defBytes = "def".getBytes(StandardCharsets.UTF_8);
            byte[] xyzBytes = "xyz".getBytes(StandardCharsets.UTF_8);

            assertThat(filter.canDropPage(abcBytes, defBytes, PhysicalType.BYTE_ARRAY)).isTrue();
            assertThat(filter.canDropPage(abcBytes, xyzBytes, PhysicalType.BYTE_ARRAY)).isFalse();
        }

        @Test
        void compoundFiltersReturnFalseForPageLevel() {
            RowGroupFilter and = RowGroupFilter.and(RowGroupFilter.eq("a", 1));
            RowGroupFilter or = RowGroupFilter.or(RowGroupFilter.eq("a", 1));
            RowGroupFilter not = RowGroupFilter.not(RowGroupFilter.eq("a", 1));

            assertThat(and.canDropPage(intBytes(0), intBytes(10), PhysicalType.INT32)).isFalse();
            assertThat(or.canDropPage(intBytes(0), intBytes(10), PhysicalType.INT32)).isFalse();
            assertThat(not.canDropPage(intBytes(0), intBytes(10), PhysicalType.INT32)).isFalse();
        }
    }

    @Nested
    class StatisticsRecordTests {

        @Test
        void hasMinMaxTrue() {
            Statistics stats = new Statistics(intBytes(1), intBytes(10), 0, -1, false);
            assertThat(stats.hasMinMax()).isTrue();
        }

        @Test
        void hasMinMaxFalseWhenMinNull() {
            Statistics stats = new Statistics(null, intBytes(10), 0, -1, false);
            assertThat(stats.hasMinMax()).isFalse();
        }

        @Test
        void hasMinMaxFalseWhenMaxNull() {
            Statistics stats = new Statistics(intBytes(1), null, 0, -1, false);
            assertThat(stats.hasMinMax()).isFalse();
        }

        @Test
        void hasMinMaxFalseWhenBothNull() {
            Statistics stats = new Statistics(null, null, -1, -1, false);
            assertThat(stats.hasMinMax()).isFalse();
        }

        @Test
        void deprecatedFlag() {
            Statistics modern = new Statistics(intBytes(1), intBytes(10), 0, 5, false);
            Statistics legacy = new Statistics(intBytes(1), intBytes(10), 0, 5, true);

            assertThat(modern.isMinMaxDeprecated()).isFalse();
            assertThat(legacy.isMinMaxDeprecated()).isTrue();
        }

        @Test
        void nullAndDistinctCounts() {
            Statistics stats = new Statistics(intBytes(1), intBytes(10), 42, 7, false);
            assertThat(stats.nullCount()).isEqualTo(42);
            assertThat(stats.distinctCount()).isEqualTo(7);
        }
    }

    @Nested
    class StatisticsConverterTests {

        @Test
        void int32Conversion() {
            assertThat(StatisticsConverter.bytesToLong(intBytes(42), PhysicalType.INT32)).isEqualTo(42);
            assertThat(StatisticsConverter.bytesToLong(intBytes(-7), PhysicalType.INT32)).isEqualTo(-7);
            assertThat(StatisticsConverter.bytesToLong(intBytes(0), PhysicalType.INT32)).isEqualTo(0);
            assertThat(StatisticsConverter.bytesToLong(intBytes(Integer.MAX_VALUE), PhysicalType.INT32)).isEqualTo(Integer.MAX_VALUE);
            assertThat(StatisticsConverter.bytesToLong(intBytes(Integer.MIN_VALUE), PhysicalType.INT32)).isEqualTo(Integer.MIN_VALUE);
        }

        @Test
        void int64Conversion() {
            assertThat(StatisticsConverter.bytesToLong(longBytes(123456789L), PhysicalType.INT64)).isEqualTo(123456789L);
            assertThat(StatisticsConverter.bytesToLong(longBytes(-123456789L), PhysicalType.INT64)).isEqualTo(-123456789L);
            assertThat(StatisticsConverter.bytesToLong(longBytes(0L), PhysicalType.INT64)).isEqualTo(0L);
            assertThat(StatisticsConverter.bytesToLong(longBytes(Long.MAX_VALUE), PhysicalType.INT64)).isEqualTo(Long.MAX_VALUE);
            assertThat(StatisticsConverter.bytesToLong(longBytes(Long.MIN_VALUE), PhysicalType.INT64)).isEqualTo(Long.MIN_VALUE);
        }

        @Test
        void booleanConversion() {
            assertThat(StatisticsConverter.bytesToLong(new byte[]{1}, PhysicalType.BOOLEAN)).isEqualTo(1L);
            assertThat(StatisticsConverter.bytesToLong(new byte[]{0}, PhysicalType.BOOLEAN)).isEqualTo(0L);
        }

        @Test
        void floatConversion() {
            assertThat(StatisticsConverter.bytesToDouble(floatBytes(3.14f), PhysicalType.FLOAT)).isEqualTo(3.14f);
            assertThat(StatisticsConverter.bytesToDouble(floatBytes(-1.5f), PhysicalType.FLOAT)).isEqualTo(-1.5f);
            assertThat(StatisticsConverter.bytesToDouble(floatBytes(0.0f), PhysicalType.FLOAT)).isEqualTo(0.0f);
        }

        @Test
        void doubleConversion() {
            assertThat(StatisticsConverter.bytesToDouble(doubleBytes(3.14), PhysicalType.DOUBLE)).isEqualTo(3.14);
            assertThat(StatisticsConverter.bytesToDouble(doubleBytes(-1.5), PhysicalType.DOUBLE)).isEqualTo(-1.5);
            assertThat(StatisticsConverter.bytesToDouble(doubleBytes(0.0), PhysicalType.DOUBLE)).isEqualTo(0.0);
        }

        @Test
        void int32ToDoubleConversion() {
            assertThat(StatisticsConverter.bytesToDouble(intBytes(42), PhysicalType.INT32)).isEqualTo(42.0);
        }

        @Test
        void int64ToDoubleConversion() {
            assertThat(StatisticsConverter.bytesToDouble(longBytes(100L), PhysicalType.INT64)).isEqualTo(100.0);
        }

        @Test
        void byteArrayToLongThrows() {
            assertThatThrownBy(() -> StatisticsConverter.bytesToLong(new byte[]{1, 2}, PhysicalType.BYTE_ARRAY))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("BYTE_ARRAY");
        }

        @Test
        void fixedLenByteArrayToLongThrows() {
            assertThatThrownBy(() -> StatisticsConverter.bytesToLong(new byte[]{1, 2}, PhysicalType.FIXED_LEN_BYTE_ARRAY))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("FIXED_LEN_BYTE_ARRAY");
        }

        @Test
        void int96ToLongThrows() {
            assertThatThrownBy(() -> StatisticsConverter.bytesToLong(new byte[12], PhysicalType.INT96))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("INT96");
        }

        @Test
        void booleanToDoubleThrows() {
            assertThatThrownBy(() -> StatisticsConverter.bytesToDouble(new byte[]{1}, PhysicalType.BOOLEAN))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void bytesComparisonBasic() {
            byte[] a = "apple".getBytes(StandardCharsets.UTF_8);
            byte[] b = "banana".getBytes(StandardCharsets.UTF_8);

            assertThat(StatisticsConverter.compareBytes(a, b)).isLessThan(0);
            assertThat(StatisticsConverter.compareBytes(b, a)).isGreaterThan(0);
            assertThat(StatisticsConverter.compareBytes(a, a)).isEqualTo(0);
        }

        @Test
        void bytesComparisonDifferentLengths() {
            byte[] short_ = "abc".getBytes(StandardCharsets.UTF_8);
            byte[] long_ = "abcdef".getBytes(StandardCharsets.UTF_8);

            assertThat(StatisticsConverter.compareBytes(short_, long_)).isLessThan(0);
            assertThat(StatisticsConverter.compareBytes(long_, short_)).isGreaterThan(0);
        }

        @Test
        void bytesComparisonEmpty() {
            byte[] empty = new byte[0];
            byte[] nonempty = "a".getBytes(StandardCharsets.UTF_8);

            assertThat(StatisticsConverter.compareBytes(empty, empty)).isEqualTo(0);
            assertThat(StatisticsConverter.compareBytes(empty, nonempty)).isLessThan(0);
            assertThat(StatisticsConverter.compareBytes(nonempty, empty)).isGreaterThan(0);
        }

        @Test
        void bytesComparisonUnsigned() {
            // 0xFF should be greater than 0x01 in unsigned comparison
            byte[] highByte = new byte[]{(byte) 0xFF};
            byte[] lowByte = new byte[]{(byte) 0x01};

            assertThat(StatisticsConverter.compareBytes(highByte, lowByte)).isGreaterThan(0);
            assertThat(StatisticsConverter.compareBytes(lowByte, highByte)).isLessThan(0);
        }
    }

    @Nested
    class MultiColumnSchemaTests {

        @Test
        void filterMatchesCorrectColumnInMultiColumnSchema() {
            Statistics fareStats = new Statistics(doubleBytes(10.0), doubleBytes(100.0), 0, -1, false);
            Statistics idStats = new Statistics(intBytes(1), intBytes(1000), 0, -1, false);

            ColumnMetaData idMeta = new ColumnMetaData(
                    PhysicalType.INT32, List.of(), List.of("id"), CompressionCodec.UNCOMPRESSED,
                    1000, 4000, 4000, 0, null, idStats);
            ColumnMetaData fareMeta = new ColumnMetaData(
                    PhysicalType.DOUBLE, List.of(), List.of("fare"), CompressionCodec.UNCOMPRESSED,
                    1000, 8000, 8000, 4000, null, fareStats);

            ColumnChunk idChunk = new ColumnChunk(idMeta, null, null, null, null);
            ColumnChunk fareChunk = new ColumnChunk(fareMeta, null, null, null, null);

            RowGroup rg = new RowGroup(List.of(idChunk, fareChunk), 12000, 1000);

            SchemaElement root = new SchemaElement("schema", null, null, null, 2, null, null, null, null, null);
            SchemaElement idCol = new SchemaElement("id", PhysicalType.INT32, null, RepetitionType.REQUIRED, null,
                    null, null, null, null, null);
            SchemaElement fareCol = new SchemaElement("fare", PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL, null,
                    null, null, null, null, null);
            FileSchema schema = FileSchema.fromSchemaElements(List.of(root, idCol, fareCol));

            // Filter on id: should use idStats (1..1000)
            assertThat(RowGroupFilter.eq("id", 500).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.eq("id", 2000).canDrop(rg, schema)).isTrue();

            // Filter on fare: should use fareStats (10.0..100.0)
            assertThat(RowGroupFilter.gt("fare", 50.0).canDrop(rg, schema)).isFalse();
            assertThat(RowGroupFilter.gt("fare", 100.0).canDrop(rg, schema)).isTrue();
        }

        @Test
        void compoundFilterAcrossMultipleColumns() {
            Statistics fareStats = new Statistics(doubleBytes(10.0), doubleBytes(100.0), 0, -1, false);
            Statistics idStats = new Statistics(intBytes(1), intBytes(1000), 0, -1, false);

            ColumnMetaData idMeta = new ColumnMetaData(
                    PhysicalType.INT32, List.of(), List.of("id"), CompressionCodec.UNCOMPRESSED,
                    1000, 4000, 4000, 0, null, idStats);
            ColumnMetaData fareMeta = new ColumnMetaData(
                    PhysicalType.DOUBLE, List.of(), List.of("fare"), CompressionCodec.UNCOMPRESSED,
                    1000, 8000, 8000, 4000, null, fareStats);

            ColumnChunk idChunk = new ColumnChunk(idMeta, null, null, null, null);
            ColumnChunk fareChunk = new ColumnChunk(fareMeta, null, null, null, null);

            RowGroup rg = new RowGroup(List.of(idChunk, fareChunk), 12000, 1000);

            SchemaElement root = new SchemaElement("schema", null, null, null, 2, null, null, null, null, null);
            SchemaElement idCol = new SchemaElement("id", PhysicalType.INT32, null, RepetitionType.REQUIRED, null,
                    null, null, null, null, null);
            SchemaElement fareCol = new SchemaElement("fare", PhysicalType.DOUBLE, null, RepetitionType.OPTIONAL, null,
                    null, null, null, null, null);
            FileSchema schema = FileSchema.fromSchemaElements(List.of(root, idCol, fareCol));

            // AND(id > 2000, fare > 50) -> id > 2000 drops because max=1000
            RowGroupFilter filter = RowGroupFilter.and(
                    RowGroupFilter.gt("id", 2000),
                    RowGroupFilter.gt("fare", 50.0));
            assertThat(filter.canDrop(rg, schema)).isTrue();

            // AND(id > 500, fare > 50) -> both keep
            RowGroupFilter filter2 = RowGroupFilter.and(
                    RowGroupFilter.gt("id", 500),
                    RowGroupFilter.gt("fare", 50.0));
            assertThat(filter2.canDrop(rg, schema)).isFalse();
        }
    }

    @Nested
    class ColumnIndexRecordTests {

        @Test
        void getPageCount() {
            var ci = new dev.hardwood.metadata.ColumnIndex(
                    List.of(false, false, true),
                    List.of(intBytes(1), intBytes(100), new byte[0]),
                    List.of(intBytes(50), intBytes(200), new byte[0]),
                    dev.hardwood.metadata.ColumnIndex.BoundaryOrder.ASCENDING,
                    List.of(0L, 5L, 100L));

            assertThat(ci.getPageCount()).isEqualTo(3);
            assertThat(ci.nullPages().get(2)).isTrue();
            assertThat(ci.boundaryOrder()).isEqualTo(dev.hardwood.metadata.ColumnIndex.BoundaryOrder.ASCENDING);
            assertThat(ci.nullCounts().get(1)).isEqualTo(5L);
        }

        @Test
        void boundaryOrderValues() {
            assertThat(dev.hardwood.metadata.ColumnIndex.BoundaryOrder.values()).hasSize(3);
            assertThat(dev.hardwood.metadata.ColumnIndex.BoundaryOrder.valueOf("UNORDERED"))
                    .isEqualTo(dev.hardwood.metadata.ColumnIndex.BoundaryOrder.UNORDERED);
        }
    }

    @Nested
    class FilterAPITests {

        @Test
        void intFilterColumnName() {
            assertThat(RowGroupFilter.eq("myCol", 1).columnName()).isEqualTo("myCol");
            assertThat(RowGroupFilter.gt("other", 5).columnName()).isEqualTo("other");
        }

        @Test
        void doubleFilterColumnName() {
            assertThat(RowGroupFilter.eq("fare", 1.0).columnName()).isEqualTo("fare");
        }

        @Test
        void stringFilterColumnName() {
            assertThat(RowGroupFilter.eq("name", "test").columnName()).isEqualTo("name");
        }

        @ParameterizedTest
        @ValueSource(longs = {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE})
        void eqIntBoundaryValues(long value) {
            // Just verifying these don't throw
            RowGroupFilter filter = RowGroupFilter.eq("col", value);
            assertThat(filter.columnName()).isEqualTo("col");
        }
    }

    // --- Helpers ---

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    private static byte[] floatBytes(float value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    private static byte[] doubleBytes(double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }

    private static RowGroup makeRowGroup(PhysicalType type, byte[] minBytes, byte[] maxBytes) {
        Statistics stats = new Statistics(minBytes, maxBytes, 0, -1, false);
        return makeRowGroupWithStats(type, stats);
    }

    private static RowGroup makeRowGroupWithStats(PhysicalType type, Statistics stats) {
        ColumnMetaData cmd = new ColumnMetaData(
                type, List.of(), List.of("col"), CompressionCodec.UNCOMPRESSED,
                1000, 4000, 4000, 0, null, stats);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 4000, 1000);
    }

    private static RowGroup makeStringRowGroup(String min, String max) {
        return makeRowGroup(PhysicalType.BYTE_ARRAY,
                min.getBytes(StandardCharsets.UTF_8),
                max.getBytes(StandardCharsets.UTF_8));
    }

    private static FileSchema makeIntSchema(String name) {
        return makeSchema(name, PhysicalType.INT32, null);
    }

    private static FileSchema makeDoubleSchema(String name) {
        return makeSchema(name, PhysicalType.DOUBLE, null);
    }

    private static FileSchema makeStringSchema(String name) {
        return makeSchema(name, PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
    }

    private static FileSchema makeSchema(String name, PhysicalType type, LogicalType logicalType) {
        SchemaElement root = new SchemaElement("schema", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement(name, type, null, RepetitionType.OPTIONAL, null,
                null, null, null, null, logicalType);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }
}
