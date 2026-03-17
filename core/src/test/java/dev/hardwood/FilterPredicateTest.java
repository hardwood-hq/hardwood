/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.RowGroupFilterEvaluator;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 15), rg, schema)).isFalse();

        // EQ 5 (below min) -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 5), rg, schema)).isTrue();

        // EQ 25 (above max) -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 25), rg, schema)).isTrue();

        // EQ 10 (equals min) -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 10), rg, schema)).isFalse();

        // EQ 20 (equals max) -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 20), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithNotEq() {
        // Row group with single value: min=max=42
        RowGroup rg = createIntRowGroup(42, 42);
        FileSchema schema = createIntSchema();

        // NOT_EQ 42 when min==max==42 -> can drop (all values are 42)
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.notEq("col", 42), rg, schema)).isTrue();

        // NOT_EQ 10 when min==max==42 -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.notEq("col", 10), rg, schema)).isFalse();

        // NOT_EQ with range min=10, max=20 -> cannot drop
        RowGroup range = createIntRowGroup(10, 20);
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.notEq("col", 15), range, schema)).isFalse();
    }

    @Test
    void testCanDropWithLt() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // LT 5 -> all values >= 10 so none < 5 -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.lt("col", 5), rg, schema)).isTrue();

        // LT 10 -> all values >= 10, none < 10 -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.lt("col", 10), rg, schema)).isTrue();

        // LT 15 -> some values < 15 -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.lt("col", 15), rg, schema)).isFalse();

        // LT 25 -> all values < 25 possible -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.lt("col", 25), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithLtEq() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // LT_EQ 9 -> all values >= 10 -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.ltEq("col", 9), rg, schema)).isTrue();

        // LT_EQ 10 -> min == 10 -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.ltEq("col", 10), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithGt() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // GT 25 -> max is 20 -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 25), rg, schema)).isTrue();

        // GT 20 -> max is 20, none > 20 -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 20), rg, schema)).isTrue();

        // GT 15 -> some values > 15 -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 15), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithGtEq() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // GT_EQ 21 -> max is 20 -> can drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gtEq("col", 21), rg, schema)).isTrue();

        // GT_EQ 20 -> max is 20 -> cannot drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gtEq("col", 20), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithLongPredicate() {
        RowGroup rg = createLongRowGroup(100L, 200L);
        FileSchema schema = createLongSchema();

        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 200L), rg, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.lt("col", 100L), rg, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 150L), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithDoublePredicate() {
        RowGroup rg = createDoubleRowGroup(1.0, 10.0);
        FileSchema schema = createDoubleSchema();

        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 10.0), rg, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.lt("col", 1.0), rg, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 5.0), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithFloatPredicate() {
        RowGroup rg = createFloatRowGroup(1.0f, 10.0f);
        FileSchema schema = createFloatSchema();

        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 10.0f), rg, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.ltEq("col", 0.5f), rg, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 5.0f), rg, schema)).isFalse();
    }

    @Test
    void testCanDropWithBooleanPredicate() {
        // Row group with all true: min=true, max=true
        RowGroup allTrue = createBooleanRowGroup(true, true);
        FileSchema schema = createBooleanSchema();

        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", false), allTrue, schema)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", true), allTrue, schema)).isFalse();

        // Row group with mixed: min=false, max=true
        RowGroup mixed = createBooleanRowGroup(false, true);
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", false), mixed, schema)).isFalse();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", true), mixed, schema)).isFalse();
    }

    @Test
    void testCanDropWithBinaryPredicate() {
        // Row group with strings "banana" to "date"
        RowGroup rg = createBinaryRowGroup("banana".getBytes(), "date".getBytes());
        FileSchema schema = createBinarySchema();

        // "apple" < "banana" -> EQ cannot match
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", "apple"), rg, schema)).isTrue();

        // "elderberry" > "date" -> EQ cannot match
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", "elderberry"), rg, schema)).isTrue();

        // "cherry" in range -> EQ might match
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
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
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(andLeftDrop, rg, schema)).isTrue();

        // AND where right can drop -> can drop
        FilterPredicate andRightDrop = FilterPredicate.and(
                FilterPredicate.lt("col", 30),   // cannot drop
                FilterPredicate.gt("col", 25)    // can drop
        );
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(andRightDrop, rg, schema)).isTrue();

        // AND where neither can drop -> cannot drop
        FilterPredicate andNeitherDrop = FilterPredicate.and(
                FilterPredicate.gt("col", 5),
                FilterPredicate.lt("col", 25)
        );
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(andNeitherDrop, rg, schema)).isFalse();
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
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(orBothDrop, rg, schema)).isTrue();

        // OR where only left can drop -> cannot drop
        FilterPredicate orLeftOnly = FilterPredicate.or(
                FilterPredicate.lt("col", 5),    // can drop
                FilterPredicate.gt("col", 15)    // cannot drop
        );
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(orLeftOnly, rg, schema)).isFalse();

        // OR where neither can drop -> cannot drop
        FilterPredicate orNeitherDrop = FilterPredicate.or(
                FilterPredicate.gt("col", 5),
                FilterPredicate.lt("col", 25)
        );
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(orNeitherDrop, rg, schema)).isFalse();
    }

    @Test
    void testMissingStatisticsNeverDrop() {
        // Row group with no statistics
        RowGroup rg = createRowGroupWithoutStatistics();
        FileSchema schema = createIntSchema();

        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("col", 42), rg, schema)).isFalse();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.gt("col", 100), rg, schema)).isFalse();
    }

    @Test
    void testUnknownColumnNeverDrop() {
        RowGroup rg = createIntRowGroup(10, 20);
        FileSchema schema = createIntSchema();

        // Filter on a column that doesn't exist -> conservative, don't drop
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(
                FilterPredicate.eq("nonexistent", 42), rg, schema)).isFalse();
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

    private static RowGroup createRowGroupWithStats(PhysicalType type, byte[] min, byte[] max) {
        Statistics stats = new Statistics(min, max, 0L, null, false);
        ColumnMetaData cmd = new ColumnMetaData(
                type, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, 0, null, stats);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 1000, 100);
    }

    private static RowGroup createRowGroupWithoutStatistics() {
        ColumnMetaData cmd = new ColumnMetaData(
                PhysicalType.INT32, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, 0, null, null);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 1000, 100);
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

    private static FileSchema createSchemaForType(PhysicalType type) {
        // Root element + one column
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement("col", type, null, RepetitionType.REQUIRED, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }
}
