/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;
import java.util.function.IntUnaryOperator;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.predicate.matcher.ints.IntInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.longs.LongInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.nulls.IsNullBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.schema.FileSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class BatchFilterCompilerTest {

    /// An unsigned column is eligible for the batch path like any other integer column. The
    /// ordered operators get matchers that compare in the column's own order; equality and
    /// membership are bit equality and reuse the signed ones.
    @Test
    void tryCompile_unsignedIntLeaf_isEligible() {
        FileSchema schema = schema(leaf("v", PhysicalType.INT32));

        for (Operator op : Operator.values()) {
            ColumnBatchMatcher[] result = compileMatchers(
                    new ResolvedPredicate.UnsignedIntPredicate(0, op, 7),
                    schema, IntUnaryOperator.identity());

            assertNotNull(result, "no batch matcher for unsigned INT32 " + op);
            assertInstanceOf(IntBatchMatcher.class, result[0]);
        }
    }

    @Test
    void tryCompile_unsignedLongLeaf_isEligible() {
        FileSchema schema = schema(leaf("v", PhysicalType.INT64));

        for (Operator op : Operator.values()) {
            ColumnBatchMatcher[] result = compileMatchers(
                    new ResolvedPredicate.UnsignedLongPredicate(0, op, 7L),
                    schema, IntUnaryOperator.identity());

            assertNotNull(result, "no batch matcher for unsigned INT64 " + op);
            assertInstanceOf(LongBatchMatcher.class, result[0]);
        }
    }

    @Test
    void tryCompile_unsignedInLeaves_areEligible() {
        FileSchema schema = schema(leaf("v", PhysicalType.INT32), leaf("w", PhysicalType.INT64));

        assertInstanceOf(IntInBatchMatcher.class, compileMatchers(
                new ResolvedPredicate.UnsignedIntInPredicate(0, new int[] { 1, 7 }),
                schema, IntUnaryOperator.identity())[0]);
        assertInstanceOf(LongInBatchMatcher.class, compileMatchers(
                new ResolvedPredicate.UnsignedLongInPredicate(1, new long[] { 1L, 7L }),
                schema, IntUnaryOperator.identity())[1]);
    }

    /// The ordered matchers must read the column's order, not the signed one: `4e9` is a
    /// negative `int`, so a signed matcher would answer `false` here.
    @Test
    void unsignedIntMatcher_ordersByUnsignedMagnitude() {
        int fourBillion = (int) 4_000_000_000L;
        ColumnBatchMatcher matcher = compileMatchers(
                new ResolvedPredicate.UnsignedIntPredicate(0, Operator.GT, 7),
                schema(leaf("v", PhysicalType.INT32)), IntUnaryOperator.identity())[0];

        BatchExchange.Batch batch = new BatchExchange.Batch();
        batch.values = new int[] { 0, fourBillion, 7, 8 };
        batch.recordCount = 4;
        long[] out = new long[1];
        matcher.test(batch, out);

        assertEquals(0b1010L, out[0]);
    }

    private static ColumnBatchMatcher[] compileMatchers(ResolvedPredicate predicate, FileSchema schema,
            IntUnaryOperator projection) {
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(predicate, schema, projection);
        return compiled == null ? null : compiled.columnMatchers();
    }

    private static FileSchema schema(SchemaElement... columns) {
        SchemaElement root = SchemaElement.root("root", columns.length);
        SchemaElement[] elements = new SchemaElement[columns.length + 1];
        elements[0] = root;
        System.arraycopy(columns, 0, elements, 1, columns.length);
        return FileSchema.fromSchemaElements(List.of(elements));
    }

    private static SchemaElement leaf(String name, PhysicalType type) {
        return SchemaElement.primitive(name, type, RepetitionType.OPTIONAL);
    }

    private static FileSchema longDoubleSchema() {
        return schema(leaf("id", PhysicalType.INT64), leaf("value", PhysicalType.DOUBLE));
    }

    @Test
    void tryCompile_andOfLongAndDoubleLeaves_returnsTwoFragments() {
        FileSchema schema = longDoubleSchema();
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0)
        ));

        ColumnBatchMatcher[] result = compileMatchers(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(2, result.length);
        assertInstanceOf(LongBatchMatcher.class, result[0]);
        assertInstanceOf(DoubleBatchMatcher.class, result[1]);
    }

    @Test
    void singleLongLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L);

        ColumnBatchMatcher[] result = compileMatchers(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(LongBatchMatcher.class, result[0]);
    }

    @Test
    void singleBooleanLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("flag", PhysicalType.BOOLEAN));
        ResolvedPredicate predicate = new ResolvedPredicate.BooleanPredicate(0, Operator.EQ, true);

        ColumnBatchMatcher[] result = compileMatchers(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(BooleanBatchMatcher.class, result[0]);
    }

    @Test
    void singleLongInLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.LongInPredicate(0, new long[]{1L, 2L, 3L});

        ColumnBatchMatcher[] result = compileMatchers(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(LongInBatchMatcher.class, result[0]);
    }

    @Test
    void singleIsNullLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.IsNullPredicate(0, 1);

        ColumnBatchMatcher[] result = compileMatchers(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(IsNullBatchMatcher.class, result[0]);
    }

    @Nested
    class IneligibleShapes {

        @Test
        void andWithNestedAnd_isFlattenedAndCompiles() {
            // ResolvedPredicate.And flattens nested And at construction, so the batch
            // path sees a single flat conjunction and compiles successfully.
            FileSchema schema = longDoubleSchema();
            ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                    new ResolvedPredicate.And(List.of(
                            new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0)
                    ))
            ));
            ColumnBatchMatcher[] result = compileMatchers(
                    predicate, schema, IntUnaryOperator.identity());
            assertNotNull(result);
            assertEquals(2, result.length);
        }

        @Test
        void andWithBinaryChild_returnsNull() {
            // BinaryPredicate is unsupported. As a child of an otherwise-eligible
            // And, it must poison the whole compile so the query falls back rather
            // than the supported leaves silently running on a partial conjunction.
            FileSchema schema = schema(
                    leaf("id", PhysicalType.INT64),
                    leaf("name", PhysicalType.BYTE_ARRAY));
            ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                    new ResolvedPredicate.BinaryPredicate(1, Operator.EQ,
                            new byte[]{'h', 'i'}, Comparison.BYTE_STRING)));
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void binaryLeaf_returnsNull() {
            FileSchema schema = schema(leaf("name", PhysicalType.BYTE_ARRAY));
            ResolvedPredicate predicate = new ResolvedPredicate.BinaryPredicate(0, Operator.EQ,
                    new byte[]{'h', 'i'}, Comparison.BYTE_STRING);
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void leafOnNonTopLevelPath_returnsNull() {
            // A nested struct: root -> nest (group, 1 child) -> id (INT64).
            // The leaf column has fieldPath ["nest", "id"] — not top-level.
            SchemaElement root = SchemaElement.root("root", 1);
            SchemaElement nest = SchemaElement.group("nest", RepetitionType.OPTIONAL, 1);
            SchemaElement id = SchemaElement.primitive("id", PhysicalType.INT64, RepetitionType.OPTIONAL);
            FileSchema schema = FileSchema.fromSchemaElements(List.of(root, nest, id));
            ResolvedPredicate predicate = new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L);
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void topLevelFieldIndex_returnsMinusOne_returnsNull() {
            FileSchema schema = schema(leaf("id", PhysicalType.INT64));
            ResolvedPredicate predicate = new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L);
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, col -> -1));
        }

    }

    @Test
    void twoLeavesOnSameColumn_composeIntoAndMatcher() {
        // Range-style predicates (`id >= x AND id <= y`) put two leaves on the
        // same projected column. The compiler keeps a single slot per column
        // and folds the second leaf into an AndBatchMatcher composite.
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 5L),
                new ResolvedPredicate.LongPredicate(0, Operator.LT_EQ, 100L)
        ));
        ColumnBatchMatcher[] result = compileMatchers(
                predicate, schema, IntUnaryOperator.identity());
        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(AndBatchMatcher.class, result[0]);
    }

    @Test
    void twoLeavesOnSameColumn_underOr_composeIntoOrMatcher() {
        // Same-column OR: `id < -5 OR id > 5` folds into a single OrBatchMatcher
        // in one column slot, and the MergePlan is a single Column node.
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.LT, -5L),
                new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L)
        ));
        CompiledBatchFilter result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());
        assertNotNull(result);
        assertEquals(1, result.columnMatchers().length);
        assertInstanceOf(OrBatchMatcher.class, result.columnMatchers()[0]);
        assertInstanceOf(MergePlan.Column.class, result.mergePlan());
    }

    @Test
    void orOfLongAndDoubleLeaves_compilesAsOr() {
        FileSchema schema = longDoubleSchema();
        ResolvedPredicate predicate = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0)
        ));
        CompiledBatchFilter result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());
        assertNotNull(result);
        assertInstanceOf(LongBatchMatcher.class, result.columnMatchers()[0]);
        assertInstanceOf(DoubleBatchMatcher.class, result.columnMatchers()[1]);
        assertInstanceOf(MergePlan.Or.class, result.mergePlan());
    }

    @Test
    void andOfLeafAndOrOnDistinctColumn_compilesAsMixedTree() {
        // `id > 5 AND (value < 0 OR value > 1000)` — the OR subtree is fully
        // contained on the `value` column and folds into a single OrBatchMatcher
        // slot, while the outer AND becomes a MergePlan.And.
        FileSchema schema = longDoubleSchema();
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.DoublePredicate(1, Operator.LT, 0.0),
                        new ResolvedPredicate.DoublePredicate(1, Operator.GT, 1000.0)
                ))
        ));
        CompiledBatchFilter result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());
        assertNotNull(result);
        assertInstanceOf(LongBatchMatcher.class, result.columnMatchers()[0]);
        assertInstanceOf(OrBatchMatcher.class, result.columnMatchers()[1]);
        assertInstanceOf(MergePlan.And.class, result.mergePlan());
    }

    @Test
    void andWithSingleMixedChild_compilesAsThatChildsCombine() {
        // ResolvedPredicate.And/Or accept a single child (only empty is rejected).
        // A 1-child And wrapping a mixed-column Or is semantically the Or itself;
        // ensure the compiler unwraps rather than producing a 1-element compound
        // (which throws "requires at least two children").
        FileSchema schema = longDoubleSchema();
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                        new ResolvedPredicate.DoublePredicate(1, Operator.LT, 0.0)))
        ));
        CompiledBatchFilter result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());
        assertNotNull(result);
        assertInstanceOf(MergePlan.Or.class, result.mergePlan());
    }

    @Test
    void andWithSingleSameColumnChild_compilesAsSingleColumn() {
        // 1-child And wrapping a same-column Or (`id < -5 OR id > 5`) should
        // unwrap to that subtree's single-column composite — no MergePlan.And
        // wrapper, since it would have only one child.
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.LT, -5L),
                        new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L)))
        ));
        CompiledBatchFilter result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());
        assertNotNull(result);
        assertInstanceOf(OrBatchMatcher.class, result.columnMatchers()[0]);
        assertInstanceOf(MergePlan.Column.class, result.mergePlan());
    }

    @Test
    void columnAppearingInTwoSiblingSubtrees_returnsNull() {
        // (a > 5 AND b > 5) OR (a < 0 AND b < 0): column `a` lives in both OR
        // branches. The per-column matcher model can't carry two distinct
        // predicates for `a`, so the compile rejects.
        FileSchema schema = longDoubleSchema();
        ResolvedPredicate predicate = new ResolvedPredicate.Or(List.of(
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                        new ResolvedPredicate.DoublePredicate(1, Operator.GT, 5.0))),
                new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.LongPredicate(0, Operator.LT, 0L),
                        new ResolvedPredicate.DoublePredicate(1, Operator.LT, 0.0)))
        ));
        assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
    }
}
