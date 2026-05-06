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

import dev.hardwood.internal.predicate.matcher.longs.LongInBatchMatcher;
import dev.hardwood.internal.predicate.matcher.nulls.IsNullBatchMatcher;
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

    private static FileSchema schema(SchemaElement... columns) {
        SchemaElement root = new SchemaElement("root", null, null, null, columns.length,
                null, null, null, null, null);
        SchemaElement[] elements = new SchemaElement[columns.length + 1];
        elements[0] = root;
        System.arraycopy(columns, 0, elements, 1, columns.length);
        return FileSchema.fromSchemaElements(List.of(elements));
    }

    private static SchemaElement leaf(String name, PhysicalType type) {
        return new SchemaElement(name, type, null, RepetitionType.OPTIONAL, null, null, null, null, null, null);
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

        BatchMatcher[] result = BatchFilterCompiler.tryCompile(
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

        BatchMatcher[] result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(LongBatchMatcher.class, result[0]);
    }

    @Test
    void singleBooleanLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("flag", PhysicalType.BOOLEAN));
        ResolvedPredicate predicate = new ResolvedPredicate.BooleanPredicate(0, Operator.EQ, true);

        BatchMatcher[] result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(BooleanBatchMatcher.class, result[0]);
    }

    @Test
    void singleLongInLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.LongInPredicate(0, new long[]{1L, 2L, 3L});

        BatchMatcher[] result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(LongInBatchMatcher.class, result[0]);
    }

    @Test
    void singleIsNullLeaf_returnsOneFragment() {
        FileSchema schema = schema(leaf("id", PhysicalType.INT64));
        ResolvedPredicate predicate = new ResolvedPredicate.IsNullPredicate(0);

        BatchMatcher[] result = BatchFilterCompiler.tryCompile(
                predicate, schema, IntUnaryOperator.identity());

        assertNotNull(result);
        assertEquals(1, result.length);
        assertInstanceOf(IsNullBatchMatcher.class, result[0]);
    }

    @Nested
    class IneligibleShapes {

        @Test
        void or_atRoot_returnsNull() {
            FileSchema schema = longDoubleSchema();
            ResolvedPredicate predicate = new ResolvedPredicate.Or(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0)
            ));
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void andWithNestedAnd_returnsNull() {
            FileSchema schema = longDoubleSchema();
            ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                    new ResolvedPredicate.And(List.of(
                            new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0)
                    ))
            ));
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void andWithOrChild_returnsNull() {
            FileSchema schema = longDoubleSchema();
            ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                    new ResolvedPredicate.Or(List.of(
                            new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0),
                            new ResolvedPredicate.DoublePredicate(1, Operator.GT, 1000.0)
                    ))
            ));
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void binaryLeaf_returnsNull() {
            FileSchema schema = schema(leaf("name", PhysicalType.BYTE_ARRAY));
            ResolvedPredicate predicate = new ResolvedPredicate.BinaryPredicate(0, Operator.EQ,
                    new byte[]{'h', 'i'}, false);
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }

        @Test
        void leafOnNonTopLevelPath_returnsNull() {
            // A nested struct: root -> nest (group, 1 child) -> id (INT64).
            // The leaf column has fieldPath ["nest", "id"] — not top-level.
            SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
            SchemaElement nest = new SchemaElement("nest", null, null, RepetitionType.OPTIONAL, 1,
                    null, null, null, null, null);
            SchemaElement id = new SchemaElement("id", PhysicalType.INT64, null, RepetitionType.OPTIONAL,
                    null, null, null, null, null, null);
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

        @Test
        void twoLeavesOnSameColumn_returnsNull() {
            // v1 doesn't merge per-column fragments; the eligibility check rejects
            // the duplicate so the existing FilteredRowReader path handles ranges.
            FileSchema schema = schema(leaf("id", PhysicalType.INT64));
            ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 5L),
                    new ResolvedPredicate.LongPredicate(0, Operator.LT, 100L)
            ));
            assertNull(BatchFilterCompiler.tryCompile(predicate, schema, IntUnaryOperator.identity()));
        }
    }
}
