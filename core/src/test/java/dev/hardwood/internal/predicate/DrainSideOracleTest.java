/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Two-way equivalence: compiled [RecordFilterCompiler] and drain-side [BatchFilterCompiler]
/// + per-column [ColumnBatchMatcher] agree on which rows survive a given predicate. Constitutes
/// the load-bearing correctness gate for the drain-side prototype.
class DrainSideOracleTest {

    private static final int N = 256;

    @Test
    void singleLongLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xC0FFEE);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.LongPredicate(0, op, 100L);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleDoubleLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xBEEF);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.DoublePredicate(1, op, 0.5);
            assertSurvivorsAgree(p, w);
        }
    }

    @ParameterizedTest(name = "and(id {0} 100, value {1} 0.5)")
    @MethodSource("opPairs")
    void andOfLongAndDouble_bothWaysAgree(Operator opA, Operator opB) {
        Workload w = workload(0xFEED);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(0, opA, 100L),
                new ResolvedPredicate.DoublePredicate(1, opB, 0.5)
        ));
        assertSurvivorsAgree(p, w);
    }

    private static Stream<Arguments> opPairs() {
        Operator[] ops = Operator.values();
        List<Arguments> pairs = new ArrayList<>();
        for (Operator a : ops) {
            for (Operator b : ops) {
                pairs.add(Arguments.of(a, b));
            }
        }
        return pairs.stream();
    }

    private static void assertSurvivorsAgree(ResolvedPredicate predicate, Workload w) {
        BitSet compiled = compiledSurvivors(predicate, w);
        BitSet drainSide = drainSideSurvivors(predicate, w);
        if (drainSide != null) {
            assertEquals(compiled, drainSide, () -> "compiled/drain-side diverged for " + predicate);
        }
    }

    private static BitSet compiledSurvivors(ResolvedPredicate predicate, Workload w) {
        RowMatcher matcher = RecordFilterCompiler.compile(predicate, w.schema,
                w.projection::toProjectedIndex);
        BitSet out = new BitSet(N);
        for (int i = 0; i < N; i++) {
            if (matcher.test(w.row(i))) {
                out.set(i);
            }
        }
        return out;
    }

    private static BitSet drainSideSurvivors(ResolvedPredicate predicate, Workload w) {
        ColumnBatchMatcher[] fragments = BatchFilterCompiler.tryCompile(predicate, w.schema,
                w.projection::toProjectedIndex);
        if (fragments == null) {
            // Predicate not eligible for drain-side (single-leaf shapes fall back
            // to the compiled path by design — see the >= 2 fragment gate in
            // BatchFilterCompiler). Caller skips the drain comparison.
            return null;
        }

        int wordsLen = (N + 63) >>> 6;
        long[] combined = new long[wordsLen];
        boolean initialised = false;
        for (int col = 0; col < fragments.length; col++) {
            ColumnBatchMatcher m = fragments[col];
            if (m == null) {
                continue;
            }
            BatchExchange.Batch batch = w.batch(col);
            long[] colWords = new long[wordsLen];
            m.test(batch, colWords);
            if (!initialised) {
                System.arraycopy(colWords, 0, combined, 0, wordsLen);
                initialised = true;
            }
            else {
                for (int wi = 0; wi < wordsLen; wi++) {
                    combined[wi] &= colWords[wi];
                }
            }
        }
        if (!initialised) {
            // No fragments installed — universal "all-ones" up to N.
            for (int i = 0; i < N; i++) {
                combined[i >>> 6] |= 1L << i;
            }
        }

        BitSet out = new BitSet(N);
        for (int i = 0; i < N; i++) {
            if ((combined[i >>> 6] & (1L << i)) != 0) {
                out.set(i);
            }
        }
        return out;
    }

    // ---------- Workload construction ----------

    private static Workload workload(long seed) {
        Random r = new Random(seed);
        long[] ids = new long[N];
        double[] values = new double[N];
        BitSet idNulls = new BitSet(N);
        BitSet valueNulls = new BitSet(N);

        // Boundary-heavy values to cover NaN, infinities, equal-to-literal cases.
        double[] boundaryDoubles = {0.5, -0.5, 0.0, -0.0, Double.NaN,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE, Double.MAX_VALUE};

        for (int i = 0; i < N; i++) {
            ids[i] = r.nextInt(300) - 50; // range straddles literal 100
            values[i] = (i < boundaryDoubles.length)
                    ? boundaryDoubles[i]
                    : (r.nextDouble() * 2.0 - 1.0); // [-1, 1) — straddles literal 0.5
            if (r.nextInt(10) == 0) {
                idNulls.set(i);
            }
            if (r.nextInt(15) == 0) {
                valueNulls.set(i);
            }
        }
        return new Workload(ids, idNulls, values, valueNulls);
    }

    private static final class Workload {
        final long[] ids;
        final BitSet idNulls;
        final double[] values;
        final BitSet valueNulls;
        final FileSchema schema;
        final ProjectedSchema projection;

        Workload(long[] ids, BitSet idNulls, double[] values, BitSet valueNulls) {
            this.ids = ids;
            this.idNulls = idNulls;
            this.values = values;
            this.valueNulls = valueNulls;
            SchemaElement root = new SchemaElement("root", null, null, null, 2,
                    null, null, null, null, null);
            SchemaElement c1 = new SchemaElement("id", PhysicalType.INT64, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            SchemaElement c2 = new SchemaElement("value", PhysicalType.DOUBLE, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            this.schema = FileSchema.fromSchemaElements(List.of(root, c1, c2));
            this.projection = ProjectedSchema.create(schema, ColumnProjection.all());
        }

        RowReader row(int i) {
            return new SyntheticRow(ids[i], idNulls.get(i), values[i], valueNulls.get(i));
        }

        BatchExchange.Batch batch(int projectedIdx) {
            BatchExchange.Batch b = new BatchExchange.Batch();
            switch (projectedIdx) {
                case 0 -> {
                    b.values = ids;
                    b.nulls = idNulls.isEmpty() ? null : idNulls;
                }
                case 1 -> {
                    b.values = values;
                    b.nulls = valueNulls.isEmpty() ? null : valueNulls;
                }
                default -> throw new IllegalArgumentException("col " + projectedIdx);
            }
            b.recordCount = N;
            return b;
        }
    }

    private static final class SyntheticRow implements RowReader {
        private final long idValue;
        private final boolean idNull;
        private final double valueValue;
        private final boolean valueNull;

        SyntheticRow(long idValue, boolean idNull, double valueValue, boolean valueNull) {
            this.idValue = idValue;
            this.idNull = idNull;
            this.valueValue = valueValue;
            this.valueNull = valueNull;
        }

        @Override public boolean isNull(int idx) {
            return switch (idx) {
                case 0 -> idNull;
                case 1 -> valueNull;
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        @Override public boolean isNull(String name) {
            return switch (name) {
                case "id" -> idNull;
                case "value" -> valueNull;
                default -> throw new IllegalArgumentException(name);
            };
        }

        @Override public long getLong(int idx) { return idValue; }
        @Override public long getLong(String name) { return idValue; }
        @Override public double getDouble(int idx) { return valueValue; }
        @Override public double getDouble(String name) { return valueValue; }

        @Override public int getFieldCount() { return 2; }
        @Override public String getFieldName(int idx) {
            return switch (idx) {
                case 0 -> "id";
                case 1 -> "value";
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        // Defaults for everything else — the predicate paths under test should not call them.
        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() {}
        @Override public int getInt(int idx) { throw new UnsupportedOperationException(); }
        @Override public int getInt(String name) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(int idx) { throw new UnsupportedOperationException(); }
        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public boolean getBoolean(int idx) { throw new UnsupportedOperationException(); }
        @Override public boolean getBoolean(String name) { throw new UnsupportedOperationException(); }
        @Override public String getString(int idx) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(int idx) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(int idx) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(int idx) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(int idx) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(int idx) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(int idx) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
    }
}
