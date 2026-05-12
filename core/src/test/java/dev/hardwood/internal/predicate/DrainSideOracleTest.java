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
import static org.junit.jupiter.api.Assertions.assertNotNull;

/// Two-way equivalence: compiled [RecordFilterCompiler] and drain-side [BatchFilterCompiler]
/// + per-column [ColumnBatchMatcher] agree on which rows survive a given predicate. Constitutes
/// the load-bearing correctness gate for the drain-side prototype.
///
/// The workload carries one column per supported primitive type — `id: long`, `value: double`,
/// `tag: int`, `score: float`, `flag: boolean` — each with its own scattered-null profile and
/// boundary-heavy values. Tests exercise every `(type, op)` pair listed in the design doc's
/// eligibility section plus `IntIn` / `LongIn` / `IsNull` / `IsNotNull`, both as single leaves
/// and in cross-type `And` compounds.
class DrainSideOracleTest {

    // 200 = 3 full 64-bit words + an 8-bit tail. Not a multiple of 64, so every
    // oracle method exercises the matcher's `tail != 0` branch and the consumer's
    // `bit < limit` / `Math.min(bit, limit)` clamps against stale trailing bits.
    private static final int N = 200;

    // Projected column indices used throughout the test.
    private static final int COL_ID = 0;     // long
    private static final int COL_VALUE = 1;  // double
    private static final int COL_TAG = 2;    // int
    private static final int COL_SCORE = 3;  // float
    private static final int COL_FLAG = 4;   // boolean

    // ---------- Single-leaf coverage, all supported (type, op) pairs ----------

    @Test
    void singleLongLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xC0FFEE);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.LongPredicate(COL_ID, op, 100L);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleDoubleLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xBEEF);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.DoublePredicate(COL_VALUE, op, 0.5);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleIntLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xA1107A6);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.IntPredicate(COL_TAG, op, 100);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleFloatLeaf_allOps_bothWaysAgree() {
        Workload w = workload(0xF10A75);
        for (Operator op : Operator.values()) {
            ResolvedPredicate p = new ResolvedPredicate.FloatPredicate(COL_SCORE, op, 0.5f);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void singleBooleanLeaf_eqAndNotEq_bothWaysAgree() {
        // BatchFilterCompiler.isSupported permits boolean only for EQ / NOT_EQ; other
        // operators force a drain-side fallback and the oracle assertion is skipped.
        Workload w = workload(0xB001EA1);
        for (Operator op : new Operator[]{Operator.EQ, Operator.NOT_EQ}) {
            for (boolean lit : new boolean[]{true, false}) {
                ResolvedPredicate p = new ResolvedPredicate.BooleanPredicate(COL_FLAG, op, lit);
                assertSurvivorsAgree(p, w);
            }
        }
    }

    @Test
    void intIn_bothWaysAgree() {
        Workload w = workload(0x1A110);
        int[] values = new int[]{0, 100, -50, 250, Integer.MAX_VALUE, Integer.MIN_VALUE};
        ResolvedPredicate p = new ResolvedPredicate.IntInPredicate(COL_TAG, values);
        assertSurvivorsAgree(p, w);
    }

    @Test
    void longIn_bothWaysAgree() {
        Workload w = workload(0xB16601);
        long[] values = new long[]{0L, 100L, -50L, 250L, Long.MAX_VALUE, Long.MIN_VALUE};
        ResolvedPredicate p = new ResolvedPredicate.LongInPredicate(COL_ID, values);
        assertSurvivorsAgree(p, w);
    }

    @Test
    void isNull_eachColumn_bothWaysAgree() {
        Workload w = workload(0x15A11);
        for (int col = 0; col < 5; col++) {
            ResolvedPredicate p = new ResolvedPredicate.IsNullPredicate(col);
            assertSurvivorsAgree(p, w);
        }
    }

    @Test
    void isNotNull_eachColumn_bothWaysAgree() {
        Workload w = workload(0x15A011);
        for (int col = 0; col < 5; col++) {
            ResolvedPredicate p = new ResolvedPredicate.IsNotNullPredicate(col);
            assertSurvivorsAgree(p, w);
        }
    }

    // ---------- Cross-type AND coverage ----------

    @ParameterizedTest(name = "and(id {0} 100, value {1} 0.5)")
    @MethodSource("opPairs")
    void andOfLongAndDouble_bothWaysAgree(Operator opA, Operator opB) {
        Workload w = workload(0xFEED);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, opA, 100L),
                new ResolvedPredicate.DoublePredicate(COL_VALUE, opB, 0.5)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfIntAndBoolean_bothWaysAgree() {
        Workload w = workload(0xA10A);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IntPredicate(COL_TAG, Operator.GT, 0),
                new ResolvedPredicate.BooleanPredicate(COL_FLAG, Operator.EQ, true)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfFloatAndIsNotNull_bothWaysAgree() {
        Workload w = workload(0xF10F);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.FloatPredicate(COL_SCORE, Operator.LT, 0.5f),
                new ResolvedPredicate.IsNotNullPredicate(COL_ID)
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andOfLongInAndIntIn_bothWaysAgree() {
        Workload w = workload(0x10F1A);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongInPredicate(COL_ID, new long[]{0L, 100L, -50L}),
                new ResolvedPredicate.IntInPredicate(COL_TAG, new int[]{0, 100, -50})
        ));
        assertSurvivorsAgree(p, w);
    }

    @Test
    void andSameColumnRange_bothWaysAgree() {
        // Same-column AND is now composed into an AndBatchMatcher in BatchFilterCompiler;
        // the oracle confirms the composite agrees with the compiled per-row path on
        // a range predicate.
        Workload w = workload(0x6A0E);
        ResolvedPredicate p = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.GT_EQ, 0L),
                new ResolvedPredicate.LongPredicate(COL_ID, Operator.LT_EQ, 200L)
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

    // ---------- Oracle plumbing ----------

    private static void assertSurvivorsAgree(ResolvedPredicate predicate, Workload w) {
        BitSet compiled = compiledSurvivors(predicate, w);
        BitSet drainSide = drainSideSurvivors(predicate, w);
        // Every predicate used in this file is intentionally drain-eligible. A `null`
        // here means BatchFilterCompiler.tryCompile newly refused to compile something
        // it used to handle — that's a regression we want to catch loudly, not skip.
        // Ineligibility tests live in BatchFilterCompilerTest.IneligibleShapes.
        assertNotNull(drainSide,
                () -> "BatchFilterCompiler.tryCompile returned null for an expected-eligible predicate: " + predicate);
        assertEquals(compiled, drainSide, () -> "compiled/drain-side diverged for " + predicate);
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
            // Predicate not eligible for drain-side compilation. Callers in this file
            // intentionally use eligible predicates, so `assertSurvivorsAgree` will
            // fail loudly on a null return rather than silently skipping.
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
        int[] tags = new int[N];
        float[] scores = new float[N];
        boolean[] flags = new boolean[N];
        BitSet idNulls = new BitSet(N);
        BitSet valueNulls = new BitSet(N);
        BitSet tagNulls = new BitSet(N);
        BitSet scoreNulls = new BitSet(N);
        BitSet flagNulls = new BitSet(N);

        // Boundary-heavy values to cover NaN, infinities, type extremes, and
        // equal-to-literal cases. The first few rows of each column carry these.
        double[] boundaryDoubles = {0.5, -0.5, 0.0, -0.0, Double.NaN,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE, Double.MAX_VALUE};
        float[] boundaryFloats = {0.5f, -0.5f, 0.0f, -0.0f, Float.NaN,
                Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
                Float.MIN_VALUE, Float.MAX_VALUE};
        int[] boundaryInts = {100, -100, 0, Integer.MIN_VALUE, Integer.MAX_VALUE};

        for (int i = 0; i < N; i++) {
            ids[i] = r.nextInt(300) - 50; // straddles literal 100
            values[i] = (i < boundaryDoubles.length)
                    ? boundaryDoubles[i]
                    : (r.nextDouble() * 2.0 - 1.0); // [-1, 1) — straddles literal 0.5
            tags[i] = (i < boundaryInts.length)
                    ? boundaryInts[i]
                    : r.nextInt(300) - 50; // straddles literal 100
            scores[i] = (i < boundaryFloats.length)
                    ? boundaryFloats[i]
                    : (float) (r.nextDouble() * 2.0 - 1.0); // straddles literal 0.5
            flags[i] = r.nextBoolean();
            if (r.nextInt(10) == 0) {
                idNulls.set(i);
            }
            if (r.nextInt(15) == 0) {
                valueNulls.set(i);
            }
            if (r.nextInt(12) == 0) {
                tagNulls.set(i);
            }
            if (r.nextInt(13) == 0) {
                scoreNulls.set(i);
            }
            if (r.nextInt(14) == 0) {
                flagNulls.set(i);
            }
        }
        return new Workload(ids, idNulls, values, valueNulls,
                tags, tagNulls, scores, scoreNulls, flags, flagNulls);
    }

    private static final class Workload {
        final long[] ids;
        final BitSet idNulls;
        final double[] values;
        final BitSet valueNulls;
        final int[] tags;
        final BitSet tagNulls;
        final float[] scores;
        final BitSet scoreNulls;
        final boolean[] flags;
        final BitSet flagNulls;
        final FileSchema schema;
        final ProjectedSchema projection;

        Workload(long[] ids, BitSet idNulls,
                 double[] values, BitSet valueNulls,
                 int[] tags, BitSet tagNulls,
                 float[] scores, BitSet scoreNulls,
                 boolean[] flags, BitSet flagNulls) {
            this.ids = ids;
            this.idNulls = idNulls;
            this.values = values;
            this.valueNulls = valueNulls;
            this.tags = tags;
            this.tagNulls = tagNulls;
            this.scores = scores;
            this.scoreNulls = scoreNulls;
            this.flags = flags;
            this.flagNulls = flagNulls;
            SchemaElement root = new SchemaElement("root", null, null, null, 5,
                    null, null, null, null, null);
            SchemaElement c1 = new SchemaElement("id", PhysicalType.INT64, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            SchemaElement c2 = new SchemaElement("value", PhysicalType.DOUBLE, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            SchemaElement c3 = new SchemaElement("tag", PhysicalType.INT32, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            SchemaElement c4 = new SchemaElement("score", PhysicalType.FLOAT, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            SchemaElement c5 = new SchemaElement("flag", PhysicalType.BOOLEAN, null,
                    RepetitionType.OPTIONAL, null, null, null, null, null, null);
            this.schema = FileSchema.fromSchemaElements(List.of(root, c1, c2, c3, c4, c5));
            this.projection = ProjectedSchema.create(schema, ColumnProjection.all());
        }

        RowReader row(int i) {
            return new SyntheticRow(
                    ids[i], idNulls.get(i),
                    values[i], valueNulls.get(i),
                    tags[i], tagNulls.get(i),
                    scores[i], scoreNulls.get(i),
                    flags[i], flagNulls.get(i));
        }

        BatchExchange.Batch batch(int projectedIdx) {
            BatchExchange.Batch b = new BatchExchange.Batch();
            switch (projectedIdx) {
                case COL_ID -> {
                    b.values = ids;
                    b.nulls = idNulls.isEmpty() ? null : idNulls;
                }
                case COL_VALUE -> {
                    b.values = values;
                    b.nulls = valueNulls.isEmpty() ? null : valueNulls;
                }
                case COL_TAG -> {
                    b.values = tags;
                    b.nulls = tagNulls.isEmpty() ? null : tagNulls;
                }
                case COL_SCORE -> {
                    b.values = scores;
                    b.nulls = scoreNulls.isEmpty() ? null : scoreNulls;
                }
                case COL_FLAG -> {
                    b.values = flags;
                    b.nulls = flagNulls.isEmpty() ? null : flagNulls;
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
        private final int tagValue;
        private final boolean tagNull;
        private final float scoreValue;
        private final boolean scoreNull;
        private final boolean flagValue;
        private final boolean flagNull;

        SyntheticRow(long idValue, boolean idNull,
                     double valueValue, boolean valueNull,
                     int tagValue, boolean tagNull,
                     float scoreValue, boolean scoreNull,
                     boolean flagValue, boolean flagNull) {
            this.idValue = idValue;
            this.idNull = idNull;
            this.valueValue = valueValue;
            this.valueNull = valueNull;
            this.tagValue = tagValue;
            this.tagNull = tagNull;
            this.scoreValue = scoreValue;
            this.scoreNull = scoreNull;
            this.flagValue = flagValue;
            this.flagNull = flagNull;
        }

        @Override public boolean isNull(int idx) {
            return switch (idx) {
                case COL_ID -> idNull;
                case COL_VALUE -> valueNull;
                case COL_TAG -> tagNull;
                case COL_SCORE -> scoreNull;
                case COL_FLAG -> flagNull;
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        @Override public boolean isNull(String name) {
            return switch (name) {
                case "id" -> idNull;
                case "value" -> valueNull;
                case "tag" -> tagNull;
                case "score" -> scoreNull;
                case "flag" -> flagNull;
                default -> throw new IllegalArgumentException(name);
            };
        }

        @Override public long getLong(int idx) { return idValue; }
        @Override public long getLong(String name) { return idValue; }
        @Override public double getDouble(int idx) { return valueValue; }
        @Override public double getDouble(String name) { return valueValue; }
        @Override public int getInt(int idx) { return tagValue; }
        @Override public int getInt(String name) { return tagValue; }
        @Override public float getFloat(int idx) { return scoreValue; }
        @Override public float getFloat(String name) { return scoreValue; }
        @Override public boolean getBoolean(int idx) { return flagValue; }
        @Override public boolean getBoolean(String name) { return flagValue; }

        @Override public int getFieldCount() { return 5; }
        @Override public String getFieldName(int idx) {
            return switch (idx) {
                case COL_ID -> "id";
                case COL_VALUE -> "value";
                case COL_TAG -> "tag";
                case COL_SCORE -> "score";
                case COL_FLAG -> "flag";
                default -> throw new IndexOutOfBoundsException(idx);
            };
        }

        // Defaults for everything else — the predicate paths under test should not call them.
        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public void next() { throw new UnsupportedOperationException(); }
        @Override public void close() {}
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
        @Override public Object getRawValue(int idx) { throw new UnsupportedOperationException(); }
        @Override public Object getRawValue(String name) { throw new UnsupportedOperationException(); }
    }
}
