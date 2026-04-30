/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.RecordFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

/// Megamorphic call-site benchmark: demonstrates the fusion path's
/// robustness when many distinct 2-leaf compound shapes flow through the
/// JVM and pollute the inline caches that the generic `And2Matcher` /
/// `Or2Matcher` rely on for inlining.
///
/// **Two arms.** Both run a forced fork JVM with the fusion flag flipped:
/// - `fusedMegamorphic` (`-Dhardwood.recordfilter.fusion=true`) — each
///   compiled matcher is a unique synthetic class. Inner sites
///   `a.test(row)` / `b.test(row)` do not exist; the comparison is
///   inlined into the body of the fused class.
/// - `genericMegamorphic` (`-Dhardwood.recordfilter.fusion=false`) — all
///   compiled matchers route through `And2Matcher` / `Or2Matcher`, which
///   share the same inner `a.test()` / `b.test()` bytecode locations.
///   With ≥3 distinct leaf classes the inner sites go megamorphic.
///
/// **Inner loop.** Iterates `BATCH_SIZE × SHAPE_COUNT`, calling every
/// matcher against every row. The outer site `matcher.test(row)` is
/// megamorphic in both arms; the difference is purely the body cost.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar RecordFilterMegamorphicBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(RecordFilterMegamorphicBenchmark.BATCH_SIZE
        * RecordFilterMegamorphicBenchmark.SHAPE_COUNT)
public class RecordFilterMegamorphicBenchmark {

    static final int BATCH_SIZE = 4096;
    static final int SHAPE_COUNT = 12;

    private FileSchema schema;
    private StructAccessor[] rows;
    private ResolvedPredicate[] predicates;
    private RowMatcher[] fused;
    private RowMatcher[] generic;

    @Setup
    public void setup() {
        schema = buildSchema();
        rows = buildRows(BATCH_SIZE, 42L);
        predicates = buildPredicates();
        fused = new RowMatcher[predicates.length];
        generic = new RowMatcher[predicates.length];
        for (int i = 0; i < predicates.length; i++) {
            fused[i] = RecordFilterCompiler.compile(predicates[i], schema);
            generic[i] = compileGeneric2(predicates[i], schema);
        }
    }

    @Benchmark
    public void fusedMegamorphic(Blackhole bh) {
        RowMatcher[] ms = fused;
        StructAccessor[] batch = rows;
        for (int i = 0; i < batch.length; i++) {
            StructAccessor r = batch[i];
            for (int j = 0; j < ms.length; j++) {
                bh.consume(ms[j].test(r));
            }
        }
    }

    @Benchmark
    public void genericMegamorphic(Blackhole bh) {
        RowMatcher[] ms = generic;
        StructAccessor[] batch = rows;
        for (int i = 0; i < batch.length; i++) {
            StructAccessor r = batch[i];
            for (int j = 0; j < ms.length; j++) {
                bh.consume(ms[j].test(r));
            }
        }
    }

    /// Legacy interpreter baseline: every row goes through
    /// [RecordFilterEvaluator.matchesRow], which walks the predicate tree
    /// per row with a sealed-type switch and name-keyed accessor lookups.
    /// No compiler involvement at all — this is pre-stage-1 behaviour.
    @Benchmark
    public void legacyMegamorphic(Blackhole bh) {
        ResolvedPredicate[] ps = predicates;
        StructAccessor[] batch = rows;
        FileSchema sch = schema;
        for (int i = 0; i < batch.length; i++) {
            StructAccessor r = batch[i];
            for (int j = 0; j < ps.length; j++) {
                bh.consume(RecordFilterEvaluator.matchesRow(ps[j], r, sch));
            }
        }
    }

    // ==================== Predicate construction ====================

    /// 12 distinct shapes — every fused (combo × connective) tuple appears
    /// at least once so the fused arm's outer call site sees 12 unique
    /// synthetic classes (megamorphic), while every body is direct
    /// arithmetic with no inner virtual call.
    private static ResolvedPredicate[] buildPredicates() {
        return new ResolvedPredicate[] {
                // 0: long+long same-column AND (BETWEEN)
                and(new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.LongPredicate(0, Operator.LT, 9_999L)),
                // 1: long+long same-column OR
                or(new ResolvedPredicate.LongPredicate(0, Operator.LT, -1L),
                   new ResolvedPredicate.LongPredicate(0, Operator.GT, 8_192L)),
                // 2: int+int same-column AND
                and(new ResolvedPredicate.IntPredicate(2, Operator.GT_EQ, 0),
                    new ResolvedPredicate.IntPredicate(2, Operator.LT, 100)),
                // 3: int+int same-column OR (manual; not the IN API)
                or(new ResolvedPredicate.IntPredicate(2, Operator.EQ, 5),
                   new ResolvedPredicate.IntPredicate(2, Operator.EQ, 47)),
                // 4: double+double same-column AND
                and(new ResolvedPredicate.DoublePredicate(1, Operator.GT_EQ, 0.0),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, 1_000.0)),
                // 5: long+double diff-column AND
                and(new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, 999.0)),
                // 6: long+double diff-column OR
                or(new ResolvedPredicate.LongPredicate(0, Operator.NOT_EQ, -1L),
                   new ResolvedPredicate.DoublePredicate(1, Operator.LT_EQ, 1_000.0)),
                // 7: int+long diff-column AND
                and(new ResolvedPredicate.IntPredicate(2, Operator.LT, 50),
                    new ResolvedPredicate.LongPredicate(0, Operator.GT, 100L)),
                // 8: int+double diff-column AND
                and(new ResolvedPredicate.IntPredicate(2, Operator.GT_EQ, 0),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, 500.0)),
                // 9: boolean+boolean diff-column AND (degenerate, but exercises path)
                and(new ResolvedPredicate.BooleanPredicate(3, Operator.EQ, true),
                    new ResolvedPredicate.BooleanPredicate(3, Operator.NOT_EQ, false)),
                // 10: double+long canonical-swap (canonicalises to long+double)
                and(new ResolvedPredicate.DoublePredicate(1, Operator.GT, -1.0),
                    new ResolvedPredicate.LongPredicate(0, Operator.LT, 9_999L)),
                // 11: binary+binary same-column AND
                and(new ResolvedPredicate.BinaryPredicate(4, Operator.GT_EQ,
                            "a".getBytes(StandardCharsets.UTF_8), false),
                    new ResolvedPredicate.BinaryPredicate(4, Operator.LT,
                            "z".getBytes(StandardCharsets.UTF_8), false)),
        };
    }

    private static ResolvedPredicate and(ResolvedPredicate a, ResolvedPredicate b) {
        return new ResolvedPredicate.And(List.of(a, b));
    }

    private static ResolvedPredicate or(ResolvedPredicate a, ResolvedPredicate b) {
        return new ResolvedPredicate.Or(List.of(a, b));
    }

    /// Builds a generic-shape arity-2 compound by compiling each child
    /// independently and joining via a single `and2`/`or2` helper lambda.
    /// Bypasses fusion regardless of the system property — every helper
    /// returned shares the same inner `a.test()` / `b.test()` bytecode
    /// locations, which is what makes those sites megamorphic when many
    /// leaf classes flow through.
    private static RowMatcher compileGeneric2(ResolvedPredicate p, FileSchema schema) {
        if (p instanceof ResolvedPredicate.And and && and.children().size() == 2) {
            RowMatcher a = RecordFilterCompiler.compile(and.children().get(0), schema);
            RowMatcher b = RecordFilterCompiler.compile(and.children().get(1), schema);
            return and2(a, b);
        }
        if (p instanceof ResolvedPredicate.Or or && or.children().size() == 2) {
            RowMatcher a = RecordFilterCompiler.compile(or.children().get(0), schema);
            RowMatcher b = RecordFilterCompiler.compile(or.children().get(1), schema);
            return or2(a, b);
        }
        throw new IllegalArgumentException("expected arity-2 AND/OR, got " + p);
    }

    private static RowMatcher and2(RowMatcher a, RowMatcher b) {
        return row -> a.test(row) && b.test(row);
    }

    private static RowMatcher or2(RowMatcher a, RowMatcher b) {
        return row -> a.test(row) || b.test(row);
    }

    // ==================== Schema and row stubs ====================

    private static FileSchema buildSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 5, null, null, null, null, null);
        SchemaElement id = new SchemaElement("id", PhysicalType.INT64, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement tag = new SchemaElement("tag", PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement flag = new SchemaElement("flag", PhysicalType.BOOLEAN, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement bin = new SchemaElement("bin", PhysicalType.BYTE_ARRAY, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, id, value, tag, flag, bin));
    }

    private static StructAccessor[] buildRows(int n, long seed) {
        StructAccessor[] out = new StructAccessor[n];
        Random rng = new Random(seed);
        for (int i = 0; i < n; i++) {
            byte[] bin = new byte[] { (byte) ('a' + rng.nextInt(26)) };
            out[i] = new FlatStub(i, rng.nextDouble() * 1000.0, rng.nextInt(100), rng.nextBoolean(), bin);
        }
        return out;
    }

    /// Single concrete row class — keeps the get* call sites monomorphic so
    /// any megamorphism in the benchmark numbers comes from the matcher
    /// dispatch, not the row.
    private static final class FlatStub implements StructAccessor {
        private final long idVal;
        private final double valueVal;
        private final int tagVal;
        private final boolean flagVal;
        private final byte[] binVal;

        FlatStub(long idVal, double valueVal, int tagVal, boolean flagVal, byte[] binVal) {
            this.idVal = idVal;
            this.valueVal = valueVal;
            this.tagVal = tagVal;
            this.flagVal = flagVal;
            this.binVal = binVal;
        }

        @Override public boolean isNull(String name) { return false; }
        @Override public long getLong(String name) { return idVal; }
        @Override public double getDouble(String name) { return valueVal; }
        @Override public int getInt(String name) { return tagVal; }
        @Override public boolean getBoolean(String name) { return flagVal; }
        @Override public byte[] getBinary(String name) { return binVal; }

        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalDate getDate(String name) { throw new UnsupportedOperationException(); }
        @Override public LocalTime getTime(String name) { throw new UnsupportedOperationException(); }
        @Override public Instant getTimestamp(String name) { throw new UnsupportedOperationException(); }
        @Override public BigDecimal getDecimal(String name) { throw new UnsupportedOperationException(); }
        @Override public UUID getUuid(String name) { throw new UnsupportedOperationException(); }
        @Override public PqInterval getInterval(String name) { throw new UnsupportedOperationException(); }
        @Override public PqStruct getStruct(String name) { throw new UnsupportedOperationException(); }
        @Override public PqIntList getListOfInts(String name) { throw new UnsupportedOperationException(); }
        @Override public PqLongList getListOfLongs(String name) { throw new UnsupportedOperationException(); }
        @Override public PqDoubleList getListOfDoubles(String name) { throw new UnsupportedOperationException(); }
        @Override public PqList getList(String name) { throw new UnsupportedOperationException(); }
        @Override public PqMap getMap(String name) { throw new UnsupportedOperationException(); }
        @Override public PqVariant getVariant(String name) { throw new UnsupportedOperationException(); }
        @Override public Object getValue(String name) { throw new UnsupportedOperationException(); }
        @Override public int getFieldCount() { return 5; }
        @Override public String getFieldName(int index) {
            return switch (index) {
                case 0 -> "id";
                case 1 -> "value";
                case 2 -> "tag";
                case 3 -> "flag";
                case 4 -> "bin";
                default -> throw new IndexOutOfBoundsException(index);
            };
        }
    }
}
