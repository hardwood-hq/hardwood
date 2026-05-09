/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.math.BigDecimal;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.predicate.RecordFilterCompiler;
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

/// Predicate evaluation micro-benchmark, isolated from any I/O.
///
/// Measures the per-row cost of [RecordFilterCompiler.compile] + [RowMatcher.test]
/// across a range of predicate shapes that stress different aspects of the
/// dispatch path:
///
/// - `single*` — one leaf, no recursion
/// - `and2/3/4` — N-leaf `AND`, exercises recursion + iterator allocation
/// - `or2` — short-circuiting `OR`
/// - `nested` — `AND(OR(...), ...)`, mixed dispatch
/// - `intIn5/intIn32` — IN-list with linear search vs binary search
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar RecordFilterMicroBenchmark -rf json -rff record-filter-jmh.json
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(RecordFilterMicroBenchmark.BATCH_SIZE)
public class RecordFilterMicroBenchmark {

    static final int BATCH_SIZE = 4096;

    @Param({ "single", "and2", "and3", "and4", "or2", "nested", "intIn5", "intIn32" })
    public String shape;

    private StructAccessor[] rows;
    private RowMatcher compiled;

    @Setup
    public void setup() {
        FileSchema schema = buildSchema();
        rows = buildRows(BATCH_SIZE, 42L);
        ResolvedPredicate predicate = buildPredicate(shape);
        compiled = RecordFilterCompiler.compile(predicate, schema);
    }

    @Benchmark
    public void compiled(Blackhole bh) {
        RowMatcher m = compiled;
        StructAccessor[] batch = rows;
        for (int i = 0; i < batch.length; i++) {
            bh.consume(m.test(batch[i]));
        }
    }

    // ==================== Fixtures ====================

    /// Schema with four flat columns: id (INT64), value (DOUBLE), tag (INT32),
    /// flag (BOOLEAN). All required.
    private static FileSchema buildSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 4, null, null, null, null, null);
        SchemaElement id = new SchemaElement("id", PhysicalType.INT64, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.DOUBLE, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement tag = new SchemaElement("tag", PhysicalType.INT32, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        SchemaElement flag = new SchemaElement("flag", PhysicalType.BOOLEAN, null, RepetitionType.REQUIRED,
                null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, id, value, tag, flag));
    }

    private static StructAccessor[] buildRows(int n, long seed) {
        StructAccessor[] out = new StructAccessor[n];
        Random rng = new Random(seed);
        for (int i = 0; i < n; i++) {
            long idVal = i;
            double valueVal = rng.nextDouble() * 1000.0;
            int tagVal = rng.nextInt(100);
            boolean flagVal = rng.nextBoolean();
            out[i] = new FlatStub(idVal, valueVal, tagVal, flagVal);
        }
        return out;
    }

    /// Builds predicate shapes that all match every row in the batch.
    /// Match-all is the worst case for predicate-evaluation cost — every
    /// branch is taken to completion, no early-exits hide the work.
    private static ResolvedPredicate buildPredicate(String shape) {
        return switch (shape) {
            case "single" -> new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L);
            case "and2" -> new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, Double.MAX_VALUE)));
            case "and3" -> new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, Double.MAX_VALUE),
                    new ResolvedPredicate.IntPredicate(2, Operator.GT_EQ, 0)));
            case "and4" -> new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, Double.MAX_VALUE),
                    new ResolvedPredicate.IntPredicate(2, Operator.GT_EQ, 0),
                    new ResolvedPredicate.BooleanPredicate(3, Operator.NOT_EQ, false)));
            case "or2" -> new ResolvedPredicate.Or(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.DoublePredicate(1, Operator.LT, 0.0)));
            case "nested" -> new ResolvedPredicate.And(List.of(
                    new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
                    new ResolvedPredicate.Or(List.of(
                            new ResolvedPredicate.DoublePredicate(1, Operator.LT, 0.0),
                            new ResolvedPredicate.BooleanPredicate(3, Operator.NOT_EQ, false))),
                    new ResolvedPredicate.IntPredicate(2, Operator.LT, Integer.MAX_VALUE)));
            case "intIn5" -> intIn(2, 5);
            case "intIn32" -> intIn(2, 32);
            default -> throw new IllegalArgumentException(shape);
        };
    }

    private static ResolvedPredicate intIn(int columnIndex, int size) {
        int[] vals = new int[size];
        for (int i = 0; i < size; i++) {
            vals[i] = i; // matches tag values 0..size-1
        }
        return new ResolvedPredicate.IntInPredicate(columnIndex, vals);
    }

    /// Minimal four-field StructAccessor — only the accessors used by the
    /// benchmarked predicates are implemented. All others throw, keeping
    /// the call sites monomorphic for HotSpot.
    private static final class FlatStub implements StructAccessor {
        private final long idVal;
        private final double valueVal;
        private final int tagVal;
        private final boolean flagVal;

        FlatStub(long idVal, double valueVal, int tagVal, boolean flagVal) {
            this.idVal = idVal;
            this.valueVal = valueVal;
            this.tagVal = tagVal;
            this.flagVal = flagVal;
        }

        @Override public boolean isNull(String name) { return false; }
        @Override public long getLong(String name) { return idVal; }
        @Override public double getDouble(String name) { return valueVal; }
        @Override public int getInt(String name) { return tagVal; }
        @Override public boolean getBoolean(String name) { return flagVal; }

        @Override public float getFloat(String name) { throw new UnsupportedOperationException(); }
        @Override public String getString(String name) { throw new UnsupportedOperationException(); }
        @Override public byte[] getBinary(String name) { throw new UnsupportedOperationException(); }
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
        @Override public int getFieldCount() { return 4; }
        @Override public String getFieldName(int index) {
            return switch (index) {
                case 0 -> "id";
                case 1 -> "value";
                case 2 -> "tag";
                case 3 -> "flag";
                default -> throw new IndexOutOfBoundsException(index);
            };
        }
    }
}
