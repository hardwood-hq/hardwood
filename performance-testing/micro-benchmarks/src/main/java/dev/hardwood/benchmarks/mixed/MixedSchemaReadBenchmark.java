/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.mixed;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.HardwoodContext;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.NestedReads;
import dev.hardwood.reader.ParquetFileReader;

/// Schema-composition effects on the nested read path — the workload behind #732
/// (per-column-worker routing), plus the shapes whose acceptance criteria demand a
/// no-regression guard.
///
/// - `columnScalarsMixed` vs `columnScalarsFlat` — scan the same 12 non-repeated
///   scalar columns out of a file that also holds two `LIST` columns
///   (`mixed.parquet`, routed onto the nested reader today) versus a file with only
///   the scalars (`flat_scalars.parquet`, routed onto the flat reader). The delta is
///   the per-scalar-column tax #732 removes.
/// - `columnStruct` vs `columnStructFlat` — a non-repeated `STRUCT` of primitives
///   versus the same leaves as top-level columns (the flat-struct path #732 subsumes).
/// - `columnRepeatedHeavy` — several `LIST` columns and no scalars: the no-regression
///   guard for repetition-heavy schemas.
/// - `columnListOfList` / `rowListOfList` / `columnListOfStruct` / `rowListOfStruct` —
///   columns with more than one repetition layer, where #751's single-repeated-layer
///   fast path deliberately does not fire; these must not regress.
///
/// All leaves are numeric. Fixtures are generated on demand from `@Setup`.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class MixedSchemaReadBenchmark {

    private static final int SCALAR_GROUP = 4;

    private Path mixedPath;
    private Path flatScalarsPath;
    private Path structPath;
    private Path structFlatPath;
    private Path repeatedHeavyPath;
    private Path listOfListPath;
    private Path listOfStructPath;
    private HardwoodContext context;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path dir = Path.of(BenchmarkData.dir());
        long rows = BenchmarkData.rows();
        long totalValues = BenchmarkData.totalValues();
        MixedSchemaFileGenerator.ensureMixed(dir, rows);
        MixedSchemaFileGenerator.ensureStruct(dir, rows);
        MixedSchemaFileGenerator.ensureRepeatedHeavy(dir, totalValues);
        MixedSchemaFileGenerator.ensureListOfList(dir, totalValues);
        MixedSchemaFileGenerator.ensureListOfStruct(dir, totalValues);
        mixedPath = MixedSchemaFileGenerator.mixedFile(dir);
        flatScalarsPath = MixedSchemaFileGenerator.flatScalarsFile(dir);
        structPath = MixedSchemaFileGenerator.structFile(dir);
        structFlatPath = MixedSchemaFileGenerator.structFlatFile(dir);
        repeatedHeavyPath = MixedSchemaFileGenerator.repeatedHeavyFile(dir);
        listOfListPath = MixedSchemaFileGenerator.listOfListFile(dir);
        listOfStructPath = MixedSchemaFileGenerator.listOfStructFile(dir);
        context = HardwoodContext.create();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
    }

    @Benchmark
    public double columnScalarsMixed() throws IOException {
        return sumScalars(mixedPath);
    }

    @Benchmark
    public double columnScalarsFlat() throws IOException {
        return sumScalars(flatScalarsPath);
    }

    @Benchmark
    public double columnStruct() throws IOException {
        return sumStruct(structPath, "s.a", "s.b", "s.c");
    }

    @Benchmark
    public double columnStructFlat() throws IOException {
        return sumStruct(structFlatPath, "a", "b", "c");
    }

    @Benchmark
    public double columnRepeatedHeavy() throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = NestedReads.open(repeatedHeavyPath, context)) {
            for (int c = 0; c < 4; c++) {
                sum += NestedReads.sumDoubleColumn(reader, "vec" + c + ".list.element");
            }
        }
        return sum;
    }

    @Benchmark
    public double columnListOfList() throws IOException {
        try (ParquetFileReader reader = NestedReads.open(listOfListPath, context)) {
            return NestedReads.sumDoubleColumn(reader, 0);
        }
    }

    @Benchmark
    public double rowListOfList() throws IOException {
        return NestedReads.sumRowsListOfList(listOfListPath, "vec", context);
    }

    @Benchmark
    public double columnListOfStruct() throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = NestedReads.open(listOfStructPath, context)) {
            sum += NestedReads.sumLongColumn(reader, 0);
            sum += NestedReads.sumDoubleColumn(reader, 1);
        }
        return sum;
    }

    @Benchmark
    public double rowListOfStruct() throws IOException {
        return NestedReads.sumRowsListOfStruct(listOfStructPath, "vec", context);
    }

    /// Folds the 12 non-repeated scalar columns (`i0..i3`, `l0..l3`, `d0..d3`) of a
    /// file, sharing one reader across all of them.
    private double sumScalars(Path path) throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = NestedReads.open(path, context)) {
            for (int c = 0; c < SCALAR_GROUP; c++) {
                sum += NestedReads.sumIntColumn(reader, "i" + c);
            }
            for (int c = 0; c < SCALAR_GROUP; c++) {
                sum += NestedReads.sumLongColumn(reader, "l" + c);
            }
            for (int c = 0; c < SCALAR_GROUP; c++) {
                sum += NestedReads.sumDoubleColumn(reader, "d" + c);
            }
        }
        return sum;
    }

    private double sumStruct(Path path, String a, String b, String c) throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = NestedReads.open(path, context)) {
            sum += NestedReads.sumLongColumn(reader, a);
            sum += NestedReads.sumDoubleColumn(reader, b);
            sum += NestedReads.sumIntColumn(reader, c);
        }
        return sum;
    }
}
