/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.nested;

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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.HardwoodContext;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.Elem;
import dev.hardwood.benchmarks.NestedReads;
import dev.hardwood.benchmarks.nested.NestedListFileGenerator.NullDensity;

/// The general nested read path over **many** top-level `LIST<primitive>` columns
/// folded through one consumer thread, against the same flat decode floor. The
/// single-column [NestedListReadBenchmark] hides consumer-side reconstruction cost
/// once it is moved onto the per-column drains, because one consumer folding one
/// column is rarely the bottleneck; this fans the fold across
/// [NestedListFileGenerator#MULTI_COLUMN_COUNT] columns so the shared consumer does
/// that many columns' work while the drains reconstruct in parallel.
///
/// - `columnMulti` — one [dev.hardwood.reader.ColumnReader] per column, all folded
///   on the calling thread.
/// - `rowMulti` — the [dev.hardwood.reader.RowReader] all-items path over every list
///   field per row.
/// - `flatMultiFloor` — the identical leaf streams as plain primitive columns: the
///   fastest these bytes move with no list structure.
///
/// The fixture holds the same total leaf count as the single-column one, split
/// evenly across the columns, so per-file byte volume is comparable.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class NestedMultiListReadBenchmark {

    @Param({ "int64", "float64" })
    private String elem;

    @Param({ "none", "sparse", "dense" })
    private String nullDensity;

    private Elem elemKind;
    private Path listPath;
    private Path flatPath;
    private String[] listLeaves;
    private String[] listFields;
    private String[] flatColumns;
    private HardwoodContext context;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        elemKind = Elem.valueOf(elem.toUpperCase());
        NullDensity density = NullDensity.valueOf(nullDensity.toUpperCase());
        Path dir = Path.of(BenchmarkData.dir());
        long totalValues = BenchmarkData.totalValues();
        NestedListFileGenerator.ensureMultiList(dir, elemKind, density, totalValues);
        listPath = NestedListFileGenerator.multiListFile(dir, elemKind, density);
        flatPath = NestedListFileGenerator.multiListFlatFile(dir, elemKind, density);
        listLeaves = NestedListFileGenerator.multiListLeaves();
        listFields = NestedListFileGenerator.multiListFields();
        flatColumns = NestedListFileGenerator.multiFlatColumns();
        context = HardwoodContext.create();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
    }

    @Benchmark
    public double columnMulti() throws IOException {
        return NestedReads.sumColumns(listPath, listLeaves, elemKind, context);
    }

    @Benchmark
    public double rowMulti() throws IOException {
        return NestedReads.sumRowsMultiList(listPath, listFields, elemKind, context);
    }

    @Benchmark
    public double flatMultiFloor() throws IOException {
        return NestedReads.sumColumns(flatPath, flatColumns, elemKind, context);
    }
}
