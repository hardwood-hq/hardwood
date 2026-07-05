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

/// The general nested read path over a top-level `LIST<primitive>`, isolated against a
/// flat decode floor. This is the workload the reconstruction improvements (#750
/// bulk-copy of present runs, #751 `RealView` on the drain) target, kept numeric so
/// the measured time is level handling and value copy rather than string decode.
///
/// - `columnNested` — the [dev.hardwood.reader.ColumnReader] real-items path: pull
///   each batch's compacted leaf array and fold it. This is what #750/#751 accelerate.
/// - `rowNested` — the [dev.hardwood.reader.RowReader] all-items path: materialize the
///   `PqList` per row and fold its elements. #751 must not regress this path.
/// - `flatFloor` — the identical leaf stream as a plain primitive column read through
///   the column reader: the fastest these bytes move with no list structure.
///
/// The gap between `columnNested`/`rowNested` and `flatFloor` is the reconstruction
/// cost; an improvement moves the nested numbers toward the floor while the floor
/// itself stays put. `nullDensity` sweeps the run structure the bulk copy depends on
/// — nulls and null/empty lists break the contiguous present runs #750 copies in one
/// shot. Element type spans a 4-byte and an 8-byte leaf.
///
/// Generate the corpus first (also done on demand from `@Setup`):
/// ```
/// java ... dev.hardwood.benchmarks.nested.NestedListFileGenerator <dataDir>
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class NestedListReadBenchmark {

    private static final String LIST_FIELD = "vec";
    private static final String FLAT_COLUMN = "value";

    @Param({ "int64", "float64" })
    private String elem;

    @Param({ "none", "sparse", "dense" })
    private String nullDensity;

    private Elem elemKind;
    private Path listPath;
    private Path flatPath;
    private HardwoodContext context;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        elemKind = Elem.valueOf(elem.toUpperCase());
        NullDensity density = NullDensity.valueOf(nullDensity.toUpperCase());
        Path dir = Path.of(BenchmarkData.dir());
        long totalValues = BenchmarkData.totalValues();
        NestedListFileGenerator.ensureList(dir, elemKind, density, totalValues);
        listPath = NestedListFileGenerator.listFile(dir, elemKind, density);
        flatPath = NestedListFileGenerator.listFlatFile(dir, elemKind, density);
        context = HardwoodContext.create();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
    }

    @Benchmark
    public double columnNested() throws IOException {
        return NestedReads.sumColumn(listPath, NestedListFileGenerator.LIST_LEAF, elemKind, context);
    }

    @Benchmark
    public double rowNested() throws IOException {
        return NestedReads.sumRowsList(listPath, LIST_FIELD, elemKind, context);
    }

    @Benchmark
    public double flatFloor() throws IOException {
        return NestedReads.sumColumn(flatPath, FLAT_COLUMN, elemKind, context);
    }
}
