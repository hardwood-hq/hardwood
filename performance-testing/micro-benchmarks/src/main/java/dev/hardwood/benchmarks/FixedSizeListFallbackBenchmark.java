/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

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
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;

/// Measures the price the fixed-size-list detector pays on a page that is *almost*
/// fixed-width: every row is a present list of length `k` except one row of a
/// different length. The detector's def-gate passes (all present), so it scans the
/// repetition levels, discovers the odd row, and the whole (single) page falls back
/// to the regular decode — the detector work is wasted.
///
/// The wasted cost is `detectorThenFallback − regularNoDetector`: both do the same
/// regular decode of the same file; only the former also runs the (failing)
/// detector. `differPos` places the odd row `last` (the detector scans nearly the
/// whole level stream before failing) or `second` (it fails almost immediately).
/// `k = 4` exercises the bit-packed rep regime, `k = 768` the RLE-interior regime.
/// `flatFloor` is the plain-column decode floor.
///
/// Each file is a single data page (large `data_page_size`), so the fallback is
/// whole-file rather than per-page. Generate the corpus with
/// `generate_fixed_size_list_data.py`, then run with `-p dataDir=<dir>`.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class FixedSizeListFallbackBenchmark {

    private static final String LIST_COLUMN = "vec.list.element";
    private static final String FLAT_COLUMN = "value";

    @Param({})
    private String dataDir;

    @Param({ "4", "768" })
    private int k;

    @Param({ "last", "second" })
    private String differPos;

    private Path listPath;
    private Path flatPath;
    private HardwoodContext context;
    private ReaderConfig fastConfig;
    private ReaderConfig noFastConfig;

    @Setup(Level.Trial)
    public void setup() {
        listPath = resolve("nonfixed_k" + k + "_" + differPos + ".parquet");
        flatPath = resolve("nonfixed_k" + k + "_flat.parquet");
        context = HardwoodContext.create();
        fastConfig = ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "true").build();
        noFastConfig = ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "false").build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
    }

    /// Fast path on: the detector scans the rep levels, fails, and the page falls
    /// back to the regular decode.
    @Benchmark
    public double detectorThenFallback() throws IOException {
        return sumColumn(listPath, LIST_COLUMN, fastConfig);
    }

    /// Fast path off: the regular decode with no detector — the baseline the price
    /// is measured against.
    @Benchmark
    public double regularNoDetector() throws IOException {
        return sumColumn(listPath, LIST_COLUMN, noFastConfig);
    }

    /// The same values as a plain float column — the decode floor.
    @Benchmark
    public double flatFloor() throws IOException {
        return sumColumn(flatPath, FLAT_COLUMN, fastConfig);
    }

    private double sumColumn(Path path, String column, ReaderConfig config) throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context, config);
             ColumnReader col = reader.columnReader(column)) {
            while (col.nextBatch()) {
                float[] values = (float[]) col.getFloats();
                int n = col.getValueCount();
                for (int i = 0; i < n; i++) {
                    sum += values[i];
                }
            }
        }
        return sum;
    }

    private Path resolve(String fileName) {
        Path path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Benchmark file not found: " + path
                    + ". Run 'python performance-testing/generate_fixed_size_list_data.py " + dataDir + "' first.");
        }
        return path;
    }
}
