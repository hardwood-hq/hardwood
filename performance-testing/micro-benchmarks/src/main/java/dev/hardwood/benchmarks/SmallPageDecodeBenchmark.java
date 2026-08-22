/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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

/// Measures full-column decode throughput across the page-size and compression
/// axes from #810. Every fixture contains the same eight million float32 values.
///
/// Generate the corpus and run the benchmark:
/// ```
/// python performance-testing/generate_small_page_decode_data.py <dataDir>
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
///     SmallPageDecodeBenchmark -p dataDir=<dataDir>
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class SmallPageDecodeBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "small", "large" })
    private String pagination;

    @Param({ "zstd", "uncompressed" })
    private String compression;

    @Param({ "false", "true" })
    private boolean expandedWindow;

    private HardwoodContext context;
    private Path path;

    @Setup(Level.Trial)
    public void setup() {
        System.setProperty("hardwood.internal.smallPageWindow", Boolean.toString(expandedWindow));
        context = HardwoodContext.create();
        path = Path.of(dataDir)
                .resolve("small_page_decode_" + pagination + "_" + compression + ".parquet")
                .toAbsolutePath()
                .normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Benchmark file not found: " + path);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
        System.clearProperty("hardwood.internal.smallPageWindow");
    }

    @Benchmark
    public double decodeAndSum() throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
             ColumnReader column = reader.columnReader("value")) {
            while (column.nextBatch()) {
                float[] values = column.getFloats();
                int count = column.getValueCount();
                for (int i = 0; i < count; i++) {
                    sum += values[i];
                }
            }
        }
        return sum;
    }
}
