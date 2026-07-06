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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

/// Scans a low-cardinality, dictionary-encoded string column end to end via
/// [ColumnReader#getStrings()]. Intended to be run with `-prof gc`: the headline
/// metric is `gc.alloc.rate.norm` (bytes allocated per full-column scan), which
/// isolates the effect of reusing one interned `String` per dictionary entry
/// versus decoding a fresh `String` per value.
///
/// Fixture: `dict_strings.parquet` (`label`, 8 distinct values over 2M rows) —
/// run `python performance-testing/generate_dict_string_data.py` first.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = { "-Xms1g", "-Xmx1g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryStringReadBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "dict_strings.parquet" })
    private String fileName;

    private Path path;

    @Setup
    public void setup() {
        path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path
                    + ". Run 'python performance-testing/generate_dict_string_data.py' first.");
        }
    }

    @Benchmark
    public void readStrings(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
                ColumnReader column = reader.columnReader("label")) {
            while (column.nextBatch()) {
                String[] values = column.getStrings();
                for (String value : values) {
                    blackhole.consume(value);
                }
            }
        }
    }
}
