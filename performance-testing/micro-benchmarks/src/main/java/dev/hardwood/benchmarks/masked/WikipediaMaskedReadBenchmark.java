/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.masked;

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

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;

/// Masked reads of one shard of Wikipedia with embeddings, all columns projected: tails, and
/// `_id` range filters the column index narrows. `full` is an unmasked read of every row, for
/// comparison.
///
/// The shard has 100,000 rows in 10 row groups: article text and a list of 1,024 floats per
/// row, every element present. The `_id` ranges keep 93%, 44% and 5% of the rows.
///
/// Fixture: `wikipedia_en_0079_index.parquet`, which
/// `python performance-testing/generate_nested_masking_data.py --wikipedia` downloads
/// (`CohereLabs/wikipedia-2023-11-embed-multilingual-v3`, `en/0079.parquet`, 221 MB) and
/// rewrites with a page index.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar WikipediaMaskedReadBenchmark -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms4g", "-Xmx4g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class WikipediaMaskedReadBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "wikipedia_en_0079_index.parquet" })
    private String fileName;

    @Param({ "full", "tail5", "tail1000", "tail5000", "tailAllButOne",
            "ids93Percent", "ids44Percent", "ids5Percent" })
    private String read;

    private MaskedReads.Read reader;
    private long rows;

    @Setup
    public void setup() throws IOException {
        Path path = MaskedReads.fixture(dataDir, fileName);
        long totalRows;
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(path))) {
            totalRows = file.getFileMetaData().numRows();
        }
        reader = switch (read) {
            case "full" -> () -> MaskedReads.full(path);
            case "tail5" -> () -> MaskedReads.tail(path, 5);
            case "tail1000" -> () -> MaskedReads.tail(path, 1_000);
            case "tail5000" -> () -> MaskedReads.tail(path, 5_000);
            case "tailAllButOne" -> () -> MaskedReads.tail(path, totalRows - 1);
            case "ids93Percent" -> ids(path, "20231101.en_67000", "20231101.en_9");
            case "ids44Percent" -> ids(path, "20231101.en_68000", "20231101.en_72000");
            case "ids5Percent" -> ids(path, "20231101.en_70000", "20231101.en_70500");
            default -> throw new IllegalStateException("Unknown read: " + read);
        };
        rows = reader.rows();
    }

    @Benchmark
    public long read() throws IOException {
        return MaskedReads.checked(reader.rows(), rows);
    }

    /// The rows whose `_id` lies in `[from, to)`.
    private static MaskedReads.Read ids(Path path, String from, String to) {
        FilterPredicate filter = FilterPredicate.and(FilterPredicate.gtEq("_id", from), FilterPredicate.lt("_id", to));
        return () -> MaskedReads.filtered(path, filter);
    }
}
