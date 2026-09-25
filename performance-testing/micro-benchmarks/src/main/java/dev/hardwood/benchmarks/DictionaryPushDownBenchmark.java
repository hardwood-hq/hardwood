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
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;

/// Filtered reads over a dictionary-encoded low-cardinality column, measuring both sides of
/// dictionary predicate push-down (see `_designs/STATISTICS_PRUNING.md`): the saving when the
/// dictionary proves a value absent and the row group is skipped, and the cost when it does not.
///
/// Fixtures: `dict_pushdown.parquet` (10M rows, 10 row groups, 512 distinct `cat_<even>` values)
/// and `dict_pushdown_no_stats.parquet` — run
/// `python performance-testing/generate_dict_pushdown_data.py` first.
///
/// The `probe` parameter selects a predicate and a fixture. Each probe comes as a pair — the same
/// predicate with push-down eligible and ineligible — so the `_no_dictionary` variant is the
/// control for what push-down changed:
///
/// - `absent` — `cat_1`, inside every row group's min/max but in no dictionary, so statistics
///   cannot drop it and the dictionary drops every row group without touching a data page.
/// - `absent_no_dictionary` — the same probe with push-down ineligible: nothing prunes and the
///   whole file is filtered. The work `absent` avoids.
/// - `present` — `cat_2`, in every dictionary, so every row group survives and the dictionary read
///   pruned nothing.
/// - `present_no_dictionary` — the control for `present`.
///
/// A `_no_dictionary` probe reads `dict_pushdown_no_stats.parquet`, which is
/// `dict_pushdown.parquet` with `category`'s `encoding_stats` dropped from the footer so the
/// chunk is ineligible for push-down. The files are otherwise byte-identical, so a pair differs in
/// eligibility and nothing else.
///
/// `absent` is a complete run in which nothing prunes before the dictionary probe, so its whole
/// score bounds what push-down can cost when it prunes nothing. That cost is one request per
/// filtered column per surviving row group; `DictionaryPushDownIoTest` in `hardwood-core` pins
/// that request count deterministically.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar DictionaryPushDownBenchmark -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms1g", "-Xmx1g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryPushDownBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "dict_pushdown.parquet" })
    private String fileName;

    /// Same file with `category`'s `encoding_stats` dropped from the footer, which makes the chunk
    /// ineligible for push-down. Pages, offsets, statistics and the dictionary pages themselves are
    /// byte-identical to `fileName`.
    @Param({ "dict_pushdown_no_stats.parquet" })
    private String noStatsFileName;

    @Param({ "absent", "absent_no_dictionary", "present", "present_no_dictionary" })
    private String probe;

    private Path path;
    private FilterPredicate filter;

    @Setup
    public void setup() {
        String probedFile = probe.endsWith("_no_dictionary") ? noStatsFileName : fileName;
        path = Path.of(dataDir).resolve(probedFile).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path
                    + ". Run 'python performance-testing/generate_dict_pushdown_data.py' first.");
        }
        filter = switch (probe) {
            // Odd suffix: within every row group's min/max, in no dictionary.
            case "absent", "absent_no_dictionary" -> FilterPredicate.eq("category", "cat_1");
            // Even suffix: in every dictionary, so no row group is dropped.
            case "present", "present_no_dictionary" -> FilterPredicate.eq("category", "cat_2");
            default -> throw new IllegalStateException("Unknown probe: " + probe);
        };
    }

    /// Filtered read of `payload` alone. `category` is decoded to evaluate the predicate but is not
    /// projected, which is the shape a real filtered scan takes.
    @Benchmark
    public void columnReaderPayload(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
             ColumnReader values = reader.buildColumnReader("payload").filter(filter).build()) {
            long sum = 0;
            while (values.nextBatch()) {
                long[] batch = values.getLongs();
                int count = values.getRecordCount();
                for (int i = 0; i < count; i++) {
                    sum += batch[i];
                }
            }
            blackhole.consume(sum);
        }
    }
}
