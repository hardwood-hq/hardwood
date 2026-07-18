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
import dev.hardwood.reader.RowReader;

/// End-to-end filtered reads over a sorted file, measuring what the always-match
/// statistics decision (#795) saves: when row-group min/max prove every row of a
/// group matches, per-row predicate evaluation is skipped for that group, and when
/// every surviving group fully matches the filter is dropped wholesale.
///
/// Fixture: `sorted_filter.parquet` (10M rows, 10 tight-statistics row groups) —
/// run `python performance-testing/generate_filter_pushdown_data.py` first.
///
/// The `selectivity` parameter positions one `>=` cutoff against the row-group
/// statistics:
///
/// - `noFilter` — unfiltered scan, the floor the always-match path converges to
/// - `full` — cutoff below the file minimum: every group fully matches, the
///   filter is discarded before any worker starts
/// - `half` — cutoff mid-file: dropped groups, one partially matching group
///   (evaluated per row), and fully matching groups (skipped) in one read
///
/// Each selectivity runs on two read paths: `rowReaderString` filters on the
/// string column through the record-matcher path (where per-row evaluation is
/// most expensive), `columnReaderLong` filters on `id` while draining `value`
/// through the vectorized batch-matcher path.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar AlwaysMatchReadBenchmark -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = { "-Xms1g", "-Xmx1g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class AlwaysMatchReadBenchmark {

    private static final int LABEL_WIDTH = 12;

    @Param({})
    private String dataDir;

    @Param({ "sorted_filter.parquet" })
    private String fileName;

    @Param({ "noFilter", "full", "half" })
    private String selectivity;

    private Path path;
    private FilterPredicate longFilter;
    private FilterPredicate stringFilter;

    @Setup
    public void setup() throws IOException {
        path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path
                    + ". Run 'python performance-testing/generate_filter_pushdown_data.py' first.");
        }
        long numRows;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            numRows = reader.getFileMetaData().numRows();
        }
        long cutoff = switch (selectivity) {
            case "noFilter" -> -1;
            case "full" -> 0;
            case "half" -> numRows / 2;
            default -> throw new IllegalStateException("Unknown selectivity: " + selectivity);
        };
        if (cutoff >= 0) {
            longFilter = FilterPredicate.gtEq("id", cutoff);
            stringFilter = FilterPredicate.gtEq("label",
                    String.format("%0" + LABEL_WIDTH + "d", cutoff));
        }
    }

    @Benchmark
    public void rowReaderString(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            ParquetFileReader.RowReaderBuilder builder = reader.buildRowReader();
            if (stringFilter != null) {
                builder.filter(stringFilter);
            }
            try (RowReader rows = builder.build()) {
                long idSum = 0;
                int labelLengthSum = 0;
                while (rows.hasNext()) {
                    rows.next();
                    idSum += rows.getLong("id");
                    labelLengthSum += rows.getString("label").length();
                }
                blackhole.consume(idSum);
                blackhole.consume(labelLengthSum);
            }
        }
    }

    @Benchmark
    public void columnReaderLong(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            ParquetFileReader.ColumnReaderBuilder builder = reader.buildColumnReader("value");
            if (longFilter != null) {
                builder.filter(longFilter);
            }
            try (ColumnReader values = builder.build()) {
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
}
