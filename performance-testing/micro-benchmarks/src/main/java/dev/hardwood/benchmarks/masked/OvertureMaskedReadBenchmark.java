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

/// Masked reads of the Overture Maps places file, all columns projected: tails, and bounding-box
/// filters the column index narrows. `full` is an unmasked read of every row, for comparison.
///
/// The file has 5,124,095 rows in 512 row groups, with nested structs, lists and maps. A tail
/// narrows the row group it starts in; a bounding-box filter narrows every row group whose pages
/// it partly keeps.
///
/// Fixture: `overture_places_index.zstd.parquet`, which
/// `python performance-testing/generate_nested_masking_data.py` writes from the file the
/// `test-data-setup` module downloads.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar OvertureMaskedReadBenchmark -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms4g", "-Xmx4g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class OvertureMaskedReadBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "overture_places_index.zstd.parquet" })
    private String fileName;

    @Param({ "full", "tail5", "tail1000", "tail50000", "tailAllButOne",
            "bboxDowntownChicago", "bboxToronto", "bboxChicagoArea", "bboxLargeRegion" })
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
            case "tail50000" -> () -> MaskedReads.tail(path, 50_000);
            case "tailAllButOne" -> () -> MaskedReads.tail(path, totalRows - 1);
            case "bboxDowntownChicago" -> bbox(path, -87.7, -87.6, 41.85, 41.92);
            case "bboxToronto" -> bbox(path, -79.6, -79.1, 43.58, 43.86);
            case "bboxChicagoArea" -> bbox(path, -88.0, -87.5, 41.6, 42.1);
            case "bboxLargeRegion" -> bbox(path, -84, -76.5, 42, 47);
            default -> throw new IllegalStateException("Unknown read: " + read);
        };
        rows = reader.rows();
    }

    @Benchmark
    public long read() throws IOException {
        return MaskedReads.checked(reader.rows(), rows);
    }

    /// The places whose bounding box's lower-left corner lies in `[xMin, xMax) × [yMin, yMax)`.
    private static MaskedReads.Read bbox(Path path, double xMin, double xMax, double yMin, double yMax) {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("bbox.xmin", xMin), FilterPredicate.lt("bbox.xmin", xMax),
                FilterPredicate.gtEq("bbox.ymin", yMin), FilterPredicate.lt("bbox.ymin", yMax));
        return () -> MaskedReads.filtered(path, filter);
    }
}
