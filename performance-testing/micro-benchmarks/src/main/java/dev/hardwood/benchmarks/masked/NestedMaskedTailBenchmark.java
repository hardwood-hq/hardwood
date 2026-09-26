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
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;

/// `tail(N)` reads of nested columns in a file without a page index, all columns projected. A
/// tail that starts inside a row group narrows it, and every page the read keeps there carries a
/// row mask. Without an offset index, the read finds those pages by walking their headers and
/// counts each page's records from its repetition levels.
///
/// Fixture: `nested_masking_v2_no_index_zstd.parquet`, 2,000,000 rows in two row groups, with
/// list and struct columns, v2 pages and no page index. Run
/// `python performance-testing/generate_nested_masking_data.py` first.
///
/// `tailRows` is `5`, `1000`, `halfRowGroup` (half the first row group) or `allButOne` (every
/// row but the first, which narrows only the first row group).
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar NestedMaskedTailBenchmark -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class NestedMaskedTailBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "nested_masking_v2_no_index_zstd.parquet" })
    private String fileName;

    @Param({ "5", "1000", "halfRowGroup", "allButOne" })
    private String tailRows;

    private Path path;
    private long rows;

    @Setup
    public void setup() throws IOException {
        path = MaskedReads.fixture(dataDir, fileName);
        FileMetaData metaData;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            metaData = reader.getFileMetaData();
        }
        rows = switch (tailRows) {
            case "halfRowGroup" -> metaData.rowGroups().getFirst().numRows() / 2;
            case "allButOne" -> metaData.numRows() - 1;
            default -> Long.parseLong(tailRows);
        };
    }

    @Benchmark
    public long tail() throws IOException {
        return MaskedReads.checked(MaskedReads.tail(path, rows), rows);
    }
}
