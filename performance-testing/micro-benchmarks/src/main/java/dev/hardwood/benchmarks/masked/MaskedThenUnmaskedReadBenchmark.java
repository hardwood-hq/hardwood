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
import org.openjdk.jmh.annotations.Level;
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

/// Full reads of nested columns, in a JVM that has or has not run masked reads of the same
/// file first.
///
/// Masked and unmasked pages share the code that assembles nested values. If masked pages
/// reach it, the JIT compiles that code for both shapes once a masked read has run, and the
/// unmasked reads after it run slower. With `afterMaskedReads=true`, each fork runs masked
/// tails of the file before the warm-up; with `false` it does not. The two results match when
/// masked reads leave unmasked reads unaffected.
///
/// Fixture: the Overture Maps places file with a page index,
/// `overture_places_index.zstd.parquet`, which `python performance-testing/generate_nested_masking_data.py`
/// writes when the file downloaded by `test-data-setup` is present. Any nested file with a page
/// index works through `-p fileName=...`.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar MaskedThenUnmaskedReadBenchmark -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms4g", "-Xmx4g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class MaskedThenUnmaskedReadBenchmark {

    /// Masked reads each fork runs before the warm-up when [#afterMaskedReads] is set: this
    /// many of each of two tails.
    private static final int MASKED_READS = 10;

    @Param({})
    private String dataDir;

    @Param({ "overture_places_index.zstd.parquet" })
    private String fileName;

    @Param({ "false", "true" })
    private boolean afterMaskedReads;

    private Path path;
    private long rows;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        path = MaskedReads.fixture(dataDir, fileName);
        long rowGroupRows;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            FileMetaData metaData = reader.getFileMetaData();
            rows = metaData.numRows();
            rowGroupRows = metaData.rowGroups().getFirst().numRows();
        }
        if (afterMaskedReads) {
            for (int i = 0; i < MASKED_READS; i++) {
                MaskedReads.checked(MaskedReads.tail(path, rows - 1), rows - 1);
                MaskedReads.checked(MaskedReads.tail(path, rowGroupRows / 2), rowGroupRows / 2);
            }
        }
    }

    @Benchmark
    public long fullRead() throws IOException {
        return MaskedReads.checked(MaskedReads.full(path), rows);
    }
}
