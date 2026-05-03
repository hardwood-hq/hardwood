/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end record-filter scenarios on a 10 M-row file. Each `@Test` is one
/// scenario timed independently to keep JIT warmup ordering from biasing later
/// cells. Acts as the production regression baseline now that drain-side
/// filtering is the default for eligible queries.
///
/// Schema:
/// - `id` (long, sequential 0..N)
/// - `value` (double, uniform 0..1000)
/// - `tag` (int, uniform 0..99)
/// - `flag` (boolean, uniform)
///
/// Each scenario row in the printed table is annotated with whether the predicate
/// is **drain-eligible** (column-local AND with ≥ 2 distinct columns and only
/// supported `(type, op)` pairs) or **fallback** (single-leaf, OR, same-column
/// duplicates, IN-list as the only leaf, etc. — see [BatchFilterCompiler]).
///
/// Run:
/// ```
/// ./mvnw -pl core install -DskipTests
/// ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
///     -Dtest="RecordFilterScenariosBenchmarkTest" -Dperf.runs=5
/// ```
class RecordFilterScenariosBenchmarkTest {

    private static final Path BENCHMARK_FILE = Path.of("target/drain_comparison_benchmark.parquet");
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int DEFAULT_RUNS = 5;

    private static int runs;

    @BeforeAll
    static void setupOnce() throws IOException {
        runs = Integer.parseInt(System.getProperty("perf.runs", String.valueOf(DEFAULT_RUNS)));
        ensureBenchmarkFileExists();
        System.out.printf("%n=== Record-Filter Scenarios ===%n");
        System.out.printf("File: %s (%d MB), rows: %,d, runs/scenario: %d%n%n",
                BENCHMARK_FILE, Files.size(BENCHMARK_FILE) / (1024 * 1024), TOTAL_ROWS, runs);
        System.out.printf("  %-58s %12s %14s %18s%n",
                "Scenario", "Time (ms)", "Records/sec", "Path");
        System.out.println("  " + "-".repeat(108));
    }

    // ============================================================================
    // Drain-eligible scenarios
    // ============================================================================

    @Test
    void and2_longDouble_matchAll() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 0L),
                FilterPredicate.lt("value", Double.MAX_VALUE));
        run("and2  long+double           match-all     ", filter, TOTAL_ROWS, "drain (2 cols)");
    }

    @Test
    void and3_longDoubleInt_matchAll() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 0L),
                FilterPredicate.lt("value", Double.MAX_VALUE),
                FilterPredicate.gtEq("tag", 0));
        run("and3  long+double+int       match-all     ", filter, TOTAL_ROWS, "drain (3 cols)");
    }

    @Test
    void and4_longDoubleIntBoolean_halfPass() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 0L),
                FilterPredicate.lt("value", Double.MAX_VALUE),
                FilterPredicate.gtEq("tag", 0),
                FilterPredicate.notEq("flag", false));
        run("and4  long+double+int+bool  ~50%          ", filter, -1, "drain (4 cols)");
    }

    @Test
    void selective_compound_001pct() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.lt("id", 10_000L),
                FilterPredicate.lt("value", Double.MAX_VALUE));
        run("and2  selective ~0.1%       sparse        ", filter, 10_000, "drain (2 cols)");
    }

    @Test
    void selective_compound_50pct() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 0L),
                FilterPredicate.lt("value", 500.0));
        run("and2  mid selectivity ~50%  half          ", filter, -1, "drain (2 cols)");
    }

    @Test
    void empty_result() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.lt("id", 0L),
                FilterPredicate.lt("value", Double.MAX_VALUE));
        run("and2  empty result          zero matches  ", filter, 0, "drain (2 cols)");
    }

    // ============================================================================
    // Fallback scenarios — the gate trips, FilteredRowReader handles them
    // ============================================================================

    @Test
    void singleLeaf_fallsBackToCompiled() throws Exception {
        FilterPredicate filter = FilterPredicate.gtEq("id", 0L);
        run("single  long >= 0           match-all     ", filter, TOTAL_ROWS, "fallback (gate)");
    }

    @Test
    void or_fallsBackToCompiled() throws Exception {
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.lt("id", 0L),
                FilterPredicate.lt("value", 500.0));
        run("or  long || double          half          ", filter, -1, "fallback (or)");
    }

    @Test
    void sameColumnRange_fallsBackToCompiled() throws Exception {
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 1_000_000L),
                FilterPredicate.lt("id", 2_000_000L),
                FilterPredicate.lt("value", Double.MAX_VALUE));
        run("range  id BETWEEN + value   ~10%          ", filter, 1_000_000, "fallback (dup col)");
    }

    @Test
    void intIn_singleLeaf_fallsBackToCompiled() throws Exception {
        FilterPredicate filter = FilterPredicate.in("tag", new int[] {1, 5, 10, 25, 50});
        run("intIn5  selective ~5%       sparse        ", filter, -1, "fallback (gate)");
    }

    // ============================================================================
    // Harness
    // ============================================================================

    private static void run(String name, FilterPredicate filter, long expectedRows, String pathHint)
            throws Exception {
        // One warmup run before timing.
        runRead(filter);

        long[] times = new long[runs];
        long rows = 0;
        for (int i = 0; i < runs; i++) {
            long t0 = System.nanoTime();
            rows = runRead(filter);
            times[i] = System.nanoTime() - t0;
        }

        double avgMs = avg(times) / 1_000_000.0;
        System.out.printf("  %-58s %12.2f %,14.0f %18s%n",
                name, avgMs, rows / (avgMs / 1000.0), pathHint);

        if (expectedRows >= 0) {
            assertThat(rows).isEqualTo(expectedRows);
        }
    }

    private static long runRead(FilterPredicate filter) throws Exception {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private static void ensureBenchmarkFileExists() throws IOException {
        if (Files.exists(BENCHMARK_FILE) && Files.size(BENCHMARK_FILE) > 0) {
            return;
        }
        System.out.printf("Generating benchmark file (%d M rows, 4 columns)...%n",
                TOTAL_ROWS / 1_000_000);

        Schema schema = SchemaBuilder.record("benchmark")
                .fields()
                .requiredLong("id")
                .requiredDouble("value")
                .requiredInt("tag")
                .requiredBoolean("flag")
                .endRecord();

        Configuration conf = new Configuration();
        conf.set("parquet.writer.version", "v2");

        org.apache.hadoop.fs.Path hadoopPath =
                new org.apache.hadoop.fs.Path(BENCHMARK_FILE.toAbsolutePath().toString());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) TOTAL_ROWS * 16)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withPageWriteChecksumEnabled(false)
                .build()) {

            Random rng = new Random(42);
            for (int i = 0; i < TOTAL_ROWS; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", (long) i);
                record.put("value", rng.nextDouble() * 1000.0);
                record.put("tag", rng.nextInt(100));
                record.put("flag", rng.nextBoolean());
                writer.write(record);
            }
        }
        System.out.printf("Generated %s (%d MB)%n",
                BENCHMARK_FILE, Files.size(BENCHMARK_FILE) / (1024 * 1024));
    }

    private static double avg(long[] values) {
        long total = 0;
        for (long v : values) {
            total += v;
        }
        return (double) total / values.length;
    }
}
