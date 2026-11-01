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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Performance test for the row reader over a file whose top-level fields mix primitives with
/// non-repeated structs of primitives: `id`, `location{lat, lon}`, `metrics{value1, value2, count}`,
/// `score` and `category`, 10M rows, the structs and their fields optional and never null.
///
/// Each contender reads `id`, `location.lat` and `score` from every row. The file is generated
/// on the first run.
///
/// Run:
///   ./mvnw test -Pperformance-test -pl :hardwood-performance-testing-end-to-end \
///     -Dtest=FlatStructPerformanceTest -Dperf.contenders=HARDWOOD_ROW_READER_NAMED -Dperf.runs=5
class FlatStructPerformanceTest {

    private static final Path DATA_FILE = Path.of(
            "../test-data-setup/target/flat-struct-perf-data/flat_struct_perf.parquet");
    private static final int ROWS = 10_000_000;
    private static final int WARMUPS = 3;
    private static final int DEFAULT_RUNS = 5;
    private static final String RUNS_PROPERTY = "perf.runs";
    private static final String CONTENDERS_PROPERTY = "perf.contenders";

    enum Contender {
        HARDWOOD_ROW_READER_INDEXED("Hardwood (row reader indexed)"),
        HARDWOOD_ROW_READER_NAMED("Hardwood (row reader named)");

        private final String displayName;

        Contender(String displayName) {
            this.displayName = displayName;
        }

        static Contender fromString(String name) {
            for (Contender c : values()) {
                if (c.name().equalsIgnoreCase(name)) {
                    return c;
                }
            }
            throw new IllegalArgumentException("Unknown contender: " + name
                    + ". Valid values: " + Arrays.toString(values()));
        }
    }

    record Sums(long idSum, double latSum, double scoreSum, long rowCount) {}

    @FunctionalInterface
    private interface Reader {
        Sums read() throws IOException;
    }

    @Test
    void comparePerformance() throws IOException {
        ensureDataFileExists();
        int runs = Integer.parseInt(System.getProperty(RUNS_PROPERTY, String.valueOf(DEFAULT_RUNS)));
        Set<Contender> contenders = enabledContenders();

        System.out.println("\n=== Flat Struct Performance Test ===");
        System.out.println("File: " + DATA_FILE + String.format(" (%,.1f MB)", Files.size(DATA_FILE) / (1024.0 * 1024.0)));
        System.out.println("Contenders: " + contenders);
        System.out.println("Runs: " + runs);

        for (Contender contender : contenders) {
            Reader reader = switch (contender) {
                case HARDWOOD_ROW_READER_INDEXED -> FlatStructPerformanceTest::readIndexed;
                case HARDWOOD_ROW_READER_NAMED -> FlatStructPerformanceTest::readNamed;
            };
            measure(contender, reader, runs);
        }
    }

    private static void measure(Contender contender, Reader reader, int runs) throws IOException {
        System.out.println("\n" + contender.displayName);
        System.out.println("Warmup runs (" + WARMUPS + "):");
        Sums expected = null;
        for (int i = 0; i < WARMUPS; i++) {
            long start = System.nanoTime();
            Sums sums = reader.read();
            System.out.printf("  Warmup [%d/%d]: %.2f s%n", i + 1, WARMUPS, (System.nanoTime() - start) / 1e9);
            expected = check(expected, sums);
        }
        System.out.println("Timed runs:");
        List<Long> durationsMs = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            Sums sums = reader.read();
            long durationMs = (System.nanoTime() - start) / 1_000_000;
            durationsMs.add(durationMs);
            System.out.printf("  [%d/%d]: %.2f s%n", i + 1, runs, durationMs / 1000.0);
            expected = check(expected, sums);
        }
        printResults(contender, durationsMs);
    }

    /// Checks that a run read every row and the sums every earlier run read.
    private static Sums check(Sums expected, Sums actual) {
        assertThat(actual.rowCount()).isEqualTo(ROWS);
        assertThat(actual.idSum()).isEqualTo((long) ROWS * (ROWS - 1) / 2);
        if (expected != null) {
            assertThat(actual).isEqualTo(expected);
        }
        return actual;
    }

    private static Sums readIndexed() throws IOException {
        long idSum = 0;
        double latSum = 0.0;
        double scoreSum = 0.0;
        long rowCount = 0;
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(DATA_FILE));
             RowReader rows = file.buildRowReader().projection(ColumnProjection.columns("id", "location", "score")).build()) {
            while (rows.hasNext()) {
                rows.next();
                rowCount++;
                if (!rows.isNull(0)) {
                    idSum += rows.getLong(0);
                }
                PqStruct location = rows.getStruct(1);
                if (location != null && !location.isNull(0)) {
                    latSum += location.getDouble(0);
                }
                if (!rows.isNull(2)) {
                    scoreSum += rows.getDouble(2);
                }
            }
        }
        return new Sums(idSum, latSum, scoreSum, rowCount);
    }

    private static Sums readNamed() throws IOException {
        long idSum = 0;
        double latSum = 0.0;
        double scoreSum = 0.0;
        long rowCount = 0;
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(DATA_FILE));
             RowReader rows = file.buildRowReader().projection(ColumnProjection.columns("id", "location", "score")).build()) {
            while (rows.hasNext()) {
                rows.next();
                rowCount++;
                if (!rows.isNull("id")) {
                    idSum += rows.getLong("id");
                }
                PqStruct location = rows.getStruct("location");
                if (location != null && !location.isNull("lat")) {
                    latSum += location.getDouble("lat");
                }
                if (!rows.isNull("score")) {
                    scoreSum += rows.getDouble("score");
                }
            }
        }
        return new Sums(idSum, latSum, scoreSum, rowCount);
    }

    private static Set<Contender> enabledContenders() {
        String property = System.getProperty(CONTENDERS_PROPERTY);
        if (property == null || property.isBlank()) {
            return EnumSet.of(Contender.HARDWOOD_ROW_READER_INDEXED);
        }
        if (property.equalsIgnoreCase("all")) {
            return EnumSet.allOf(Contender.class);
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Contender::fromString)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Contender.class)));
    }

    private static void printResults(Contender contender, List<Long> durationsMs) throws IOException {
        int cores = Runtime.getRuntime().availableProcessors();
        double megabytes = Files.size(DATA_FILE) / (1024.0 * 1024.0);
        System.out.println("\n" + "=".repeat(100));
        System.out.println("FLAT STRUCT PERFORMANCE TEST RESULTS: " + contender.displayName);
        System.out.println("=".repeat(100));
        System.out.println("CPU cores:   " + cores);
        System.out.println("Total rows:  " + String.format("%,d", ROWS));
        System.out.println("File size:   " + String.format("%,.1f MB", megabytes));
        System.out.println();
        System.out.println(String.format("  %-10s %12s %15s %18s %12s",
                "Run", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(70));
        for (int i = 0; i < durationsMs.size(); i++) {
            printRow("[" + (i + 1) + "]", durationsMs.get(i), cores, megabytes);
        }
        long total = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long ms : durationsMs) {
            total += ms;
            min = Math.min(min, ms);
            max = Math.max(max, ms);
        }
        printRow("[AVG]", total / durationsMs.size(), cores, megabytes);
        System.out.printf("  min: %.2fs, max: %.2fs, spread: %.2fs%n", min / 1000.0, max / 1000.0, (max - min) / 1000.0);
        System.out.println("=".repeat(100));
    }

    private static void printRow(String label, long durationMs, int cores, double megabytes) {
        double seconds = durationMs / 1000.0;
        double recordsPerSecond = ROWS / seconds;
        System.out.printf("  %-10s %12.2f %,15.0f %,18.0f %12.1f%n",
                label, seconds, recordsPerSecond, recordsPerSecond / cores, megabytes / seconds);
    }

    /// Writes the 10M-row file once: `location` and `metrics` are optional structs of optional
    /// fields, every value present, with no dictionary encoding.
    private static void ensureDataFileExists() throws IOException {
        if (Files.exists(DATA_FILE) && Files.size(DATA_FILE) > 0) {
            return;
        }
        System.out.println("Generating " + DATA_FILE + " (" + ROWS / 1_000_000 + "M rows)...");
        Files.createDirectories(DATA_FILE.getParent());

        Schema location = Schema.createRecord("location_t", null, "dev.hardwood.perf", false);
        location.setFields(List.of(optional("lat", Schema.Type.DOUBLE), optional("lon", Schema.Type.DOUBLE)));
        Schema metrics = Schema.createRecord("metrics_t", null, "dev.hardwood.perf", false);
        metrics.setFields(List.of(optional("value1", Schema.Type.DOUBLE), optional("value2", Schema.Type.DOUBLE),
                optional("count", Schema.Type.LONG)));
        Schema schema = Schema.createRecord("flat_struct_perf", null, "dev.hardwood.perf", false);
        schema.setFields(List.of(
                optional("id", Schema.create(Schema.Type.LONG)),
                optional("location", location),
                optional("metrics", metrics),
                optional("score", Schema.create(Schema.Type.DOUBLE)),
                optional("category", Schema.create(Schema.Type.STRING))));

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord> builder(new LocalOutputFile(DATA_FILE))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withDictionaryEncoding(false)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            Random random = new Random(42);
            for (int i = 0; i < ROWS; i++) {
                GenericRecord loc = new GenericData.Record(location);
                loc.put("lat", (double) (i % 90));
                loc.put("lon", (double) (i % 180));
                GenericRecord met = new GenericData.Record(metrics);
                met.put("value1", (double) i);
                met.put("value2", (double) i * 2);
                met.put("count", (long) (i % 100));
                GenericRecord row = new GenericData.Record(schema);
                row.put("id", (long) i);
                row.put("location", loc);
                row.put("metrics", met);
                row.put("score", random.nextDouble() * 100.0);
                row.put("category", i % 2 == 0 ? "type_a" : "type_b");
                writer.write(row);
            }
        }
        System.out.println(String.format("Generated %s (%,.1f MB)", DATA_FILE, Files.size(DATA_FILE) / (1024.0 * 1024.0)));
    }

    private static Schema.Field optional(String name, Schema.Type type) {
        return optional(name, Schema.create(type));
    }

    private static Schema.Field optional(String name, Schema schema) {
        return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), schema), null,
                Schema.Field.NULL_DEFAULT_VALUE);
    }
}
