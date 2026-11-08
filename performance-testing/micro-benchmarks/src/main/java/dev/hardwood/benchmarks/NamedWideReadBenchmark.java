/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
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
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;

/// A flat file of `columns` `INT64` columns read row by row, every value by its column's name:
/// `getLong(name)` for each column of each row, the names being string literals' interned
/// instances. The names are [FieldNames#random], so how the reader's name map spreads them is
/// that of a real schema of that width.
///
/// The values repeat over a small range, so each column decodes from a small dictionary and
/// the name lookups are a large share of the read.
///
/// The fixtures are generated on first use into `dataDir` (`named_wide_<columns>.parquet`).
///
/// ```shell
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar NamedWideReadBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms4g", "-Xmx4g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class NamedWideReadBenchmark {

    private static final int ROWS = 500_000;
    private static final int BATCH_ROWS = 50_000;
    private static final int DISTINCT_VALUES = 64;
    private static final long NAME_SEED = 681;

    @Param({ "performance-testing/test-data-setup/target/benchmark-data" })
    private String dataDir;

    @Param({ "20", "105", "128", "129", "300" })
    private int columns;

    private Path path;
    private String[] names;
    private long expected;

    @Setup
    public void setup() throws IOException {
        names = FieldNames.random(columns, NAME_SEED + columns);
        path = Path.of(dataDir).resolve("named_wide_" + columns + ".parquet").toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            write(path, names);
        }
        expected = expectedSum(columns);
        long sum = read();
        if (sum != expected) {
            throw new IllegalStateException("Read a sum of " + sum + " from " + path + ", expected " + expected);
        }
    }

    @Benchmark
    public long read() throws IOException {
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
             RowReader rows = reader.rowReader()) {
            String[] fields = names;
            while (rows.hasNext()) {
                rows.next();
                for (String field : fields) {
                    sum += rows.getLong(field);
                }
            }
        }
        return sum;
    }

    private static long value(int row, int column) {
        return (row + column) % DISTINCT_VALUES;
    }

    private static long expectedSum(int columns) {
        long sum = 0;
        for (int c = 0; c < columns; c++) {
            for (int r = 0; r < ROWS; r++) {
                sum += value(r, c);
            }
        }
        return sum;
    }

    private static void write(Path path, String[] names) throws IOException {
        Files.createDirectories(path.getParent());
        System.out.printf("Generating %s (%d columns x %,d rows)...%n", path.getFileName(), names.length, ROWS);
        FileSchema.Builder builder = FileSchema.builder("named_wide");
        for (String name : names) {
            builder.addColumn(name, PhysicalType.INT64, RepetitionType.REQUIRED);
        }
        Path partial = path.resolveSibling(path.getFileName() + ".partial");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(partial), builder.build())) {
            ColumnWriter columnWriter = writer.columnWriter();
            for (int start = 0; start < ROWS; start += BATCH_ROWS) {
                int first = start;
                columnWriter.writeBatch(batch -> {
                    for (int c = 0; c < names.length; c++) {
                        long[] values = new long[BATCH_ROWS];
                        for (int r = 0; r < BATCH_ROWS; r++) {
                            values[r] = value(first + r, c);
                        }
                        batch.longs(c, values);
                    }
                });
            }
        }
        Files.move(partial, path);
    }
}
