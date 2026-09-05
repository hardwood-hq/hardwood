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
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

/// Scans a dictionary-encoded column at cardinalities that straddle the bit width where the
/// index decode changes path.
///
/// A dictionary index is bit-packed at the width needed for the largest index, so a dictionary
/// of at most 256 entries decodes at 8 bits or fewer and anything larger does not. `cardinality`
/// is chosen to sit either side of that: 200 entries needs 8 bits, 10000 needs 14.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar DictionaryIndexScanBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryIndexScanBenchmark {

    private static final int ROW_COUNT = 2_000_000;

    private static final String SCHEMA = """
            {
              "type": "record",
              "name": "Row",
              "fields": [
                {"name": "label", "type": "string"},
                {"name": "value", "type": "int"}
              ]
            }
            """;

    /// 200 distinct values pack indices into 8 bits; 10000 needs 14.
    ///
    /// The upper value cannot be raised much further: past roughly 50000 entries the writer
    /// abandons the dictionary and falls back to PLAIN, so the column carries no indices at all
    /// and measures nothing about index decoding.
    @Param({ "200", "10000" })
    private int cardinality;

    private Path directory;
    private Path file;

    @Setup
    public void setUp() throws IOException {
        directory = Files.createTempDirectory("hardwood-dict-scan");
        file = directory.resolve("dictionary.parquet");

        Schema schema = new Schema.Parser().parse(SCHEMA);
        Random random = new Random(42);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord> builder(new org.apache.hadoop.fs.Path(file.toString()))
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                // One row group, so the whole column shares a dictionary of the full cardinality
                // rather than several smaller per-group dictionaries at narrower index widths.
                .withRowGroupSize(1024L * 1024 * 1024)
                .withDictionaryPageSize(64 * 1024 * 1024)
                .build()) {

            for (int i = 0; i < ROW_COUNT; i++) {
                GenericRecord record = new GenericData.Record(schema);
                int draw = random.nextInt(cardinality);
                record.put("label", "value-" + draw);
                // Same cardinality, so both columns carry indices of the same bit width; this
                // one resolves to a primitive and so is not dominated by String construction.
                record.put("value", draw * 7);
                writer.write(record);
            }
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        try (Stream<Path> entries = Files.walk(directory)) {
            entries.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    @Benchmark
    public void scanLabels(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader column = reader.columnReader("label")) {
            while (column.nextBatch()) {
                blackhole.consume(column.getStrings());
            }
        }
    }

    @Benchmark
    public void scanValues(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader column = reader.columnReader("value")) {
            while (column.nextBatch()) {
                blackhole.consume(column.getInts());
            }
        }
    }
}
