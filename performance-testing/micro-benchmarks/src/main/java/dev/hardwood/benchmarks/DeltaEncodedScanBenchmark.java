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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;

/// End-to-end scan of a delta-encoded file, to show what the decoder's share of a real read
/// actually is. The decoder micro-benchmarks isolate it; this one does not.
///
/// The file is written by parquet-java at writer version 2.0, which encodes integer columns as
/// DELTA_BINARY_PACKED and string columns as DELTA_BYTE_ARRAY. It is generated into a temporary
/// directory during setup, so the benchmark carries no corpus dependency.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar DeltaEncodedScanBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DeltaEncodedScanBenchmark {

    private static final int ROW_COUNT = 2_000_000;

    private static final String SCHEMA = """
            {
              "type": "record",
              "name": "Event",
              "fields": [
                {"name": "event_id", "type": "long"},
                {"name": "timestamp", "type": "long"},
                {"name": "user_id", "type": "int"},
                {"name": "path", "type": "string"}
              ]
            }
            """;

    private Path directory;
    private Path file;

    @Setup
    public void setUp() throws IOException {
        directory = Files.createTempDirectory("hardwood-delta-scan");
        file = directory.resolve("delta_encoded.parquet");

        Schema schema = new Schema.Parser().parse(SCHEMA);
        Random random = new Random(42);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord> builder(new org.apache.hadoop.fs.Path(file.toString()))
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withDictionaryEncoding(false)
                .build()) {

            long timestamp = 1_700_000_000_000L;
            for (int i = 0; i < ROW_COUNT; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("event_id", (long) i);
                timestamp += random.nextInt(1000);
                record.put("timestamp", timestamp);
                record.put("user_id", random.nextInt(1_000_000));
                record.put("path", "/api/v1/resource/" + random.nextInt(100_000));
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
    public void scanTimestamps(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader column = reader.columnReader("timestamp")) {
            while (column.nextBatch()) {
                blackhole.consume(column.getLongs());
            }
        }
    }

    @Benchmark
    public void scanUserIds(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader column = reader.columnReader("user_id")) {
            while (column.nextBatch()) {
                blackhole.consume(column.getInts());
            }
        }
    }

    @Benchmark
    public void scanPaths(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                ColumnReader column = reader.columnReader("path")) {
            while (column.nextBatch()) {
                blackhole.consume(column.getStrings());
            }
        }
    }
}
