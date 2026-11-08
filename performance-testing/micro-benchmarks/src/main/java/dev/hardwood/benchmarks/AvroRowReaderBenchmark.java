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

import org.apache.avro.generic.GenericRecord;
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
import dev.hardwood.avro.AvroReaders;
import dev.hardwood.avro.AvroRowReader;
import dev.hardwood.reader.ParquetFileReader;

/// Reads the leading rows of a file into Avro `GenericRecord`s through `hardwood-avro`, all
/// columns projected. The binding resolves every field of every record by the name the file's
/// schema gives it, twice per field (the null test, then the getter), so the cost of resolving a
/// name by the schema's own string is a large part of each record.
///
/// Every invocation opens the file again, so each reads with a schema whose strings are new: the
/// names a lookup passes are the footer's, while the reader's name maps hold whichever instance
/// the JVM interned first.
///
/// - `overture-maps-data/overture_places.zstd.parquet`: Overture Maps places, nested structs,
///   lists and maps.
/// - `tlc-trip-record-data/yellow_tripdata_2016-01.parquet`: NYC taxi trips, 19 flat columns.
///
/// Both files are the ones the `test-data-setup` module downloads, found below `dataRoot`.
///
/// ```shell
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar AvroRowReaderBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms4g", "-Xmx4g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class AvroRowReaderBenchmark {

    /// The directory the `test-data-setup` module downloads into.
    @Param({ "performance-testing/test-data-setup/target" })
    private String dataRoot;

    @Param({ "overture-maps-data/overture_places.zstd.parquet",
            "tlc-trip-record-data/yellow_tripdata_2016-01.parquet" })
    private String file;

    /// Rows read per invocation, from the start of the file.
    @Param({ "1000000" })
    private long rows;

    private Path path;

    @Setup
    public void setup() throws IOException {
        path = Path.of(dataRoot).resolve(file).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw new IllegalStateException("Parquet file not found: " + path
                    + ". Run the test-data-setup module, or pass -p dataRoot=<directory>.");
        }
        long read = read(null);
        if (read != rows) {
            throw new IllegalStateException("Read " + read + " rows of " + path + ", expected " + rows);
        }
    }

    @Benchmark
    public long read(Blackhole blackhole) throws IOException {
        long count = 0;
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
             AvroRowReader reader = AvroReaders.buildRowReader(fileReader).head(rows).build()) {
            while (reader.hasNext()) {
                GenericRecord record = reader.next();
                if (blackhole != null) {
                    blackhole.consume(record);
                }
                count++;
            }
        }
        return count;
    }
}
