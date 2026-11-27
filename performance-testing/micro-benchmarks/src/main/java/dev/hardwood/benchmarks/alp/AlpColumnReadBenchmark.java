/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.alp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// A `DOUBLE` column read end to end through [ColumnReader], in nanoseconds per row, with `ALP`
/// against `PLAIN` as the reference.
///
/// Setup writes one file of [#ROWS] rows with Hardwood's writer: a `REQUIRED` column `required`
/// and an `OPTIONAL` column `optional` holding the same `decimal` values (see [AlpBenchmarkData]),
/// a tenth of its rows null at random. Each invocation opens the file and reads one column, on the
/// calling thread (`HardwoodContext.create(0)`), so the number is the cost of one core.
///
/// Run with:
/// ```shell
/// java -jar benchmarks.jar AlpColumnReadBenchmark -p codec=UNCOMPRESSED,ZSTD
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms2g", "-Xmx2g", "--enable-native-access=ALL-UNNAMED" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class AlpColumnReadBenchmark {

    private static final int ROWS = 1 << 22;
    private static final int BATCH = 1 << 16;

    @Param({ "ALP", "PLAIN" })
    private String encoding;

    @Param({ "UNCOMPRESSED" })
    private String codec;

    @Param({ "required", "optional" })
    private String column;

    private Path file;
    private HardwoodContext context;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        file = Files.createTempFile("alp-column-read", ".parquet");
        FileSchema schema = FileSchema.builder("benchmark")
                .addColumn("required", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("optional", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.valueOf(codec))
                .encoding(ColumnEncoding.valueOf(encoding))
                .build();
        double[] values = AlpBenchmarkData.doubles("decimal", ROWS);
        Random random = new Random(7);
        boolean[] nulls = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            nulls[i] = random.nextInt(10) == 0;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            for (int from = 0; from < ROWS; from += BATCH) {
                double[] batchValues = Arrays.copyOfRange(values, from, from + BATCH);
                boolean[] batchNulls = Arrays.copyOfRange(nulls, from, from + BATCH);
                writer.columnWriter().writeBatch(batch -> batch.doubles("required", batchValues)
                        .doubles("optional", batchValues, Validity.ofNulls(batchNulls)));
            }
        }
        context = HardwoodContext.create(0);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        context.close();
        Files.deleteIfExists(file);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void read(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
                ColumnReader columnReader = reader.columnReader(column)) {
            while (columnReader.nextBatch()) {
                blackhole.consume(columnReader.getDoubles());
            }
        }
    }
}
