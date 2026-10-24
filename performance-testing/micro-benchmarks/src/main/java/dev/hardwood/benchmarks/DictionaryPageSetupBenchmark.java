/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// The per-column-chunk cost of setting up a dictionary on the sequential read path.
///
/// That cost is paid once per dictionary-encoded column chunk, so the fixture is shaped to put
/// it in the foreground: many row groups, each carrying a wide dictionary of long values over
/// few rows, which leaves the dictionary page an order of magnitude larger than the data page
/// referencing it. An ordinary file pays exactly the same setup cost and buries it under value
/// decoding, so a scan of one is a test of whether the setup regressed, not a measurement of it.
///
/// The fixture is written by Hardwood's own writer, which emits no page index — that is what puts
/// the read on [dev.hardwood.internal.reader.fetch.SequentialFetchPlan] rather than the indexed
/// plan — and which stamps a CRC on every page. The `dictionaryValues` sweep widens the dictionary
/// the header describes and the checksum covers.
///
/// The fixture is read into memory once and scanned from there, because the cost under test is
/// decode work: served off a file it is a rounding error next to the bytes moved.
///
/// Self-generating: fixtures are written under `dataDir` on first run and reused after. The
/// widest is around 100 MB.
///
/// ```shell
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar DictionaryPageSetup \
///     -p dataDir=performance-testing/test-data-setup/target/benchmark-data
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryPageSetupBenchmark {

    /// Row groups in the fixture, and so dictionary pages the scan sets up.
    private static final int ROW_GROUPS = 100;

    /// Rows per row group. Twice the widest dictionary, so every value repeats and the writer
    /// keeps the chunk dictionary-encoded rather than falling back to `PLAIN`.
    private static final int ROWS_PER_ROW_GROUP = 8192;

    /// Bytes per value. Long values are what make the dictionary page large next to the page of
    /// indices pointing into it, which is the ratio this benchmark needs.
    private static final int VALUE_BYTES = 256;

    @Param({})
    private String dataDir;

    /// Distinct values per dictionary. The dictionary body grows with this, and so does the
    /// checksum computed over it.
    @Param({ "1024", "4096" })
    private int dictionaryValues;

    private ByteBuffer fixture;

    @Setup
    public void setup() throws IOException {
        Path dir = Path.of(dataDir).toAbsolutePath().normalize();
        Files.createDirectories(dir);
        Path path = dir.resolve("dict_page_setup_" + dictionaryValues + ".parquet");
        if (!Files.exists(path)) {
            write(path, dictionaryValues);
        }
        fixture = ByteBuffer.wrap(Files.readAllBytes(path));
    }

    @Benchmark
    public void scanDictionaryEncodedColumn(Blackhole blackhole) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(fixture));
                ColumnReader column = reader.columnReader("label")) {
            while (column.nextBatch()) {
                blackhole.consume(column.getStrings());
            }
        }
    }

    private static void write(Path path, int dictionaryValues) throws IOException {
        FileSchema schema = FileSchema.builder("dict_page_setup")
                .addColumn("label", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        // Uncompressed so the dictionary body the checksum covers is the dictionary itself,
        // not whatever a codec squeezed it down to.
        WriterConfig config = WriterConfig.builder()
                .rowGroupTargetRows(ROWS_PER_ROW_GROUP)
                .codec(CompressionCodec.UNCOMPRESSED)
                .build();

        byte[][] values = new byte[ROWS_PER_ROW_GROUP][];
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
            for (int rowGroup = 0; rowGroup < ROW_GROUPS; rowGroup++) {
                for (int row = 0; row < ROWS_PER_ROW_GROUP; row++) {
                    // Distinct across row groups, so no dictionary is shared and each chunk
                    // carries its own page of `dictionaryValues` entries.
                    values[row] = value(rowGroup, row % dictionaryValues);
                }
                byte[][] batch = values;
                writer.columnWriter().writeBatch(b -> b.bytes(0, batch));
            }
        }
    }

    /// A [#VALUE_BYTES]-long value, unique to the `(rowGroup, ordinal)` pair, padded out so that
    /// what makes the dictionary large is value length rather than entry count alone.
    private static byte[] value(int rowGroup, int ordinal) {
        byte[] bytes = new byte[VALUE_BYTES];
        byte[] prefix = ("rg" + rowGroup + "-label-" + ordinal + "-")
                .getBytes(StandardCharsets.UTF_8);
        System.arraycopy(prefix, 0, bytes, 0, Math.min(prefix.length, VALUE_BYTES));
        for (int i = prefix.length; i < VALUE_BYTES; i++) {
            bytes[i] = (byte) ('a' + (i % 26));
        }
        return bytes;
    }
}
