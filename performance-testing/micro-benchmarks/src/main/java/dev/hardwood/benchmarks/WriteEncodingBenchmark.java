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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// What an encoding policy costs to produce and what it does to the file, over the flat
/// fixture [FlatWriteBenchmark] measures the three write APIs on.
///
/// [FlatWriteBenchmark] is deliberately untouched by this class: its number is the one the
/// throughput stages before this one were argued against, and comparability across them is
/// worth more than one class carrying every axis. The two share [FlatWriteFixture] and nothing
/// else.
///
/// This benchmark carries **Hardwood's columnar API alone**. An encoding policy is a question
/// about this writer rather than about the gap to parquet-java, which [FlatWriteBenchmark]
/// measures; and both Hardwood APIs reach the same encoder through the same column-chunk
/// buffer, so the row layer's staging — which [FlatWriteBenchmark] also measures — would move
/// every case by the same amount.
///
/// ## The cases
///
/// Each case names a policy and the columns it applies to, leaving every other column on
/// [ColumnEncoding#AUTO]. A case rather than a bare [ColumnEncoding] because the legal
/// (policy, physical type) pairs are not a rectangle: a file-wide `DELTA_BINARY_PACKED` over
/// this schema is rejected at writer creation, the schema holding columns that cannot carry it.
///
/// | Case | Policy | Applied to | The question |
/// |---|---|---|---|
/// | `AUTO` | — | nothing | today's behaviour, the baseline the others are read against |
/// | `PLAIN_ON_DISTINCT` | `PLAIN` | `id`, `pickup_ts`, `fare` | what interning a column that discards its dictionary costs |
/// | `DELTA_INTEGERS` | `DELTA_BINARY_PACKED` | `id`, `pickup_ts`, `passenger_count` | what delta buys on ascending integers, and what producing it costs |
/// | `SPLIT_NUMERIC` | `BYTE_STREAM_SPLIT` | `fare`, `id`, `pickup_ts` | whether reordering bytes pays for itself once the codec runs |
///
/// `PLAIN_ON_DISTINCT` is not an encoding recommendation, it is a measurement. A column under
/// a named policy builds no dictionary at all, so applying `PLAIN` to exactly the columns the
/// flush-time comparison rejects anyway writes the same pages as `AUTO` while skipping the
/// interning that produced them. **The gap between the two cases is what the writer's most
/// expensive per-value decision costs.**
///
/// ## The codecs
///
/// `ZSTD` is [WriterConfig]'s default wherever zstd-jni is on the classpath, so it is the
/// configuration nearly every produced file actually uses, and it is the only one under which
/// `BYTE_STREAM_SPLIT` means anything — the encoding changes no page's size by itself and
/// reorders bytes so the codec after it finds structure.
///
/// `UNCOMPRESSED` is not a second codec under evaluation but the control: at `ZSTD` the codec
/// owns a large share of the time and attenuates every encode-side movement, so `UNCOMPRESSED`
/// is the reading where an encode-path change shows at full size, and the ratio between the two
/// is how much of the number such a change can reach at all.
///
/// ## Reading the result
///
/// **Produced file size is reported from the trial setup**, per configuration. On a codec axis
/// size is one half of a trade; on an encoding axis it is most of the question, and a case that
/// encodes faster into a larger file has not won.
///
/// The setup also reads each file back and asserts that every column the case names actually
/// carries the encoding it asked for. A policy that silently failed to apply would otherwise
/// report `AUTO`'s number under another case's name, which is worse than no number.
///
/// Run the matrix with:
///
/// ```
/// ./mvnw -Pperformance-test -pl performance-testing/micro-benchmarks -am package -Dquick
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar WriteEncodingBenchmark -prof gc
/// ```
///
/// Profile a single point with a **probe** instead — one fork and few iterations, so a profiler
/// pass costs seconds rather than re-running the matrix:
///
/// ```
/// java -jar .../benchmarks.jar WriteEncodingBenchmark -f 1 -wi 2 -i 3 -r 5 -w 5 \
///     -p encoding=AUTO -p codec=UNCOMPRESSED -prof perfnorm
/// ```
///
/// Keep `-Dperf.rows` at its default for a probe. At fewer rows the fixture stops filling a row
/// group, and a profile that never reaches a flush is a profile of half the writer.
///
/// **Give an iteration several operations.** One write costs about 150 ms on a developer
/// machine and about a second on slower hardware, so the one-second iterations this class
/// declares can degenerate to a single operation per sample and fold each GC pause wholly into
/// one of them. `-r 5 -w 5` is what any number worth quoting is measured with.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// jvmArgsAppend, not jvmArgs, as in every benchmark in this module: the latter replaces the
// inherited command line, which would drop the -Dperf.* properties the fork's setup reads.
@Fork(value = 2, jvmArgsAppend = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WriteEncodingBenchmark {

    /// Records per [ColumnWriter#writeBatch] call, as in [FlatWriteBenchmark], so a number
    /// here is read against one there.
    private static final int BATCH_ROWS = 1024;

    private static final int PAGE_TARGET_BYTES = 1 << 20;
    private static final long ROW_GROUP_TARGET_BYTES = 16L << 20;

    /// The columns whose values are all distinct on this fixture, and whose dictionaries the
    /// flush-time comparison therefore rejects. `passenger_count`, `payment_type` and `vendor`
    /// are the low-cardinality ones a dictionary wins on.
    private static final List<String> ALL_DISTINCT = List.of("id", "pickup_ts", "fare");

    /// One encoding case: the policy each named column is written under.
    enum Case {

        AUTO(Map.of()),
        PLAIN_ON_DISTINCT(policy(ColumnEncoding.PLAIN, ALL_DISTINCT)),
        DELTA_INTEGERS(policy(ColumnEncoding.DELTA_BINARY_PACKED,
                List.of("id", "pickup_ts", "passenger_count"))),
        SPLIT_NUMERIC(policy(ColumnEncoding.BYTE_STREAM_SPLIT, List.of("fare", "id", "pickup_ts")));

        private final Map<String, ColumnEncoding> columns;

        Case(Map<String, ColumnEncoding> columns) {
            this.columns = columns;
        }

        private static Map<String, ColumnEncoding> policy(ColumnEncoding encoding, List<String> columns) {
            Map<String, ColumnEncoding> policies = new LinkedHashMap<>();
            for (String column : columns) {
                policies.put(column, encoding);
            }
            return policies;
        }

        /// The policies this case sets, keyed by the column's dotted leaf path.
        Map<String, ColumnEncoding> columns() {
            return columns;
        }
    }

    @Param({ "AUTO", "PLAIN_ON_DISTINCT", "DELTA_INTEGERS", "SPLIT_NUMERIC" })
    private String encoding;

    @Param({ "ZSTD", "UNCOMPRESSED" })
    private String codec;

    private FlatWriteFixture fixture;
    private FileSchema schema;
    private WriterConfig config;

    @Setup
    public void setUp() throws IOException {
        fixture = FlatWriteFixture.generate(FlatWriteFixture.configuredRows(), BATCH_ROWS);
        schema = FlatWriteFixture.schema();
        Case encodingCase = Case.valueOf(encoding);
        config = configFor(encodingCase);

        reportProducedFile(encodingCase);
    }

    private WriterConfig configFor(Case encodingCase) {
        WriterConfig.Builder builder = WriterConfig.builder()
                .pageTargetBytes(PAGE_TARGET_BYTES)
                .rowGroupBufferTargetBytes(ROW_GROUP_TARGET_BYTES)
                .codec(CompressionCodec.valueOf(codec));
        encodingCase.columns().forEach(builder::encoding);
        return builder.build();
    }

    @Benchmark
    public long hardwoodColumnar() throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        write(out, config);
        return out.position();
    }

    /// Hands each batch's column arrays to the writer as they are, exactly as
    /// [FlatWriteBenchmark]'s columnar contender does, so the only thing this benchmark varies
    /// is the configuration it writes under.
    private void write(InMemoryOutputFile out, WriterConfig writerConfig) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, writerConfig)) {
            ColumnWriter columns = writer.columnWriter();
            for (int b = 0; b < fixture.batchCount(); b++) {
                int index = b;
                columns.writeBatch(batch -> batch
                        .longs("id", fixture.id[index])
                        .longs("pickup_ts", fixture.pickupMicros[index])
                        .ints("passenger_count", fixture.passengerCount[index], fixture.passengerCountNulls[index])
                        .doubles("fare", fixture.fare[index])
                        .bytes("payment_type", fixture.paymentTypeBytes[index])
                        .bytes("vendor", fixture.vendorBytes[index], fixture.vendorNulls[index]));
            }
        }
    }

    /// Writes the fixture once, checks that the file holds the records the fixture has and that
    /// every column carries the encoding this case asked for, and prints its size — so a time is
    /// never read without the size it bought, nor under a case name it did not honour.
    private void reportProducedFile(Case encodingCase) throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        write(out, config);
        ByteBuffer file = out.buffer();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            long rows = reader.getFileMetaData().numRows();
            if (rows != fixture.rows()) {
                throw new IllegalStateException(
                        encoding + " wrote " + rows + " rows, expected " + fixture.rows());
            }
            checkEncodings(reader, encodingCase);
            int rowGroups = reader.getFileMetaData().rowGroups().size();
            System.out.printf("%nWriteEncodingBenchmark: %,d rows, encoding %s, codec %s%n",
                    fixture.rows(), encoding, codec);
            System.out.printf("  %,15d bytes, %d row groups%n", file.remaining(), rowGroups);
            if (encodingCase == Case.PLAIN_ON_DISTINCT) {
                checkMatchesAuto(file.remaining(), rowGroups);
            }
        }
    }

    /// Asserts that every column the case names carries the encoding it asked for and no
    /// dictionary, and — under [Case#AUTO] — that the fixture still splits into the
    /// dictionary-rejecting and dictionary-keeping columns the cases are built around.
    private void checkEncodings(ParquetFileReader reader, Case encodingCase) {
        for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
            for (int c = 0; c < rowGroup.columns().size(); c++) {
                ColumnMetaData column = rowGroup.columns().get(c).metaData();
                String path = column.pathInSchema().toString();
                ColumnEncoding policy = encodingCase.columns().get(path);
                if (policy != null) {
                    // The named encoding, and no dictionary: a column under a policy builds none.
                    requireEncoding(path, column.encodings(), expected(policy), true);
                    requireEncoding(path, column.encodings(), Encoding.RLE_DICTIONARY, false);
                }
                else if (encodingCase == Case.AUTO) {
                    requireEncoding(path, column.encodings(), Encoding.RLE_DICTIONARY,
                            !ALL_DISTINCT.contains(path));
                }
            }
        }
    }

    private static Encoding expected(ColumnEncoding policy) {
        return switch (policy) {
            case PLAIN -> Encoding.PLAIN;
            case DELTA_BINARY_PACKED -> Encoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY -> Encoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY -> Encoding.DELTA_BYTE_ARRAY;
            case BYTE_STREAM_SPLIT -> Encoding.BYTE_STREAM_SPLIT;
            case AUTO -> throw new IllegalStateException("AUTO names no encoding to check for");
        };
    }

    private static void requireEncoding(String column, List<Encoding> encodings, Encoding expected,
                                        boolean present) {
        if (encodings.contains(expected) != present) {
            throw new IllegalStateException("Column " + column + " should " + (present ? "" : "not ")
                    + "carry " + expected + " but its chunk declares " + encodings);
        }
    }

    /// Holds the equivalence `PLAIN_ON_DISTINCT` is built on: it writes the same pages as
    /// `AUTO`, so the two files differ only in the `distinct_count` the `AUTO` chunks state and
    /// the policied ones cannot. A divergence beyond that means the comparison is keeping a
    /// dictionary for a column this case declares all-distinct, and the case has stopped
    /// measuring what it claims to.
    private void checkMatchesAuto(int size, int rowGroups) throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        write(out, configFor(Case.AUTO));
        int autoSize = out.buffer().remaining();
        // Every chunk of a policied column loses a distinct_count, a Thrift i64 behind its field
        // header. Budgeting 64 bytes per chunk of the whole schema is far above what those fields
        // can cost and far below the smallest difference an encoding change would make.
        long slack = 64L * schema.getColumnCount() * rowGroups;
        if (Math.abs((long) autoSize - size) > slack) {
            throw new IllegalStateException("PLAIN_ON_DISTINCT produced " + size
                    + " bytes against AUTO's " + autoSize + "; the two should differ only in distinct_count");
        }
    }
}
