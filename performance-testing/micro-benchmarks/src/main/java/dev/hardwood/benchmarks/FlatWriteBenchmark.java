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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
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
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

/// Encodes a flat, taxi-shaped fixture through Hardwood's columnar and row write APIs and
/// parquet-java's record writer, the write-side counterpart of the read path's
/// `FlatPerformanceTest`.
///
/// | Contender | API |
/// |-----------|-----|
/// | `hardwoodColumnar` | [ColumnWriter#writeBatch], 1024-row batches |
/// | `hardwoodRow` | [ParquetFileWriter#rowWriter()] → [RowWriter#writeRow], fields set by name |
/// | `hardwoodRowByIndex` | as `hardwoodRow`, fields set by index |
/// | `hardwoodRowByIndexRaw` | as `hardwoodRowByIndex`, with pre-encoded `BYTE_ARRAY` values and raw timestamps |
/// | `parquetJavaGroup` | parquet-java's [ExampleParquetWriter] over `SimpleGroup` |
///
/// **parquet-java has no columnar write API** — its `WriteSupport` is record-at-a-time by
/// construction — so the comparison is really the record-shaped APIs head to head, with
/// Hardwood's columnar API as the ceiling no row API can beat.
///
/// What each contender pays inside the measured region is the cost of its own API, which is
/// not the same cost for all of them:
///
/// - `parquetJavaGroup` builds one `SimpleGroup` per record. That object is inherent to
///   parquet-java's design, so it belongs in the number, but the gap is not pure encoding
///   speed.
/// - `hardwoodRow` and `hardwoodRowByIndex` write `pickup_ts` through
///   [dev.hardwood.writer.StructBuilder#setTimestamp], so the annotated-value conversion the
///   other contenders do not perform is in their numbers. That is
///   what a caller holding records actually pays. The [Instant] objects themselves come from
///   the fixture, so their allocation is outside the measured region and only the conversion
///   and the pointer chase are inside it.
/// - parquet-java writes a column index and an offset index per column chunk, which Hardwood
///   does not produce yet, so its files carry a little metadata Hardwood's do not.
/// - The Hardwood contenders write into `ByteBufferOutputFile` and parquet-java into
///   [MemoryOutputFile], which are not the same sink. Both accumulate into a
///   `ByteArrayOutputStream`; `ByteBufferOutputFile` takes a [ByteBuffer] and appends the
///   array behind it, so neither side copies the payload twice on the way to the buffer.
///
/// Everything a caller can match is matched: page target, row-group target, codec, dictionary
/// encoding, writer version, and page checksums. The dictionary page limit is parquet-java's
/// alone — Hardwood chooses a chunk's encoding by comparing sizes rather than by consulting a
/// limit, so there is nothing to match it to. The row-group target is an
/// explicit 16 MiB on both sides so a million rows produces a handful of row groups and the
/// flush path is exercised, rather than a single group at the 128 MiB default. **The size of
/// each produced file is reported from the trial setup**, because a contender that is faster
/// and writes a larger file has not won.
///
/// Both sides write to memory, so the number is encode throughput rather than the container's
/// I/O noise. Correctness is not asserted per invocation — the write path's round-trip,
/// equivalence and interop tests cover it — beyond the trial setup reading each produced file
/// back through Hardwood and checking its row count.
///
/// Run it with:
///
/// ```
/// ./mvnw -Pperformance-test -pl performance-testing/micro-benchmarks -am package -Dquick
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar FlatWriteBenchmark -prof gc
/// ```
///
/// `-Dperf.rows` sets the record count (default one million, roughly 50 MB of source values);
/// `-Dperf.dir` writes to files in that directory instead of to memory, for the case where
/// end-to-end cost including the filesystem is the question.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// jvmArgsAppend, not jvmArgs, as in every benchmark in this module: the latter replaces the
// inherited command line, which would drop the -Dperf.* properties the fork's setup reads.
@Fork(value = 2, jvmArgsAppend = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class FlatWriteBenchmark {

    /// Records per [ColumnWriter#writeBatch] call, the arrival unit of a columnar
    /// producer. It is also what [RowWriter] stages before submitting a batch, so both
    /// Hardwood contenders reach the encoder in batches of the same size.
    private static final int BATCH_ROWS = 1024;

    private static final int PAGE_TARGET_BYTES = 1 << 20;
    private static final long ROW_GROUP_TARGET_BYTES = 16L << 20;
    /// parquet-java's dictionary page limit. Hardwood has no counterpart: it decides a chunk's
    /// encoding by comparing sizes rather than by consulting a limit, so this is one setting the
    /// two sides cannot match.
    private static final int PARQUET_JAVA_DICTIONARY_PAGE_LIMIT_BYTES = 1 << 20;

    private static final String COLUMNAR_FILE = "hardwood-columnar.parquet";
    private static final String ROW_FILE = "hardwood-row.parquet";
    private static final String ROW_INDEXED_FILE = "hardwood-row-indexed.parquet";
    private static final String ROW_RAW_FILE = "hardwood-row-raw.parquet";

    /// The record's fields in the order [FlatWriteFixture#schema] declares them, which is what a
    /// writer's field index is a position in.
    private static final int ID = 0;
    private static final int PICKUP_TS = 1;
    private static final int PASSENGER_COUNT = 2;
    private static final int FARE = 3;
    private static final int PAYMENT_TYPE = 4;
    private static final int VENDOR = 5;
    private static final String PARQUET_JAVA_FILE = "parquet-java-group.parquet";

    private static final String PARQUET_JAVA_SCHEMA = """
            message flat {
              required int64 id;
              required int64 pickup_ts (TIMESTAMP(MICROS,true));
              optional int32 passenger_count;
              required double fare;
              required binary payment_type (STRING);
              optional binary vendor (STRING);
            }
            """;

    /// The codec dimension, across what both writers produce. `BROTLI` is Hardwood-only —
    /// parquet-java resolves it through `org.apache.hadoop.io.compress.BrotliCodec`, which is
    /// not on its classpath — so including it would report one contender against nothing.
    @Param({ "UNCOMPRESSED", "LZ4_RAW", "SNAPPY", "ZSTD", "GZIP" })
    private String codec;

    private FlatWriteFixture fixture;
    private FileSchema hardwoodSchema;
    private WriterConfig writerConfig;
    private MessageType parquetJavaSchema;
    private Configuration hadoopConf;
    private CompressionCodecName parquetJavaCodec;

    /// Destination directory, or null when both sides write to memory.
    private Path dir;

    /// One contender's write, so the destination handling is written once for both Hardwood
    /// APIs rather than per benchmark method.
    @FunctionalInterface
    private interface HardwoodWrite {
        void writeTo(OutputFile out) throws IOException;
    }

    @Setup
    public void setUp() throws IOException {
        String configured = System.getProperty("perf.dir");
        dir = configured == null || configured.isBlank() ? null : Files.createDirectories(Path.of(configured));

        fixture = FlatWriteFixture.generate(FlatWriteFixture.configuredRows(), BATCH_ROWS);
        hardwoodSchema = FlatWriteFixture.schema();
        writerConfig = WriterConfig.builder()
                .pageTargetBytes(PAGE_TARGET_BYTES)
                .rowGroupBufferTargetBytes(ROW_GROUP_TARGET_BYTES)
                .codec(CompressionCodec.valueOf(codec))
                .build();
        parquetJavaSchema = MessageTypeParser.parseMessageType(PARQUET_JAVA_SCHEMA);
        parquetJavaCodec = CompressionCodecName.valueOf(codec);
        hadoopConf = new Configuration();
        // Hadoop's LocalFileSystem writes a .crc sidecar beside every file it creates: a second
        // checksum pass over the output and a second file, neither of which Hardwood's
        // destination pays. RawLocalFileSystem writes the file alone, so -Dperf.dir measures the
        // same work on both sides.
        hadoopConf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);

        reportProducedFiles();
    }

    @Benchmark
    public long hardwoodColumnar() throws IOException {
        return writeHardwood(COLUMNAR_FILE, this::writeColumnar);
    }

    @Benchmark
    public long hardwoodRow() throws IOException {
        return writeHardwood(ROW_FILE, this::writeRows);
    }

    /// The row layer reached by field index rather than by name.
    ///
    /// The named setters resolve a name per field per record, where the columnar contender
    /// resolves one per field per batch — a thousand records apart on this fixture. Everything
    /// else is what [#hardwoodRow] does, so the difference between them is what a caller pays for
    /// naming its fields in a hot loop.
    @Benchmark
    public long hardwoodRowByIndex() throws IOException {
        return writeHardwood(ROW_INDEXED_FILE, this::writeRowsByIndex);
    }

    /// The row layer given what the columnar contender is given: field indices, `BYTE_ARRAY`
    /// values already encoded, and the timestamp already in the units the column stores.
    ///
    /// [#hardwoodRow] hands the writer `String`s and `Instant`s, and the columnar contender is
    /// handed the same values pre-encoded from the same fixture — so the two are not writing the
    /// same work, whatever they are writing to. This one is, and what remains between it and
    /// [#hardwoodColumnar] is the row layer's own staging.
    @Benchmark
    public long hardwoodRowByIndexRaw() throws IOException {
        return writeHardwood(ROW_RAW_FILE, this::writeRowsRaw);
    }

    @Benchmark
    public long parquetJavaGroup() throws IOException {
        if (dir == null) {
            MemoryOutputFile out = new MemoryOutputFile();
            writeGroups(ExampleParquetWriter.builder(out));
            return out.size();
        }
        Path path = dir.resolve(PARQUET_JAVA_FILE);
        Files.deleteIfExists(path);
        writeGroups(ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toUri())));
        return Files.size(path);
    }

    /// Writes the fixture through one of the Hardwood APIs, returning the size of the file
    /// produced.
    private long writeHardwood(String fileName, HardwoodWrite write) throws IOException {
        if (dir == null) {
            ByteBufferOutputFile out = new ByteBufferOutputFile();
            write.writeTo(out);
            return out.position();
        }
        Path path = dir.resolve(fileName);
        write.writeTo(OutputFile.of(path));
        return Files.size(path);
    }

    /// Hands each batch's column arrays to the writer as they are.
    private void writeRowsByIndex(OutputFile out) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, hardwoodSchema, writerConfig)) {
            RowWriter rows = writer.rowWriter();
            for (int b = 0; b < fixture.batchCount(); b++) {
                long[] ids = fixture.id[b];
                Instant[] pickups = fixture.pickup[b];
                int[] passengers = fixture.passengerCount[b];
                boolean[] passengersNull = fixture.passengerCountNulls[b];
                double[] fares = fixture.fare[b];
                String[] payments = fixture.paymentType[b];
                String[] vendors = fixture.vendor[b];
                for (int r = 0; r < ids.length; r++) {
                    int row = r;
                    rows.writeRow(record -> {
                        record.setLong(ID, ids[row])
                                .setTimestamp(PICKUP_TS, pickups[row])
                                .setDouble(FARE, fares[row])
                                .setString(PAYMENT_TYPE, payments[row])
                                .setString(VENDOR, vendors[row]);
                        if (passengersNull[row]) {
                            record.setNull(PASSENGER_COUNT);
                        }
                        else {
                            record.setInt(PASSENGER_COUNT, passengers[row]);
                        }
                    });
                }
            }
        }
    }

    private void writeRowsRaw(OutputFile out) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, hardwoodSchema, writerConfig)) {
            RowWriter rows = writer.rowWriter();
            for (int b = 0; b < fixture.batchCount(); b++) {
                long[] ids = fixture.id[b];
                long[] pickups = fixture.pickupMicros[b];
                int[] passengers = fixture.passengerCount[b];
                boolean[] passengersNull = fixture.passengerCountNulls[b];
                double[] fares = fixture.fare[b];
                byte[][] payments = fixture.paymentTypeBytes[b];
                byte[][] vendors = fixture.vendorBytes[b];
                // The fixture parks a placeholder in the value array where a vendor is absent, as
                // the columnar contender's validity mask expects; the mask is what says absent.
                boolean[] vendorsNull = fixture.vendorNulls[b];
                for (int r = 0; r < ids.length; r++) {
                    int row = r;
                    rows.writeRow(record -> {
                        record.setLong(ID, ids[row])
                                .setLong(PICKUP_TS, pickups[row])
                                .setDouble(FARE, fares[row])
                                .setBinary(PAYMENT_TYPE, payments[row]);
                        if (vendorsNull[row]) {
                            record.setNull(VENDOR);
                        }
                        else {
                            record.setBinary(VENDOR, vendors[row]);
                        }
                        if (passengersNull[row]) {
                            record.setNull(PASSENGER_COUNT);
                        }
                        else {
                            record.setInt(PASSENGER_COUNT, passengers[row]);
                        }
                    });
                }
            }
        }
    }

    private void writeColumnar(OutputFile out) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, hardwoodSchema, writerConfig)) {
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

    /// Walks the same arrays record by record, writing each field by name.
    private void writeRows(OutputFile out) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, hardwoodSchema, writerConfig)) {
            RowWriter rows = writer.rowWriter();
            for (int b = 0; b < fixture.batchCount(); b++) {
                long[] ids = fixture.id[b];
                Instant[] pickups = fixture.pickup[b];
                int[] passengers = fixture.passengerCount[b];
                boolean[] passengersNull = fixture.passengerCountNulls[b];
                double[] fares = fixture.fare[b];
                String[] payments = fixture.paymentType[b];
                String[] vendors = fixture.vendor[b];
                for (int r = 0; r < ids.length; r++) {
                    int row = r;
                    rows.writeRow(record -> {
                        record.setLong("id", ids[row])
                                .setTimestamp("pickup_ts", pickups[row])
                                .setDouble("fare", fares[row])
                                .setString("payment_type", payments[row])
                                .setString("vendor", vendors[row]);
                        if (passengersNull[row]) {
                            record.setNull("passenger_count");
                        }
                        else {
                            record.setInt("passenger_count", passengers[row]);
                        }
                    });
                }
            }
        }
    }

    /// Walks the same arrays record by record, constructing the `SimpleGroup` parquet-java's
    /// record-shaped API takes.
    private void writeGroups(ExampleParquetWriter.Builder builder) throws IOException {
        try (ParquetWriter<Group> writer = builder
                .withConf(hadoopConf)
                .withType(parquetJavaSchema)
                .withCompressionCodec(parquetJavaCodec)
                .withPageSize(PAGE_TARGET_BYTES)
                // Hardwood bounds a page by size alone, so parquet-java's 20k-row page cap is
                // lifted: with it in place the two would not be cutting pages on the same rule.
                .withPageRowCountLimit(Integer.MAX_VALUE)
                .withRowGroupSize(ROW_GROUP_TARGET_BYTES)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(PARQUET_JAVA_DICTIONARY_PAGE_LIMIT_BYTES)
                .withWriterVersion(WriterVersion.PARQUET_1_0)
                .withPageWriteChecksumEnabled(true)
                .withValidation(false)
                .build()) {
            SimpleGroupFactory groups = new SimpleGroupFactory(parquetJavaSchema);
            for (int b = 0; b < fixture.batchCount(); b++) {
                long[] ids = fixture.id[b];
                long[] pickups = fixture.pickupMicros[b];
                int[] passengers = fixture.passengerCount[b];
                boolean[] passengersNull = fixture.passengerCountNulls[b];
                double[] fares = fixture.fare[b];
                String[] payments = fixture.paymentType[b];
                String[] vendors = fixture.vendor[b];
                boolean[] vendorsNull = fixture.vendorNulls[b];
                for (int r = 0; r < ids.length; r++) {
                    Group group = groups.newGroup()
                            .append("id", ids[r])
                            .append("pickup_ts", pickups[r])
                            .append("fare", fares[r])
                            .append("payment_type", payments[r]);
                    if (!passengersNull[r]) {
                        group.append("passenger_count", passengers[r]);
                    }
                    if (!vendorsNull[r]) {
                        group.append("vendor", vendors[r]);
                    }
                    writer.write(group);
                }
            }
        }
    }

    /// Writes the fixture once through each contender to memory, checks that each produced
    /// file holds the records the fixture has, and prints the three sizes so the times are
    /// never read without them.
    private void reportProducedFiles() throws IOException {
        ByteBufferOutputFile columnar = new ByteBufferOutputFile();
        writeColumnar(columnar);
        ByteBufferOutputFile row = new ByteBufferOutputFile();
        writeRows(row);
        ByteBufferOutputFile rowIndexed = new ByteBufferOutputFile();
        writeRowsByIndex(rowIndexed);
        ByteBufferOutputFile rowRaw = new ByteBufferOutputFile();
        writeRowsRaw(rowRaw);
        MemoryOutputFile groups = new MemoryOutputFile();
        writeGroups(ExampleParquetWriter.builder(groups));

        byte[] columnarFile = columnar.toByteArray();
        byte[] rowFile = row.toByteArray();
        // The two Hardwood APIs write the same bytes for the same records, which the writer's
        // equivalence tests hold them to. A divergence here is a writer defect that would
        // otherwise be read as one contender producing a leaner file than the other.
        if (columnarFile.length != rowFile.length) {
            throw new IllegalStateException("The two Hardwood APIs produced files of different size: "
                    + columnarFile.length + " bytes columnar against " + rowFile.length + " bytes row");
        }

        // The row variants differ from `writeRows` only in how the same values are handed over —
        // by index rather than by name, and already encoded rather than as `String` and `Instant`.
        // Byte equality is what says so: a variant that wrote a different file would be measuring
        // different work, and the comparison it exists for would be worthless.
        requireSameBytes("by-index", rowFile, rowIndexed.toByteArray());
        requireSameBytes("by-index-raw", rowFile, rowRaw.toByteArray());

        System.out.printf("%nFlatWriteBenchmark: %,d rows, codec %s, %,d-row batches%n",
                fixture.rows(), codec, BATCH_ROWS);
        report("HARDWOOD_COLUMNAR", columnarFile);
        report("HARDWOOD_ROW", rowFile);
        report("PARQUET_JAVA_GROUP", groups.toByteArray());
    }

    private static void requireSameBytes(String variant, byte[] expected, byte[] actual) {
        if (!java.util.Arrays.equals(expected, actual)) {
            throw new IllegalStateException("The " + variant + " row variant produced a different file: "
                    + actual.length + " bytes against " + expected.length
                    + ", so it is not writing the same records as hardwoodRow");
        }
    }

    private void report(String contender, byte[] file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            long rows = reader.getFileMetaData().numRows();
            if (rows != fixture.rows()) {
                throw new IllegalStateException(contender + " wrote " + rows + " rows, expected " + fixture.rows());
            }
            System.out.printf("  %-20s %,15d bytes, %,d rows, %d row groups%n",
                    contender, file.length, rows, reader.getFileMetaData().rowGroups().size());
        }
    }
}
