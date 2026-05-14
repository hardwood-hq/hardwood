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
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.Validity;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/// Performance comparison test between Hardwood, and parquet-java.
///
/// Uses NYC Yellow Taxi Trip Records (downloaded by test-file-setup module) and compares
/// reading performance while verifying correctness by comparing calculated sums.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlatPerformanceTest {

    private static final Path DATA_DIR = Path.of("../test-data-setup/target/tlc-trip-record-data");
    private static final YearMonth DEFAULT_START = YearMonth.of(2016, 1);
    private static final YearMonth DEFAULT_END = YearMonth.of(2025, 11);
    private static final int DEFAULT_RUNS = 10;
    private static final String CONTENDERS_PROPERTY = "perf.contenders";
    private static final String START_PROPERTY = "perf.start";
    private static final String END_PROPERTY = "perf.end";
    private static final String RUNS_PROPERTY = "perf.runs";

    enum Contender {
        HARDWOOD_ROW_READER_NAMED("Hardwood (row reader named)"),
        HARDWOOD_ROW_READER_INDEXED("Hardwood (row reader indexed)"),
        HARDWOOD_ROW_READER_INDEXED_FILE_BY_FILE("Hardwood (row reader indexed file-by-file)"),
        HARDWOOD_ROW_READER_ALL_COLUMNS("Hardwood (row reader all columns)"),
        HARDWOOD_COLUMN_READER("Hardwood (column reader)"),
        PARQUET_JAVA_NAMED("parquet-java (named)"),
        PARQUET_JAVA_COLUMN("parquet-java (column)");

        private final String displayName;

        Contender(String displayName) {
            this.displayName = displayName;
        }

        String displayName() {
            return displayName;
        }

        static Contender fromString(String name) {
            for (Contender c : values()) {
                if (c.name().equalsIgnoreCase(name) || c.displayName.equalsIgnoreCase(name)) {
                    return c;
                }
            }
            throw new IllegalArgumentException("Unknown contender: " + name +
                    ". Valid values: " + Arrays.toString(values()));
        }
    }

    record Result(long passengerCount, double tripDistance, double fareAmount, long durationMs, long rowCount) {
    }

    record SchemaGroup(List<Path> files, boolean passengerCountIsLong) {
    }

    private Set<Contender> getEnabledContenders() {
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

    private YearMonth getStartMonth() {
        String property = System.getProperty(START_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_START;
        }
        return YearMonth.parse(property);
    }

    private YearMonth getEndMonth() {
        String property = System.getProperty(END_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_END;
        }
        YearMonth requested = YearMonth.parse(property);
        return requested.isAfter(DEFAULT_END) ? DEFAULT_END : requested;
    }

    private int getRunCount() {
        String property = System.getProperty(RUNS_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_RUNS;
        }
        return Integer.parseInt(property);
    }

    private List<Path> getAvailableFiles() throws IOException {
        List<Path> files = new ArrayList<>();
        YearMonth start = getStartMonth();
        YearMonth end = getEndMonth();
        for (YearMonth ym = start; !ym.isAfter(end); ym = ym.plusMonths(1)) {
            String filename = String.format("yellow_tripdata_%d-%02d.parquet", ym.getYear(), ym.getMonthValue());
            Path file = DATA_DIR.resolve(filename);
            if (Files.exists(file) && Files.size(file) > 0) {
                files.add(file);
            }
        }
        return files;
    }

    @Test
    void comparePerformance() throws IOException {
        List<Path> files = getAvailableFiles();
        assertThat(files).as("At least one data file should be available. Run test-file-setup first.").isNotEmpty();

        Set<Contender> enabledContenders = getEnabledContenders();
        assertThat(enabledContenders).as("At least one contender must be enabled").isNotEmpty();

        int runCount = getRunCount();

        System.out.println("\n=== Performance Test ===");
        System.out.println("Files available: " + files.size());
        System.out.println("Runs per contender: " + runCount);
        System.out.println("Enabled contenders: " + enabledContenders.stream()
                .map(Contender::displayName)
                .collect(Collectors.joining(", ")));

        // Warmup: 3 full runs per contender to let the JIT stabilize
        System.out.println("\nWarmup runs (3 full runs per contender):");
        for (Contender contender : enabledContenders) {
            for (int i = 0; i < 3; i++) {
                long warmStart = System.currentTimeMillis();
                getRunner(contender).apply(files);
                long warmDuration = System.currentTimeMillis() - warmStart;
                System.out.println(String.format("  Warmup %s [%d/3]: %.2f s",
                        contender.displayName(), i + 1, warmDuration / 1000.0));
            }
        }

        // Timed runs
        System.out.println("\nTimed runs:");
        java.util.Map<Contender, List<Result>> results = new java.util.EnumMap<>(Contender.class);

        for (Contender contender : enabledContenders) {
            List<Result> contenderResults = new ArrayList<>();
            for (int i = 0; i < runCount; i++) {
                Result result = timeRun(contender.displayName() + " [" + (i + 1) + "/" + runCount + "]",
                        () -> getRunner(contender).apply(files));
                contenderResults.add(result);
            }
            results.put(contender, contenderResults);
        }

        // Print results
        printResults(files.size(), runCount, enabledContenders, results);

        // Verify correctness - compare all results against each other
        verifyCorrectness(results);
    }

    private void verifyCorrectness(java.util.Map<Contender, List<Result>> results) {
        if (results.size() < 2) {
            return;
        }

        // Use first result from first contender as reference
        java.util.Map.Entry<Contender, List<Result>> first = results.entrySet().iterator().next();
        Result reference = first.getValue().get(0);
        String referenceName = first.getKey().displayName();

        for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
            if (entry.getKey() == first.getKey()) {
                continue;
            }
            Result other = entry.getValue().get(0);
            String otherName = entry.getKey().displayName();

            assertThat(other.passengerCount())
                    .as("%s passenger_count should match %s", otherName, referenceName)
                    .isEqualTo(reference.passengerCount());
            assertThat(other.tripDistance())
                    .as("%s trip_distance should match %s", otherName, referenceName)
                    .isCloseTo(reference.tripDistance(), withinPercentage(0.0001));
            assertThat(other.fareAmount())
                    .as("%s fare_amount should match %s", otherName, referenceName)
                    .isCloseTo(reference.fareAmount(), withinPercentage(0.0001));
        }

        System.out.println("\nAll results match!");
    }

    private Function<List<Path>, Result> getRunner(Contender contender) {
        return switch (contender) {
            case HARDWOOD_ROW_READER_NAMED -> this::runHardwoodRowReaderNamed;
            case HARDWOOD_ROW_READER_INDEXED -> this::runHardwoodRowReaderIndexed;
            case HARDWOOD_ROW_READER_INDEXED_FILE_BY_FILE -> this::runHardwoodRowReaderIndexedFileByFile;
            case HARDWOOD_ROW_READER_ALL_COLUMNS -> this::runHardwoodRowReaderAllColumns;
            case HARDWOOD_COLUMN_READER -> this::runHardwoodColumnReader;
            case PARQUET_JAVA_NAMED -> this::runParquetJavaNamed;
            case PARQUET_JAVA_COLUMN -> this::runParquetJavaColumn;
        };
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        System.out.println(String.format("  %s: %.2f s", name, duration / 1000.0));
        return new Result(result.passengerCount(), result.tripDistance(),
                result.fareAmount(), duration, result.rowCount());
    }

    /// Row-oriented reader opened via [ParquetFileReader#openAll] with
    /// projection and indexed field access.
    ///
    /// Groups files by schema compatibility (passenger_count type varies across files)
    /// and reads each group as a single multi-file read.
    private Result runHardwoodRowReaderIndexed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");
        // Projected indices: 0=passenger_count, 1=trip_distance, 2=fare_amount

        List<SchemaGroup> groups = groupFilesBySchema(files);

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groups) {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(group.files()));
                     RowReader rowReader = parquet.buildRowReader().projection(projection).build()) {
                    boolean pcIsLong = group.passengerCountIsLong();

                    while (rowReader.hasNext()) {
                        rowReader.next();
                        rowCount++;

                        if (!rowReader.isNull(0)) { // passenger_count
                            if (pcIsLong) {
                                passengerCount += rowReader.getLong(0);
                            }
                            else {
                                passengerCount += (long) rowReader.getDouble(0);
                            }
                        }

                        if (!rowReader.isNull(1)) { // trip_distance
                            tripDistance += rowReader.getDouble(1);
                        }

                        if (!rowReader.isNull(2)) { // fare_amount
                            fareAmount += rowReader.getDouble(2);
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read files", e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// Same as [#runHardwoodRowReaderIndexed] but uses field names instead of projected indices.
    private Result runHardwoodRowReaderNamed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");

        List<SchemaGroup> groups = groupFilesBySchema(files);

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groups) {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(group.files()));
                     RowReader rowReader = parquet.buildRowReader().projection(projection).build()) {
                    boolean pcIsLong = group.passengerCountIsLong();

                    while (rowReader.hasNext()) {
                        rowReader.next();
                        rowCount++;

                        if (!rowReader.isNull("passenger_count")) {
                            if (pcIsLong) {
                                passengerCount += rowReader.getLong("passenger_count");
                            }
                            else {
                                passengerCount += (long) rowReader.getDouble("passenger_count");
                            }
                        }

                        if (!rowReader.isNull("trip_distance")) {
                            tripDistance += rowReader.getDouble("trip_distance");
                        }

                        if (!rowReader.isNull("fare_amount")) {
                            fareAmount += rowReader.getDouble("fare_amount");
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read files", e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// Row-oriented reader opened one file at a time via [ParquetFileReader],
    /// with projection and indexed field access. Exists to quantify the
    /// cross-file prefetching benefit of [ParquetFileReader#openAll].
    private Result runHardwoodRowReaderIndexedFileByFile(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");

        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader reader = hardwood.open(InputFile.of(file));
                        RowReader rowReader = reader.buildRowReader().projection(projection).build()) {

                    SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");
                    boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                            && pn.type() == PhysicalType.INT64;

                    while (rowReader.hasNext()) {
                        rowReader.next();
                        rowCount++;
                        if (!rowReader.isNull(0)) { // passenger_count
                            if (pcIsLong) {
                                passengerCount += rowReader.getLong(0);
                            }
                            else {
                                passengerCount += (long) rowReader.getDouble(0);
                            }
                        }

                        if (!rowReader.isNull(1)) { // trip_distance
                            tripDistance += rowReader.getDouble(1);
                        }

                        if (!rowReader.isNull(2)) { // fare_amount
                            fareAmount += rowReader.getDouble(2);
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to read file: " + file, e);
                }
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// Row-oriented reader opened via [ParquetFileReader#openAll] that
    /// projects **all** columns, with indexed field access for the three sum
    /// columns. Indices are resolved once per schema group (all files in a
    /// group share the same fingerprint, so leaf-column positions are stable).
    ///
    /// Useful for stressing the per-column pipeline with a wide projection —
    /// e.g. measuring whether sequential vs parallel batch polling in
    /// [dev.hardwood.internal.reader.FlatRowReader#loadNextBatch] becomes a
    /// bottleneck as column count grows.
    ///
    /// Groups files by full schema fingerprint (every leaf column's type)
    /// rather than just `passenger_count`, since other columns (e.g.
    /// `airport_fee`) drift across files and would otherwise prevent a
    /// multi-file open.
    private Result runHardwoodRowReaderAllColumns(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        List<SchemaGroup> groups = groupFilesByFullSchema(files);

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groups) {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(group.files()));
                     RowReader rowReader = parquet.rowReader()) {
                    boolean pcIsLong = group.passengerCountIsLong();

                    int passengerCountIndex = parquet.getFileSchema().getColumn("passenger_count").columnIndex();
                    int tripDistanceIndex = parquet.getFileSchema().getColumn("trip_distance").columnIndex();
                    int fareAmountIndex = parquet.getFileSchema().getColumn("fare_amount").columnIndex();

                    while (rowReader.hasNext()) {
                        rowReader.next();
                        rowCount++;

                        if (!rowReader.isNull(passengerCountIndex)) {
                            if (pcIsLong) {
                                passengerCount += rowReader.getLong(passengerCountIndex);
                            }
                            else {
                                passengerCount += (long) rowReader.getDouble(passengerCountIndex);
                            }
                        }

                        if (!rowReader.isNull(tripDistanceIndex)) {
                            tripDistance += rowReader.getDouble(tripDistanceIndex);
                        }

                        if (!rowReader.isNull(fareAmountIndex)) {
                            fareAmount += rowReader.getDouble(fareAmountIndex);
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read files", e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// Like [#groupFilesBySchema] but keys on the full schema fingerprint
    /// (every leaf column's name + physical type), so that drift in any column
    /// — not just `passenger_count` — produces a separate group.
    private List<SchemaGroup> groupFilesByFullSchema(List<Path> files) {
        java.util.LinkedHashMap<String, List<Path>> bySchema = new java.util.LinkedHashMap<>();
        java.util.Map<String, Boolean> pcIsLongBySchema = new java.util.HashMap<>();

        for (Path file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
                StringBuilder fp = new StringBuilder();
                for (dev.hardwood.schema.ColumnSchema col : reader.getFileSchema().getColumns()) {
                    fp.append(col.name()).append(':').append(col.type()).append(';');
                }
                String fingerprint = fp.toString();

                SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");
                boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                        && pn.type() == PhysicalType.INT64;

                bySchema.computeIfAbsent(fingerprint, k -> new ArrayList<>()).add(file);
                pcIsLongBySchema.putIfAbsent(fingerprint, pcIsLong);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read schema from: " + file, e);
            }
        }

        List<SchemaGroup> groups = new ArrayList<>();
        for (java.util.Map.Entry<String, List<Path>> e : bySchema.entrySet()) {
            groups.add(new SchemaGroup(e.getValue(), pcIsLongBySchema.get(e.getKey())));
        }
        return groups;
    }

    /// Column-oriented reader with cross-file prefetching via shared FileManager.
    /// Uses ColumnReaders to share a single FileManager across columns.
    private Result runHardwoodColumnReader(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groupFilesBySchema(files)) {
                try (ParquetFileReader parquet = hardwood.openAll(InputFile.ofPaths(group.files()));
                     ColumnReaders columns = parquet.columnReaders(ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))) {

                    ColumnReader col0 = columns.getColumnReader("passenger_count");
                    ColumnReader col1 = columns.getColumnReader("trip_distance");
                    ColumnReader col2 = columns.getColumnReader("fare_amount");

                    if (group.passengerCountIsLong()) {
                        while (columns.nextBatch()) {
                            int count = columns.getRecordCount();
                            long[] v0 = col0.getLongs();
                            double[] v1 = col1.getDoubles();
                            double[] v2 = col2.getDoubles();
                            Validity v0Valid = col0.getLeafValidity();
                            Validity v1Valid = col1.getLeafValidity();
                            Validity v2Valid = col2.getLeafValidity();

                            boolean v0HasNulls = v0Valid.hasNulls();
                            boolean v1HasNulls = v1Valid.hasNulls();
                            boolean v2HasNulls = v2Valid.hasNulls();

                            for (int i = 0; i < count; i++) {
                                if (!v0HasNulls || v0Valid.isNotNull(i)) {
                                    passengerCount += v0[i];
                                }
                                if (!v1HasNulls || v1Valid.isNotNull(i)) {
                                    tripDistance += v1[i];
                                }
                                if (!v2HasNulls || v2Valid.isNotNull(i)) {
                                    fareAmount += v2[i];
                                }
                            }
                            rowCount += count;
                        }
                    }
                    else {
                        while (columns.nextBatch()) {
                            int count = columns.getRecordCount();
                            double[] v0 = col0.getDoubles();
                            double[] v1 = col1.getDoubles();
                            double[] v2 = col2.getDoubles();
                            Validity v0Valid = col0.getLeafValidity();
                            Validity v1Valid = col1.getLeafValidity();
                            Validity v2Valid = col2.getLeafValidity();

                            boolean v0HasNulls = v0Valid.hasNulls();
                            boolean v1HasNulls = v1Valid.hasNulls();
                            boolean v2HasNulls = v2Valid.hasNulls();

                            for (int i = 0; i < count; i++) {
                                if (!v0HasNulls || v0Valid.isNotNull(i)) {
                                    passengerCount += (long) v0[i];
                                }
                                if (!v1HasNulls || v1Valid.isNotNull(i)) {
                                    tripDistance += v1[i];
                                }
                                if (!v2HasNulls || v2Valid.isNotNull(i)) {
                                    fareAmount += v2[i];
                                }
                            }
                            rowCount += count;
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to read files in schema group", e);
                }
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// Groups files by schema compatibility (based on passenger_count physical type).
    /// Files with compatible schemas are grouped together for cross-file prefetching.
    /// Returns SchemaGroup records that include both the files and the type information,
    /// avoiding the need to re-probe files later.
    private List<SchemaGroup> groupFilesBySchema(List<Path> files) {
        List<Path> longTypeFiles = new ArrayList<>();
        List<Path> doubleTypeFiles = new ArrayList<>();

        for (Path file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
                SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");
                boolean isLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                        && pn.type() == PhysicalType.INT64;
                if (isLong) {
                    longTypeFiles.add(file);
                }
                else {
                    doubleTypeFiles.add(file);
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read schema from: " + file, e);
            }
        }

        List<SchemaGroup> groups = new ArrayList<>();
        if (!longTypeFiles.isEmpty()) {
            groups.add(new SchemaGroup(longTypeFiles, true));
        }
        if (!doubleTypeFiles.isEmpty()) {
            groups.add(new SchemaGroup(doubleTypeFiles, false));
        }
        return groups;
    }

    private Result runParquetJavaNamed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        // Group by schema so we can build a matching projection per group
        // (passenger_count is INT64 in some files and DOUBLE in others).
        for (SchemaGroup group : groupFilesBySchema(files)) {
            boolean pcIsLong = group.passengerCountIsLong();
            Configuration conf = new Configuration();
            AvroReadSupport.setRequestedProjection(conf, buildAvroProjection(pcIsLong));

            for (Path file : group.files()) {
                org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
                try (ParquetReader<GenericRecord> reader = AvroParquetReader
                        .<GenericRecord> builder(HadoopInputFile.fromPath(hadoopPath, conf))
                        .withConf(conf)
                        .build()) {
                    GenericRecord record;
                    while ((record = reader.read()) != null) {
                        rowCount++;
                        Object pc = record.get("passenger_count");
                        if (pc != null) {
                            if (pcIsLong) {
                                passengerCount += (Long) pc;
                            }
                            else {
                                passengerCount += ((Double) pc).longValue();
                            }
                        }

                        Double td = (Double) record.get("trip_distance");
                        if (td != null) {
                            tripDistance += td;
                        }

                        Double fa = (Double) record.get("fare_amount");
                        if (fa != null) {
                            fareAmount += fa;
                        }
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to read file: " + file, e);
                }
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private static Schema buildAvroProjection(boolean passengerCountIsLong) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("trip")
                .namespace("dev.hardwood.perf")
                .fields();
        if (passengerCountIsLong) {
            fields = fields.optionalLong("passenger_count");
        }
        else {
            fields = fields.optionalDouble("passenger_count");
        }
        return fields
                .optionalDouble("trip_distance")
                .optionalDouble("fare_amount")
                .endRecord();
    }

    /// Reads via parquet-java's low-level column API
    /// ([org.apache.parquet.column.ColumnReader]). Avoids Avro/`GenericRecord`
    /// materialization but is still value-at-a-time (not vectorized) and
    /// single-threaded.
    private Result runParquetJavaColumn(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;
        Configuration conf = new Configuration();

        for (Path file : files) {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            try (org.apache.parquet.hadoop.ParquetFileReader reader =
                    org.apache.parquet.hadoop.ParquetFileReader.open(
                            HadoopInputFile.fromPath(hadoopPath, conf))) {
                MessageType fileSchema = reader.getFileMetaData().getSchema();
                MessageType projection = new MessageType("trip",
                        fileSchema.getType("passenger_count"),
                        fileSchema.getType("trip_distance"),
                        fileSchema.getType("fare_amount"));
                reader.setRequestedSchema(projection);

                ColumnDescriptor pcCol = projection.getColumnDescription(new String[] { "passenger_count" });
                ColumnDescriptor tdCol = projection.getColumnDescription(new String[] { "trip_distance" });
                ColumnDescriptor faCol = projection.getColumnDescription(new String[] { "fare_amount" });

                boolean pcIsLong = pcCol.getPrimitiveType().getPrimitiveTypeName()
                        == PrimitiveType.PrimitiveTypeName.INT64;
                int pcMaxDef = pcCol.getMaxDefinitionLevel();
                int tdMaxDef = tdCol.getMaxDefinitionLevel();
                int faMaxDef = faCol.getMaxDefinitionLevel();
                String createdBy = reader.getFileMetaData().getCreatedBy();

                PageReadStore pageStore;
                while ((pageStore = reader.readNextRowGroup()) != null) {
                    long rowsInGroup = pageStore.getRowCount();
                    ColumnReadStoreImpl colStore = new ColumnReadStoreImpl(
                            pageStore, new NoOpGroupConverter(), projection, createdBy);

                    org.apache.parquet.column.ColumnReader pcReader = colStore.getColumnReader(pcCol);
                    org.apache.parquet.column.ColumnReader tdReader = colStore.getColumnReader(tdCol);
                    org.apache.parquet.column.ColumnReader faReader = colStore.getColumnReader(faCol);

                    if (pcIsLong) {
                        for (long i = 0; i < rowsInGroup; i++) {
                            if (pcReader.getCurrentDefinitionLevel() == pcMaxDef) {
                                passengerCount += pcReader.getLong();
                            }
                            pcReader.consume();
                            if (tdReader.getCurrentDefinitionLevel() == tdMaxDef) {
                                tripDistance += tdReader.getDouble();
                            }
                            tdReader.consume();
                            if (faReader.getCurrentDefinitionLevel() == faMaxDef) {
                                fareAmount += faReader.getDouble();
                            }
                            faReader.consume();
                        }
                    }
                    else {
                        for (long i = 0; i < rowsInGroup; i++) {
                            if (pcReader.getCurrentDefinitionLevel() == pcMaxDef) {
                                passengerCount += (long) pcReader.getDouble();
                            }
                            pcReader.consume();
                            if (tdReader.getCurrentDefinitionLevel() == tdMaxDef) {
                                tripDistance += tdReader.getDouble();
                            }
                            tdReader.consume();
                            if (faReader.getCurrentDefinitionLevel() == faMaxDef) {
                                fareAmount += faReader.getDouble();
                            }
                            faReader.consume();
                        }
                    }
                    rowCount += rowsInGroup;
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read file: " + file, e);
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /// No-op [GroupConverter] needed by [ColumnReadStoreImpl]. The column readers
    /// only invoke the converter when assembling records, which we never do.
    private static final class NoOpGroupConverter extends GroupConverter {
        @Override
        public Converter getConverter(int fieldIndex) {
            return new PrimitiveConverter() {
            };
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
        }
    }

    private void printResults(int fileCount, int runCount, Set<Contender> enabledContenders,
            java.util.Map<Contender, List<Result>> results) throws IOException {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        long totalBytes = 0;
        for (Path file : getAvailableFiles()) {
            totalBytes += Files.size(file);
        }

        // Use the first available result to get row count
        Result firstResult = results.values().iterator().next().get(0);

        System.out.println("\n" + "=".repeat(100));
        System.out.println("PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  CPU cores:       " + cpuCores);
        System.out.println("  Java version:    " + System.getProperty("java.version"));
        System.out.println("  OS:              " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        System.out.println();
        System.out.println("Data:");
        System.out.println("  Files processed: " + fileCount);
        System.out.println("  Total rows:      " + String.format("%,d", firstResult.rowCount()));
        System.out.println("  Total size:      " + String.format("%,.1f MB", totalBytes / (1024.0 * 1024.0)));
        System.out.println("  Runs per contender: " + runCount);
        System.out.println();

        // Correctness verification (only if multiple contenders ran)
        if (results.size() > 1) {
            System.out.println("Correctness Verification:");
            System.out.println(String.format("  %-25s %17s %17s %17s", "", "passenger_count", "trip_distance", "fare_amount"));
            for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
                Result r = entry.getValue().get(0);
                System.out.println(String.format("  %-25s %,17d %,17.2f %,17.2f",
                        entry.getKey().displayName(), r.passengerCount(), r.tripDistance(), r.fareAmount()));
            }
            System.out.println();
        }

        // Performance comparison - show all runs and averages
        System.out.println("Performance (all runs):");
        System.out.println(String.format("  %-30s %12s %15s %18s %12s",
                "Contender", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(95));

        for (java.util.Map.Entry<Contender, List<Result>> entry : results.entrySet()) {
            Contender c = entry.getKey();
            List<Result> contenderResults = entry.getValue();
            // Hardwood uses parallelism (all cores), parquet-java is single-threaded
            int cores = isHardwood(c) ? cpuCores : 1;

            // Print each individual run
            for (int i = 0; i < contenderResults.size(); i++) {
                String label = c.displayName() + " [" + (i + 1) + "]";
                printResultRow(label, contenderResults.get(i), cores, totalBytes);
            }

            // Calculate and print average
            double avgDurationMs = contenderResults.stream()
                    .mapToLong(Result::durationMs)
                    .average()
                    .orElse(0);
            long avgRowCount = contenderResults.get(0).rowCount(); // Same for all runs
            Result avgResult = new Result(0, 0, 0, (long) avgDurationMs, avgRowCount);
            printResultRow(c.displayName() + " [AVG]", avgResult, cores, totalBytes);

            // Print min/max times
            long minDuration = contenderResults.stream().mapToLong(Result::durationMs).min().orElse(0);
            long maxDuration = contenderResults.stream().mapToLong(Result::durationMs).max().orElse(0);
            System.out.println(String.format("  %-30s   min: %.2fs, max: %.2fs, spread: %.2fs",
                    "", minDuration / 1000.0, maxDuration / 1000.0, (maxDuration - minDuration) / 1000.0));
            System.out.println();
        }

        System.out.println("=".repeat(100));
    }

    private boolean isHardwood(Contender c) {
        return !c.name().startsWith("PARQUET_JAVA");
    }

    private void printResultRow(String name, Result result, int cpuCores, long totalBytes) {
        double seconds = result.durationMs() / 1000.0;
        double recordsPerSec = result.rowCount() / seconds;
        double recordsPerSecPerCore = recordsPerSec / cpuCores;
        double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / seconds;

        System.out.println(String.format("  %-30s %12.2f %,15.0f %,18.0f %12.1f",
                name,
                seconds,
                recordsPerSec,
                recordsPerSecPerCore,
                mbPerSec));
    }
}
