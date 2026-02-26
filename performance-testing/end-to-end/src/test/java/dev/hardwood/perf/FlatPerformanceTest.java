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
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.Hardwood;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * Performance comparison test between Hardwood, and parquet-java.
 *
 * <p>
 * Uses NYC Yellow Taxi Trip Records (downloaded by test-file-setup module) and compares
 * reading performance while verifying correctness by comparing calculated sums.
 * </p>
 */
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
        HARDWOOD_INDEXED("Hardwood (indexed)"),
        HARDWOOD_NAMED("Hardwood (named)"),
        HARDWOOD_PROJECTION("Hardwood (projection)"),
        HARDWOOD_MULTIFILE_INDEXED("Hardwood (multifile indexed)"),
        HARDWOOD_MULTIFILE_NAMED("Hardwood (multifile named)"),
        HARDWOOD_COLUMN_READER("Hardwood (column reader)"),
        HARDWOOD_COLUMN_READER_MULTIFILE("Hardwood (column reader multifile)"),
        PARQUET_JAVA_INDEXED("parquet-java (indexed)"),
        PARQUET_JAVA_NAMED("parquet-java (named)");

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
            // Support legacy names for backwards compatibility
            if (name.equalsIgnoreCase("hardwood")) {
                return HARDWOOD_INDEXED;
            }
            if (name.equalsIgnoreCase("parquet-java") || name.equalsIgnoreCase("parquet_java")) {
                return PARQUET_JAVA_NAMED;
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
            return EnumSet.of(Contender.HARDWOOD_MULTIFILE_INDEXED);
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

        // Warmup run (not timed) - use first enabled contender, limited to 3 years of data
        System.out.println("\nWarmup run...");
        Contender warmupContender = enabledContenders.iterator().next();
        int warmupFileLimit = Math.min(files.size(), 12); // 3 years max
        List<Path> warmupFiles = files.subList(0, warmupFileLimit);
        getRunner(warmupContender).apply(warmupFiles);

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
            case HARDWOOD_INDEXED -> this::runHardwoodIndexed;
            case HARDWOOD_NAMED -> this::runHardwoodNamed;
            case HARDWOOD_PROJECTION -> this::runHardwoodProjection;
            case HARDWOOD_MULTIFILE_INDEXED -> this::runHardwoodMultiFile;
            case HARDWOOD_MULTIFILE_NAMED -> this::runHardwoodMultiFileNamed;
            case HARDWOOD_COLUMN_READER -> this::runHardwoodColumnReader;
            case HARDWOOD_COLUMN_READER_MULTIFILE -> this::runHardwoodColumnReaderMultiFile;
            case PARQUET_JAVA_INDEXED -> this::runParquetJavaIndexed;
            case PARQUET_JAVA_NAMED -> this::runParquetJavaNamed;
        };
    }

    private Result timeRun(String name, Supplier<Result> runner) {
        System.out.println("  Running " + name + "...");
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        return new Result(result.passengerCount(), result.tripDistance(),
                result.fareAmount(), duration, result.rowCount());
    }

    private Result runHardwoodIndexed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader reader = hardwood.open(file);
                        RowReader rowReader = reader.createRowReader()) {

                    // Check column type once per file
                    SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");

                    boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                            && pn.type() == PhysicalType.INT64;

                    int passengerCountIndex = reader.getFileSchema().getColumn("passenger_count").columnIndex();
                    int tripDistanceIndex = reader.getFileSchema().getColumn("trip_distance").columnIndex();
                    int fareAmountIndex = reader.getFileSchema().getColumn("fare_amount").columnIndex();

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
                catch (IOException e) {
                    throw new RuntimeException("Failed to read file: " + file, e);
                }
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private Result runHardwoodNamed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader reader = hardwood.open(file);
                        RowReader rowReader = reader.createRowReader()) {

                    // Check column type once per file
                    SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");

                    boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                            && pn.type() == PhysicalType.INT64;

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
                catch (IOException e) {
                    throw new RuntimeException("Failed to read file: " + file, e);
                }
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private Result runHardwoodProjection(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        // Only read the 3 columns we need
        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");

        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader reader = hardwood.open(file);
                        RowReader rowReader = reader.createRowReader(projection)) {

                    // Check column type once per file
                    SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");

                    boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                            && pn.type() == PhysicalType.INT64;

                    // Use projected indices (0, 1, 2) instead of original indices
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

    /**
     * Run using MultiFileRowReader with cross-file prefetching.
     * <p>
     * Groups files by schema compatibility (passenger_count type varies across files)
     * and uses MultiFileRowReader for each group.
     * </p>
     */
    private Result runHardwoodMultiFile(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");
        // Projected indices: 0=passenger_count, 1=trip_distance, 2=fare_amount

        // Group files by schema to handle passenger_count type differences
        List<SchemaGroup> groups = groupFilesBySchema(files);

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groups) {
                try (MultiFileParquetReader parquet = hardwood.openAll(group.files());
                     MultiFileRowReader rowReader = parquet.createRowReader(projection)) {
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
            throw new RuntimeException("Failed to read files with MultiFileRowReader", e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /**
     * Run using MultiFileRowReader with named (string) field access.
     * <p>
     * Same as {@link #runHardwoodMultiFile} but uses field names instead of projected indices.
     * </p>
     */
    private Result runHardwoodMultiFileNamed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        ColumnProjection projection = ColumnProjection.columns(
                "passenger_count", "trip_distance", "fare_amount");

        List<SchemaGroup> groups = groupFilesBySchema(files);

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groups) {
                try (MultiFileParquetReader parquet = hardwood.openAll(group.files());
                     MultiFileRowReader rowReader = parquet.createRowReader(projection)) {
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
            throw new RuntimeException("Failed to read files with MultiFileRowReader", e);
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    /**
     * Run using the batch ColumnReader API for maximum throughput.
     * Reads columns independently using typed arrays with zero boxing.
     */
    private Result runHardwoodColumnReader(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create()) {
            for (Path file : files) {
                try (ParquetFileReader reader = hardwood.open(file)) {
                    // Check column type once per file
                    SchemaNode pcNode = reader.getFileSchema().getField("passenger_count");
                    boolean pcIsLong = pcNode instanceof SchemaNode.PrimitiveNode pn
                            && pn.type() == PhysicalType.INT64;

                    try (ColumnReader col0 = reader.createColumnReader("passenger_count");
                         ColumnReader col1 = reader.createColumnReader("trip_distance");
                         ColumnReader col2 = reader.createColumnReader("fare_amount")) {

                        if (pcIsLong) {
                            while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
                                int count = col0.getRecordCount();
                                long[] v0 = col0.getLongs();
                                double[] v1 = col1.getDoubles();
                                double[] v2 = col2.getDoubles();
                                BitSet n0 = col0.getElementNulls();
                                BitSet n1 = col1.getElementNulls();
                                BitSet n2 = col2.getElementNulls();

                                for (int i = 0; i < count; i++) {
                                    if (n0 == null || !n0.get(i)) passengerCount += v0[i];
                                    if (n1 == null || !n1.get(i)) tripDistance += v1[i];
                                    if (n2 == null || !n2.get(i)) fareAmount += v2[i];
                                }
                                rowCount += count;
                            }
                        } else {
                            while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
                                int count = col0.getRecordCount();
                                double[] v0 = col0.getDoubles();
                                double[] v1 = col1.getDoubles();
                                double[] v2 = col2.getDoubles();
                                BitSet n0 = col0.getElementNulls();
                                BitSet n1 = col1.getElementNulls();
                                BitSet n2 = col2.getElementNulls();

                                for (int i = 0; i < count; i++) {
                                    if (n0 == null || !n0.get(i)) passengerCount += (long) v0[i];
                                    if (n1 == null || !n1.get(i)) tripDistance += v1[i];
                                    if (n2 == null || !n2.get(i)) fareAmount += v2[i];
                                }
                                rowCount += count;
                            }
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

    /**
     * Column-oriented reader with cross-file prefetching via shared FileManager.
     * Uses MultiFileColumnReaders to share a single FileManager across columns.
     */
    private Result runHardwoodColumnReaderMultiFile(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;

        try (Hardwood hardwood = Hardwood.create()) {
            for (SchemaGroup group : groupFilesBySchema(files)) {
                try (MultiFileParquetReader parquet = hardwood.openAll(group.files());
                     MultiFileColumnReaders columns = parquet.createColumnReaders(
                        ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))) {

                    ColumnReader col0 = columns.getColumnReader("passenger_count");
                    ColumnReader col1 = columns.getColumnReader("trip_distance");
                    ColumnReader col2 = columns.getColumnReader("fare_amount");

                    if (group.passengerCountIsLong()) {
                        while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
                            int count = col0.getRecordCount();
                            long[] v0 = col0.getLongs();
                            double[] v1 = col1.getDoubles();
                            double[] v2 = col2.getDoubles();
                            BitSet n0 = col0.getElementNulls();
                            BitSet n1 = col1.getElementNulls();
                            BitSet n2 = col2.getElementNulls();

                            for (int i = 0; i < count; i++) {
                                if (n0 == null || !n0.get(i)) passengerCount += v0[i];
                                if (n1 == null || !n1.get(i)) tripDistance += v1[i];
                                if (n2 == null || !n2.get(i)) fareAmount += v2[i];
                            }
                            rowCount += count;
                        }
                    }
                    else {
                        while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
                            int count = col0.getRecordCount();
                            double[] v0 = col0.getDoubles();
                            double[] v1 = col1.getDoubles();
                            double[] v2 = col2.getDoubles();
                            BitSet n0 = col0.getElementNulls();
                            BitSet n1 = col1.getElementNulls();
                            BitSet n2 = col2.getElementNulls();

                            for (int i = 0; i < count; i++) {
                                if (n0 == null || !n0.get(i)) passengerCount += (long) v0[i];
                                if (n1 == null || !n1.get(i)) tripDistance += v1[i];
                                if (n2 == null || !n2.get(i)) fareAmount += v2[i];
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

    /**
     * Groups files by schema compatibility (based on passenger_count physical type).
     * Files with compatible schemas are grouped together for cross-file prefetching.
     * Returns SchemaGroup records that include both the files and the type information,
     * avoiding the need to re-probe files later.
     */
    private List<SchemaGroup> groupFilesBySchema(List<Path> files) {
        List<Path> longTypeFiles = new ArrayList<>();
        List<Path> doubleTypeFiles = new ArrayList<>();

        for (Path file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(file)) {
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

    private Result runParquetJavaIndexed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;
        Configuration conf = new Configuration();

        for (Path file : files) {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord> builder(HadoopInputFile.fromPath(hadoopPath, conf))
                    .build()) {
                GenericRecord record;

                // Resolve field indices once per file using the schema
                int pcIndex = -1;
                int tdIndex = -1;
                int faIndex = -1;

                record = reader.read();
                if (record != null) {
                    org.apache.avro.Schema schema = record.getSchema();
                    pcIndex = schema.getField("passenger_count").pos();
                    tdIndex = schema.getField("trip_distance").pos();
                    faIndex = schema.getField("fare_amount").pos();

                    // Process first record
                    rowCount++;
                    Long pc = (Long) record.get(pcIndex);
                    if (pc != null) {
                        passengerCount += pc;
                    }
                    Double td = (Double) record.get(tdIndex);
                    if (td != null) {
                        tripDistance += td;
                    }
                    Double fa = (Double) record.get(faIndex);
                    if (fa != null) {
                        fareAmount += fa;
                    }
                }

                while ((record = reader.read()) != null) {
                    rowCount++;
                    Long pc = (Long) record.get(pcIndex);
                    if (pc != null) {
                        passengerCount += pc;
                    }

                    Double td = (Double) record.get(tdIndex);
                    if (td != null) {
                        tripDistance += td;
                    }

                    Double fa = (Double) record.get(faIndex);
                    if (fa != null) {
                        fareAmount += fa;
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to read file: " + file, e);
            }
        }
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
    }

    private Result runParquetJavaNamed(List<Path> files) {
        long passengerCount = 0;
        double tripDistance = 0.0;
        double fareAmount = 0.0;
        long rowCount = 0;
        Configuration conf = new Configuration();

        for (Path file : files) {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord> builder(HadoopInputFile.fromPath(hadoopPath, conf))
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    rowCount++;
                    Long pc = (Long) record.get("passenger_count");
                    if (pc != null) {
                        passengerCount += pc;
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
        return new Result(passengerCount, tripDistance, fareAmount, 0, rowCount);
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
        return c == Contender.HARDWOOD_INDEXED || c == Contender.HARDWOOD_NAMED
                || c == Contender.HARDWOOD_PROJECTION || c == Contender.HARDWOOD_MULTIFILE_INDEXED
                || c == Contender.HARDWOOD_MULTIFILE_NAMED
                || c == Contender.HARDWOOD_COLUMN_READER || c == Contender.HARDWOOD_COLUMN_READER_MULTIFILE;
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
