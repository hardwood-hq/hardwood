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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * Performance comparison test for deeply nested Parquet schemas.
 *
 * <p>
 * Uses an Overture Maps places file (downloaded by test-data-setup module) with
 * deeply nested structs, lists, and maps to compare reading performance.
 * </p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NestedPerformanceTest {

    private static final Path DATA_FILE = Path.of(
            "../test-data-setup/target/overture-maps-data/overture_places.zstd.parquet");
    private static final int DEFAULT_RUNS = 5;
    private static final String CONTENDERS_PROPERTY = "perf.contenders";
    private static final String RUNS_PROPERTY = "perf.runs";

    enum Contender {
        PARQUET_JAVA("parquet-java"),
        HARDWOOD_NAMED("Hardwood (named)"),
        HARDWOOD_INDEXED("Hardwood (indexed)"),
        HARDWOOD_COLUMNAR("Hardwood (columnar)");

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
            throw new IllegalArgumentException("Unknown contender: " + name
                    + ". Valid values: " + Arrays.toString(values()));
        }
    }

    record Result(
            long rowCount,
            long durationMs,
            int minVersion,
            int maxVersion,
            double minConfidence,
            double maxConfidence,
            double minBboxXmin,
            double maxBboxXmax,
            long totalWebsiteCount,
            int maxWebsiteCount,
            long totalSourceCount,
            int maxSourceCount,
            long totalNameEntries,
            int maxNameEntries,
            int maxPrimaryNameLength,
            long totalAddressCount,
            int maxAddressCount) {
    }

    private Set<Contender> getEnabledContenders() {
        String property = System.getProperty(CONTENDERS_PROPERTY);
        if (property == null || property.isBlank()) {
            return EnumSet.of(Contender.HARDWOOD_NAMED);
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

    private int getRunCount() {
        String property = System.getProperty(RUNS_PROPERTY);
        if (property == null || property.isBlank()) {
            return DEFAULT_RUNS;
        }
        return Integer.parseInt(property);
    }

    @Test
    void comparePerformance() throws IOException {
        assertThat(Files.exists(DATA_FILE))
                .as("Overture Maps data file should exist at %s. Run test-data-setup first.", DATA_FILE)
                .isTrue();

        Set<Contender> enabledContenders = getEnabledContenders();
        int runCount = getRunCount();

        long fileSize = Files.size(DATA_FILE);
        System.out.println("\n=== Nested Schema Performance Test ===");
        System.out.println("File: " + DATA_FILE.getFileName());
        System.out.println("File size: " + String.format("%,.1f MB", fileSize / (1024.0 * 1024.0)));
        System.out.println("Runs per contender: " + runCount);
        System.out.println("Enabled contenders: " + enabledContenders.stream()
                .map(Contender::displayName)
                .collect(Collectors.joining(", ")));

        // Warmup
        System.out.println("\nWarmup run...");
        Contender warmupContender = enabledContenders.iterator().next();
        getRunner(warmupContender).get();

        // Timed runs
        System.out.println("\nTimed runs:");
        java.util.Map<Contender, List<Result>> results = new java.util.EnumMap<>(Contender.class);

        for (Contender contender : enabledContenders) {
            List<Result> contenderResults = new ArrayList<>();
            for (int i = 0; i < runCount; i++) {
                Result result = timeRun(contender.displayName() + " [" + (i + 1) + "/" + runCount + "]",
                        getRunner(contender));
                contenderResults.add(result);
            }
            results.put(contender, contenderResults);
        }

        printResults(fileSize, runCount, results);
        verifyCorrectness(results);
    }

    @Test
    void printRows() throws IOException {
        assertThat(Files.exists(DATA_FILE))
                .as("Overture Maps data file should exist at %s. Run test-data-setup first.", DATA_FILE)
                .isTrue();

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.open(DATA_FILE)) {

            System.out.println("\n=== Schema ===");
            System.out.println(reader.getFileSchema());

            System.out.println("\n=== Sample Rows (first 5 with nested data) ===\n");

            try (RowReader rowReader = reader.createRowReader()) {
                int printed = 0;
                int scanned = 0;
                while (rowReader.hasNext() && printed < 5) {
                    rowReader.next();
                    scanned++;

                    // Skip rows where most nested fields are null
                    if (rowReader.isNull("names") && rowReader.isNull("sources")) {
                        continue;
                    }

                    printed++;
                    System.out.println("--- Row " + scanned + " ---");

                    if (!rowReader.isNull("id")) {
                        System.out.println("  id: " + rowReader.getString("id"));
                    }
                    if (!rowReader.isNull("version")) {
                        System.out.println("  version: " + rowReader.getInt("version"));
                    }
                    if (!rowReader.isNull("confidence")) {
                        System.out.println("  confidence: " + rowReader.getDouble("confidence"));
                    }

                    if (!rowReader.isNull("bbox")) {
                        PqStruct bbox = rowReader.getStruct("bbox");
                        System.out.println("  bbox:");
                        System.out.println("    xmin: " + bbox.getFloat("xmin"));
                        System.out.println("    xmax: " + bbox.getFloat("xmax"));
                        System.out.println("    ymin: " + bbox.getFloat("ymin"));
                        System.out.println("    ymax: " + bbox.getFloat("ymax"));
                    }

                    if (!rowReader.isNull("names")) {
                        PqStruct names = rowReader.getStruct("names");
                        if (!names.isNull("primary")) {
                            System.out.println("  names.primary: " + names.getString("primary"));
                        }
                        if (!names.isNull("common")) {
                            PqMap common = names.getMap("common");
                            System.out.println("  names.common: " + common.size() + " entries");
                            int count = 0;
                            for (PqMap.Entry entry : common.getEntries()) {
                                if (count >= 3) {
                                    System.out.println("    ... and " + (common.size() - 3) + " more");
                                    break;
                                }
                                System.out.println("    " + entry.getStringKey() + " -> " + entry.getStringValue());
                                count++;
                            }
                        }
                    }

                    if (!rowReader.isNull("sources")) {
                        PqList sources = rowReader.getList("sources");
                        System.out.println("  sources: " + sources.size() + " entries");
                        for (int s = 0; s < Math.min(sources.size(), 2); s++) {
                            PqStruct src = (PqStruct) sources.get(s);
                            if (src != null) {
                                System.out.println("    [" + s + "]:");
                                if (!src.isNull("dataset")) {
                                    System.out.println("      dataset: " + src.getString("dataset"));
                                }
                                if (!src.isNull("record_id")) {
                                    System.out.println("      record_id: " + src.getString("record_id"));
                                }
                            }
                        }
                    }

                    if (!rowReader.isNull("websites")) {
                        PqList websites = rowReader.getList("websites");
                        System.out.println("  websites: " + websites.size() + " entries");
                        for (String url : websites.strings()) {
                            System.out.println("    - " + url);
                        }
                    }

                    if (!rowReader.isNull("addresses")) {
                        PqList addresses = rowReader.getList("addresses");
                        System.out.println("  addresses: " + addresses.size() + " entries");
                        for (int a = 0; a < Math.min(addresses.size(), 2); a++) {
                            PqStruct addr = (PqStruct) addresses.get(a);
                            if (addr != null) {
                                System.out.println("    [" + a + "]:");
                                if (!addr.isNull("freeform")) {
                                    System.out.println("      freeform: " + addr.getString("freeform"));
                                }
                                if (!addr.isNull("locality")) {
                                    System.out.println("      locality: " + addr.getString("locality"));
                                }
                                if (!addr.isNull("country")) {
                                    System.out.println("      country: " + addr.getString("country"));
                                }
                            }
                        }
                    }

                    System.out.println();
                }
                System.out.println("(Scanned " + scanned + " rows to find " + printed + " with nested data)");
            }
        }
    }

    // ==================== Runners ====================

    private Supplier<Result> getRunner(Contender contender) {
        return switch (contender) {
            case PARQUET_JAVA -> this::runParquetJava;
            case HARDWOOD_NAMED -> this::runHardwoodNamed;
            case HARDWOOD_INDEXED -> this::runHardwoodIndexed;
            case HARDWOOD_COLUMNAR -> this::runHardwoodColumnar;
        };
    }

    private Result runParquetJava() {
        long rowCount = 0;
        int minVersion = Integer.MAX_VALUE;
        int maxVersion = Integer.MIN_VALUE;
        double minConfidence = Double.MAX_VALUE;
        double maxConfidence = -Double.MAX_VALUE;
        double minBboxXmin = Double.MAX_VALUE;
        double maxBboxXmax = -Double.MAX_VALUE;
        long totalWebsiteCount = 0;
        int maxWebsiteCount = 0;
        long totalSourceCount = 0;
        int maxSourceCount = 0;
        long totalNameEntries = 0;
        int maxNameEntries = 0;
        int maxPrimaryNameLength = 0;
        long totalAddressCount = 0;
        int maxAddressCount = 0;

        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(DATA_FILE.toUri());

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(hadoopPath, conf))
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                rowCount++;

                // version (int)
                Object v = record.get("version");
                if (v != null) {
                    int version = ((Number) v).intValue();
                    if (version < minVersion) minVersion = version;
                    if (version > maxVersion) maxVersion = version;
                }

                // confidence (double)
                Object c = record.get("confidence");
                if (c != null) {
                    double confidence = ((Number) c).doubleValue();
                    if (confidence < minConfidence) minConfidence = confidence;
                    if (confidence > maxConfidence) maxConfidence = confidence;
                }

                // bbox (struct)
                Object bboxObj = record.get("bbox");
                if (bboxObj instanceof GenericRecord bbox) {
                    Object xminObj = bbox.get("xmin");
                    Object xmaxObj = bbox.get("xmax");
                    if (xminObj != null) {
                        double xmin = ((Number) xminObj).doubleValue();
                        if (xmin < minBboxXmin) minBboxXmin = xmin;
                    }
                    if (xmaxObj != null) {
                        double xmax = ((Number) xmaxObj).doubleValue();
                        if (xmax > maxBboxXmax) maxBboxXmax = xmax;
                    }
                }

                // websites (list<string>)
                Object websitesObj = record.get("websites");
                if (websitesObj instanceof java.util.List<?> websites) {
                    int size = websites.size();
                    totalWebsiteCount += size;
                    if (size > maxWebsiteCount) maxWebsiteCount = size;
                }

                // sources (list<struct>)
                Object sourcesObj = record.get("sources");
                if (sourcesObj instanceof java.util.List<?> sources) {
                    int size = sources.size();
                    totalSourceCount += size;
                    if (size > maxSourceCount) maxSourceCount = size;
                }

                // names (struct -> map, string)
                Object namesObj = record.get("names");
                if (namesObj instanceof GenericRecord names) {
                    Object commonObj = names.get("common");
                    if (commonObj instanceof java.util.Map<?, ?> commonMap) {
                        int size = commonMap.size();
                        totalNameEntries += size;
                        if (size > maxNameEntries) maxNameEntries = size;
                    }

                    Object primaryObj = names.get("primary");
                    if (primaryObj != null) {
                        int len = primaryObj.toString().length();
                        if (len > maxPrimaryNameLength) maxPrimaryNameLength = len;
                    }
                }

                // addresses (list<struct>)
                Object addressesObj = record.get("addresses");
                if (addressesObj instanceof java.util.List<?> addresses) {
                    int size = addresses.size();
                    totalAddressCount += size;
                    if (size > maxAddressCount) maxAddressCount = size;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + DATA_FILE, e);
        }

        return new Result(rowCount, 0, minVersion, maxVersion, minConfidence, maxConfidence,
                minBboxXmin, maxBboxXmax, totalWebsiteCount, maxWebsiteCount,
                totalSourceCount, maxSourceCount, totalNameEntries, maxNameEntries,
                maxPrimaryNameLength, totalAddressCount, maxAddressCount);
    }

    private Result runHardwoodNamed() {
        long rowCount = 0;
        int minVersion = Integer.MAX_VALUE;
        int maxVersion = Integer.MIN_VALUE;
        double minConfidence = Double.MAX_VALUE;
        double maxConfidence = -Double.MAX_VALUE;
        double minBboxXmin = Double.MAX_VALUE;
        double maxBboxXmax = -Double.MAX_VALUE;
        long totalWebsiteCount = 0;
        int maxWebsiteCount = 0;
        long totalSourceCount = 0;
        int maxSourceCount = 0;
        long totalNameEntries = 0;
        int maxNameEntries = 0;
        int maxPrimaryNameLength = 0;
        long totalAddressCount = 0;
        int maxAddressCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.open(DATA_FILE);
             RowReader rowReader = reader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                rowCount++;

                // version (int)
                if (!rowReader.isNull("version")) {
                    int version = rowReader.getInt("version");
                    if (version < minVersion) minVersion = version;
                    if (version > maxVersion) maxVersion = version;
                }

                // confidence (double)
                if (!rowReader.isNull("confidence")) {
                    double confidence = rowReader.getDouble("confidence");
                    if (confidence < minConfidence) minConfidence = confidence;
                    if (confidence > maxConfidence) maxConfidence = confidence;
                }

                // bbox (struct -> float)
                if (!rowReader.isNull("bbox")) {
                    PqStruct bbox = rowReader.getStruct("bbox");
                    if (!bbox.isNull("xmin")) {
                        double xmin = bbox.getFloat("xmin");
                        if (xmin < minBboxXmin) minBboxXmin = xmin;
                    }
                    if (!bbox.isNull("xmax")) {
                        double xmax = bbox.getFloat("xmax");
                        if (xmax > maxBboxXmax) maxBboxXmax = xmax;
                    }
                }

                // websites (list<string>)
                if (!rowReader.isNull("websites")) {
                    PqList websites = rowReader.getList("websites");
                    int size = websites.size();
                    totalWebsiteCount += size;
                    if (size > maxWebsiteCount) maxWebsiteCount = size;
                }

                // sources (list<struct>)
                if (!rowReader.isNull("sources")) {
                    PqList sources = rowReader.getList("sources");
                    int size = sources.size();
                    totalSourceCount += size;
                    if (size > maxSourceCount) maxSourceCount = size;
                }

                // names (struct -> map, string)
                if (!rowReader.isNull("names")) {
                    PqStruct names = rowReader.getStruct("names");
                    if (!names.isNull("common")) {
                        PqMap common = names.getMap("common");
                        int size = common.size();
                        totalNameEntries += size;
                        if (size > maxNameEntries) maxNameEntries = size;
                    }
                    if (!names.isNull("primary")) {
                        int len = names.getString("primary").length();
                        if (len > maxPrimaryNameLength) maxPrimaryNameLength = len;
                    }
                }

                // addresses (list<struct>)
                if (!rowReader.isNull("addresses")) {
                    PqList addresses = rowReader.getList("addresses");
                    int size = addresses.size();
                    totalAddressCount += size;
                    if (size > maxAddressCount) maxAddressCount = size;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + DATA_FILE, e);
        }

        return new Result(rowCount, 0, minVersion, maxVersion, minConfidence, maxConfidence,
                minBboxXmin, maxBboxXmax, totalWebsiteCount, maxWebsiteCount,
                totalSourceCount, maxSourceCount, totalNameEntries, maxNameEntries,
                maxPrimaryNameLength, totalAddressCount, maxAddressCount);
    }

    private Result runHardwoodIndexed() {
        long rowCount = 0;
        int minVersion = Integer.MAX_VALUE;
        int maxVersion = Integer.MIN_VALUE;
        double minConfidence = Double.MAX_VALUE;
        double maxConfidence = -Double.MAX_VALUE;
        double minBboxXmin = Double.MAX_VALUE;
        double maxBboxXmax = -Double.MAX_VALUE;
        long totalWebsiteCount = 0;
        int maxWebsiteCount = 0;
        long totalSourceCount = 0;
        int maxSourceCount = 0;
        long totalNameEntries = 0;
        int maxNameEntries = 0;
        int maxPrimaryNameLength = 0;
        long totalAddressCount = 0;
        int maxAddressCount = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.open(DATA_FILE);
             RowReader rowReader = reader.createRowReader()) {

            // Resolve top-level field indices once (RowReader supports int-based access)
            SchemaNode.GroupNode root = reader.getFileSchema().getRootNode();
            int versionIdx = findFieldIndex(root, "version");
            int confidenceIdx = findFieldIndex(root, "confidence");
            int bboxIdx = findFieldIndex(root, "bbox");
            int websitesIdx = findFieldIndex(root, "websites");
            int sourcesIdx = findFieldIndex(root, "sources");
            int namesIdx = findFieldIndex(root, "names");
            int addressesIdx = findFieldIndex(root, "addresses");

            while (rowReader.hasNext()) {
                rowReader.next();
                rowCount++;

                if (!rowReader.isNull(versionIdx)) {
                    int version = rowReader.getInt(versionIdx);
                    if (version < minVersion) minVersion = version;
                    if (version > maxVersion) maxVersion = version;
                }

                if (!rowReader.isNull(confidenceIdx)) {
                    double confidence = rowReader.getDouble(confidenceIdx);
                    if (confidence < minConfidence) minConfidence = confidence;
                    if (confidence > maxConfidence) maxConfidence = confidence;
                }

                // PqStruct uses name-based access only
                if (!rowReader.isNull(bboxIdx)) {
                    PqStruct bbox = rowReader.getStruct(bboxIdx);
                    if (!bbox.isNull("xmin")) {
                        double xmin = bbox.getFloat("xmin");
                        if (xmin < minBboxXmin) minBboxXmin = xmin;
                    }
                    if (!bbox.isNull("xmax")) {
                        double xmax = bbox.getFloat("xmax");
                        if (xmax > maxBboxXmax) maxBboxXmax = xmax;
                    }
                }

                if (!rowReader.isNull(websitesIdx)) {
                    PqList websites = rowReader.getList(websitesIdx);
                    int size = websites.size();
                    totalWebsiteCount += size;
                    if (size > maxWebsiteCount) maxWebsiteCount = size;
                }

                if (!rowReader.isNull(sourcesIdx)) {
                    PqList sources = rowReader.getList(sourcesIdx);
                    int size = sources.size();
                    totalSourceCount += size;
                    if (size > maxSourceCount) maxSourceCount = size;
                }

                if (!rowReader.isNull(namesIdx)) {
                    PqStruct names = rowReader.getStruct(namesIdx);
                    if (!names.isNull("common")) {
                        PqMap common = names.getMap("common");
                        int size = common.size();
                        totalNameEntries += size;
                        if (size > maxNameEntries) maxNameEntries = size;
                    }
                    if (!names.isNull("primary")) {
                        int len = names.getString("primary").length();
                        if (len > maxPrimaryNameLength) maxPrimaryNameLength = len;
                    }
                }

                if (!rowReader.isNull(addressesIdx)) {
                    PqList addresses = rowReader.getList(addressesIdx);
                    int size = addresses.size();
                    totalAddressCount += size;
                    if (size > maxAddressCount) maxAddressCount = size;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + DATA_FILE, e);
        }

        return new Result(rowCount, 0, minVersion, maxVersion, minConfidence, maxConfidence,
                minBboxXmin, maxBboxXmax, totalWebsiteCount, maxWebsiteCount,
                totalSourceCount, maxSourceCount, totalNameEntries, maxNameEntries,
                maxPrimaryNameLength, totalAddressCount, maxAddressCount);
    }

    private Result runHardwoodColumnar() {
        long rowCount = 0;
        int minVersion = Integer.MAX_VALUE;
        int maxVersion = Integer.MIN_VALUE;
        double minConfidence = Double.MAX_VALUE;
        double maxConfidence = -Double.MAX_VALUE;
        double minBboxXmin = Double.MAX_VALUE;
        double maxBboxXmax = -Double.MAX_VALUE;
        long totalWebsiteCount = 0;
        int maxWebsiteCount = 0;
        long totalSourceCount = 0;
        int maxSourceCount = 0;
        long totalAddressCount = 0;
        int maxAddressCount = 0;
        long totalNameEntries = 0;
        int maxNameEntries = 0;
        int maxPrimaryNameLength = 0;

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.open(DATA_FILE)) {

            // Resolve column indices from schema
            int versionColIdx = reader.getFileSchema().getColumn("version").columnIndex();
            int confidenceColIdx = reader.getFileSchema().getColumn("confidence").columnIndex();

            // Find bbox leaf columns by walking the schema tree
            SchemaNode.GroupNode root = reader.getFileSchema().getRootNode();
            SchemaNode.GroupNode bboxNode = (SchemaNode.GroupNode) findChild(root, "bbox");
            int bboxXminColIdx = ((SchemaNode.PrimitiveNode) findChild(bboxNode, "xmin")).columnIndex();
            int bboxXmaxColIdx = ((SchemaNode.PrimitiveNode) findChild(bboxNode, "xmax")).columnIndex();

            // For list sizes, use a leaf column under the list
            int websiteLeafColIdx = findFirstLeafColumnIndex(findChild(root, "websites"));
            int sourceLeafColIdx = findFirstLeafColumnIndex(findChild(root, "sources"));
            int addressLeafColIdx = findFirstLeafColumnIndex(findChild(root, "addresses"));

            // Names struct: common (map) and primary (string)
            SchemaNode.GroupNode namesNode = (SchemaNode.GroupNode) findChild(root, "names");
            int namesCommonKeyColIdx = findFirstLeafColumnIndex(findChild(namesNode, "common"));
            int namesPrimaryColIdx = ((SchemaNode.PrimitiveNode) findChild(namesNode, "primary")).columnIndex();

            // Read flat primitives: version and confidence
            try (ColumnReader versionCol = reader.createColumnReader(versionColIdx);
                 ColumnReader confidenceCol = reader.createColumnReader(confidenceColIdx)) {
                while (versionCol.nextBatch() & confidenceCol.nextBatch()) {
                    int count = versionCol.getRecordCount();
                    int[] versions = versionCol.getInts();
                    double[] confidences = confidenceCol.getDoubles();
                    BitSet vNulls = versionCol.getElementNulls();
                    BitSet cNulls = confidenceCol.getElementNulls();

                    for (int i = 0; i < count; i++) {
                        if (vNulls == null || !vNulls.get(i)) {
                            if (versions[i] < minVersion) minVersion = versions[i];
                            if (versions[i] > maxVersion) maxVersion = versions[i];
                        }
                        if (cNulls == null || !cNulls.get(i)) {
                            if (confidences[i] < minConfidence) minConfidence = confidences[i];
                            if (confidences[i] > maxConfidence) maxConfidence = confidences[i];
                        }
                    }
                    rowCount += count;
                }
            }

            // Read bbox leaf columns
            try (ColumnReader xminCol = reader.createColumnReader(bboxXminColIdx);
                 ColumnReader xmaxCol = reader.createColumnReader(bboxXmaxColIdx)) {
                while (xminCol.nextBatch() & xmaxCol.nextBatch()) {
                    int count = xminCol.getRecordCount();
                    float[] xmins = xminCol.getFloats();
                    float[] xmaxs = xmaxCol.getFloats();
                    BitSet xminNulls = xminCol.getElementNulls();
                    BitSet xmaxNulls = xmaxCol.getElementNulls();

                    for (int i = 0; i < count; i++) {
                        if (xminNulls == null || !xminNulls.get(i)) {
                            if (xmins[i] < minBboxXmin) minBboxXmin = xmins[i];
                        }
                        if (xmaxNulls == null || !xmaxNulls.get(i)) {
                            if (xmaxs[i] > maxBboxXmax) maxBboxXmax = xmaxs[i];
                        }
                    }
                }
            }

            // Compute list sizes from nested leaf columns
            totalWebsiteCount = computeListSizes(reader, websiteLeafColIdx, new long[2]);
            long[] websiteSizes = new long[2];
            totalWebsiteCount = computeListSizes(reader, websiteLeafColIdx, websiteSizes);
            maxWebsiteCount = (int) websiteSizes[1];

            long[] sourceSizes = new long[2];
            totalSourceCount = computeListSizes(reader, sourceLeafColIdx, sourceSizes);
            maxSourceCount = (int) sourceSizes[1];

            long[] addressSizes = new long[2];
            totalAddressCount = computeListSizes(reader, addressLeafColIdx, addressSizes);
            maxAddressCount = (int) addressSizes[1];

            // names.common map: count entries using key column offsets
            long[] commonSizes = new long[2];
            totalNameEntries = computeListSizes(reader, namesCommonKeyColIdx, commonSizes);
            maxNameEntries = (int) commonSizes[1];

            // names.primary: find max string length
            try (ColumnReader primaryCol = reader.createColumnReader(namesPrimaryColIdx)) {
                while (primaryCol.nextBatch()) {
                    int count = primaryCol.getRecordCount();
                    byte[][] values = primaryCol.getBinaries();
                    BitSet nulls = primaryCol.getElementNulls();
                    for (int i = 0; i < count; i++) {
                        if (nulls == null || !nulls.get(i)) {
                            int len = new String(values[i], java.nio.charset.StandardCharsets.UTF_8).length();
                            if (len > maxPrimaryNameLength) maxPrimaryNameLength = len;
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + DATA_FILE, e);
        }

        return new Result(rowCount, 0, minVersion, maxVersion, minConfidence, maxConfidence,
                minBboxXmin, maxBboxXmax, totalWebsiteCount, maxWebsiteCount,
                totalSourceCount, maxSourceCount, totalNameEntries, maxNameEntries,
                maxPrimaryNameLength, totalAddressCount, maxAddressCount);
    }

    /**
     * Compute list sizes from a nested leaf column using level-0 offsets.
     * Returns totalCount, and populates sizes[0]=totalCount, sizes[1]=maxCount.
     */
    private long computeListSizes(ParquetFileReader reader, int colIdx, long[] sizes) {
        long totalCount = 0;
        int maxCount = 0;

        try (ColumnReader col = reader.createColumnReader(colIdx)) {
            while (col.nextBatch()) {
                int recordCount = col.getRecordCount();
                int valueCount = col.getValueCount();
                int[] offsets = col.getOffsets(0);
                BitSet levelNulls = col.getLevelNulls(0);

                for (int r = 0; r < recordCount; r++) {
                    if (levelNulls != null && levelNulls.get(r)) continue;
                    int start = offsets[r];
                    int end = (r + 1 < recordCount) ? offsets[r + 1] : valueCount;
                    int size = end - start;
                    totalCount += size;
                    if (size > maxCount) maxCount = size;
                }
            }
        }

        sizes[0] = totalCount;
        sizes[1] = maxCount;
        return totalCount;
    }

    // ==================== Helpers ====================

    private static int findFieldIndex(SchemaNode.GroupNode group, String name) {
        for (int i = 0; i < group.children().size(); i++) {
            if (group.children().get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name + " in " + group.name());
    }

    private static SchemaNode findChild(SchemaNode.GroupNode group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalArgumentException("Child not found: " + name + " in " + group.name());
    }

    private static int findFirstLeafColumnIndex(SchemaNode node) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> prim.columnIndex();
            case SchemaNode.GroupNode group -> {
                SchemaNode first = group.isList() ? group.getListElement() : group.children().get(0);
                yield findFirstLeafColumnIndex(first);
            }
        };
    }

    // ==================== Timing and Reporting ====================

    private Result timeRun(String name, Supplier<Result> runner) {
        System.out.println("  Running " + name + "...");
        long start = System.currentTimeMillis();
        Result result = runner.get();
        long duration = System.currentTimeMillis() - start;
        return new Result(result.rowCount(), duration,
                result.minVersion(), result.maxVersion(),
                result.minConfidence(), result.maxConfidence(),
                result.minBboxXmin(), result.maxBboxXmax(),
                result.totalWebsiteCount(), result.maxWebsiteCount(),
                result.totalSourceCount(), result.maxSourceCount(),
                result.totalNameEntries(), result.maxNameEntries(),
                result.maxPrimaryNameLength(),
                result.totalAddressCount(), result.maxAddressCount());
    }

    private void printResults(long fileSize, int runCount,
                              java.util.Map<Contender, List<Result>> results) {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        Result firstResult = results.values().iterator().next().get(0);

        System.out.println("\n" + "=".repeat(100));
        System.out.println("NESTED SCHEMA PERFORMANCE TEST RESULTS");
        System.out.println("=".repeat(100));
        System.out.println();
        System.out.println("Environment:");
        System.out.println("  CPU cores:       " + cpuCores);
        System.out.println("  Java version:    " + System.getProperty("java.version"));
        System.out.println("  OS:              " + System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        System.out.println();
        System.out.println("Data:");
        System.out.println("  Total rows:      " + String.format("%,d", firstResult.rowCount()));
        System.out.println("  File size:       " + String.format("%,.1f MB", fileSize / (1024.0 * 1024.0)));
        System.out.println("  Runs per contender: " + runCount);
        System.out.println();

        // Correctness verification
        if (results.size() > 1) {
            System.out.println("Correctness Verification:");
            System.out.println(String.format("  %-25s %10s %10s %10s %12s %12s %10s",
                    "", "min_ver", "max_ver", "rows", "websites", "sources", "addresses"));
            for (var entry : results.entrySet()) {
                Result r = entry.getValue().get(0);
                System.out.println(String.format("  %-25s %,10d %,10d %,10d %,12d %,12d %,10d",
                        entry.getKey().displayName(),
                        r.minVersion(), r.maxVersion(), r.rowCount(),
                        r.totalWebsiteCount(), r.totalSourceCount(), r.totalAddressCount()));
            }
            System.out.println();
        }

        // Performance
        System.out.println("Performance (all runs):");
        System.out.println(String.format("  %-30s %12s %15s %18s %12s",
                "Contender", "Time (s)", "Records/sec", "Records/sec/core", "MB/sec"));
        System.out.println("  " + "-".repeat(95));

        for (var entry : results.entrySet()) {
            Contender c = entry.getKey();
            List<Result> contenderResults = entry.getValue();
            int cores = isHardwood(c) ? cpuCores : 1;

            for (int i = 0; i < contenderResults.size(); i++) {
                String label = c.displayName() + " [" + (i + 1) + "]";
                printResultRow(label, contenderResults.get(i), cores, fileSize);
            }

            double avgDurationMs = contenderResults.stream()
                    .mapToLong(Result::durationMs)
                    .average()
                    .orElse(0);
            Result avgResult = new Result(firstResult.rowCount(), (long) avgDurationMs,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            printResultRow(c.displayName() + " [AVG]", avgResult, cores, fileSize);

            long minDuration = contenderResults.stream().mapToLong(Result::durationMs).min().orElse(0);
            long maxDuration = contenderResults.stream().mapToLong(Result::durationMs).max().orElse(0);
            System.out.println(String.format("  %-30s   min: %.2fs, max: %.2fs, spread: %.2fs",
                    "", minDuration / 1000.0, maxDuration / 1000.0, (maxDuration - minDuration) / 1000.0));
            System.out.println();
        }

        System.out.println("=".repeat(100));
    }

    private boolean isHardwood(Contender c) {
        return c != Contender.PARQUET_JAVA;
    }

    private void printResultRow(String name, Result result, int cpuCores, long totalBytes) {
        double seconds = result.durationMs() / 1000.0;
        double recordsPerSec = result.rowCount() / seconds;
        double recordsPerSecPerCore = recordsPerSec / cpuCores;
        double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / seconds;

        System.out.println(String.format("  %-30s %12.2f %,15.0f %,18.0f %12.1f",
                name, seconds, recordsPerSec, recordsPerSecPerCore, mbPerSec));
    }

    private void verifyCorrectness(java.util.Map<Contender, List<Result>> results) {
        if (results.size() < 2) {
            return;
        }

        var first = results.entrySet().iterator().next();
        Result reference = first.getValue().get(0);
        String referenceName = first.getKey().displayName();

        for (var entry : results.entrySet()) {
            if (entry.getKey() == first.getKey()) continue;
            Result other = entry.getValue().get(0);
            String otherName = entry.getKey().displayName();

            assertThat(other.rowCount())
                    .as("%s rowCount should match %s", otherName, referenceName)
                    .isEqualTo(reference.rowCount());
            assertThat(other.minVersion())
                    .as("%s minVersion should match %s", otherName, referenceName)
                    .isEqualTo(reference.minVersion());
            assertThat(other.maxVersion())
                    .as("%s maxVersion should match %s", otherName, referenceName)
                    .isEqualTo(reference.maxVersion());
            assertThat(other.minConfidence())
                    .as("%s minConfidence should match %s", otherName, referenceName)
                    .isCloseTo(reference.minConfidence(), withinPercentage(0.0001));
            assertThat(other.maxConfidence())
                    .as("%s maxConfidence should match %s", otherName, referenceName)
                    .isCloseTo(reference.maxConfidence(), withinPercentage(0.0001));
            assertThat(other.minBboxXmin())
                    .as("%s minBboxXmin should match %s", otherName, referenceName)
                    .isCloseTo(reference.minBboxXmin(), withinPercentage(0.0001));
            assertThat(other.maxBboxXmax())
                    .as("%s maxBboxXmax should match %s", otherName, referenceName)
                    .isCloseTo(reference.maxBboxXmax(), withinPercentage(0.0001));
            assertThat(other.totalWebsiteCount())
                    .as("%s totalWebsiteCount should match %s", otherName, referenceName)
                    .isEqualTo(reference.totalWebsiteCount());
            assertThat(other.totalSourceCount())
                    .as("%s totalSourceCount should match %s", otherName, referenceName)
                    .isEqualTo(reference.totalSourceCount());
            assertThat(other.totalAddressCount())
                    .as("%s totalAddressCount should match %s", otherName, referenceName)
                    .isEqualTo(reference.totalAddressCount());
            assertThat(other.totalNameEntries())
                    .as("%s totalNameEntries should match %s", otherName, referenceName)
                    .isEqualTo(reference.totalNameEntries());
            assertThat(other.maxPrimaryNameLength())
                    .as("%s maxPrimaryNameLength should match %s", otherName, referenceName)
                    .isEqualTo(reference.maxPrimaryNameLength());
        }

        System.out.println("\nAll results match!");
    }
}
