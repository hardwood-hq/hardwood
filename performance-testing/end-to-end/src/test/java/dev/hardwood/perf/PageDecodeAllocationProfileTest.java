/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.perf;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.sun.management.ThreadMXBean;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.InPlacePageReads;
import dev.hardwood.internal.reader.MappedInputFile;
import dev.hardwood.internal.reader.PageDecoder;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageIterator;
import dev.hardwood.internal.reader.SequentialFetchPlan;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Allocation profiling test for the page read: [PageDecoder]'s in-place entry points and the
/// values written as a fill writes them, into arrays reused across pages.
///
/// Measures byte allocations per page read to quantify redundant copies.
/// Uses [ThreadMXBean#getThreadAllocatedBytes(long)] for precise per-thread measurement.
///
/// Run this test before and after refactoring to compare allocation rates.
public class PageDecodeAllocationProfileTest {

    private static final int MEASUREMENT_ITERATIONS = 5;

    private static final String[] PROFILING_FILES = {
            "profiling_uncompressed_plain.parquet",
            "profiling_snappy_plain.parquet",
            "profiling_zstd_plain.parquet",
            "profiling_uncompressed_dict.parquet",
            "profiling_snappy_dict.parquet",
    };

    /// The decode buffers of the profiling task, which runs on a context's pool.
    private DecodeBuffers buffers;

    /// Profiles as one task of a context's pool, the only place pages decode, on whose thread the
    /// profile counts the allocations.
    @Test
    void profilePageDecodeAllocations() throws Throwable {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (HardwoodContextImpl profiling = HardwoodContextImpl.create(1)) {
            profiling.decode(taskBuffers -> {
                buffers = taskBuffers;
                try {
                    profile();
                }
                catch (Throwable t) {
                    failure.set(t);
                }
                return null;
            });
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    private void profile() throws Exception {
        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long threadId = Thread.currentThread().threadId();

        System.out.println("=".repeat(100));
        System.out.println("PAGE DECODE ALLOCATION PROFILE — BASELINE");
        System.out.println("=".repeat(100));
        System.out.println();

        Map<String, FileResult> allResults = new LinkedHashMap<>();

        for (String fileName : PROFILING_FILES) {
            Path file = Paths.get("src/test/resources/" + fileName);
            if (!file.toFile().exists()) {
                System.out.println("SKIP: " + fileName + " (not found)");
                continue;
            }

            FileResult result = profileFile(file, threadMXBean, threadId);
            allResults.put(fileName, result);
        }

        // Print summary table
        System.out.println();
        System.out.println("=".repeat(100));
        System.out.println("SUMMARY");
        System.out.println("=".repeat(100));
        System.out.printf("%-45s %8s %14s %14s %14s%n",
                "File", "Pages", "Total Alloc", "Avg/Page", "Uncompressed");
        System.out.println("-".repeat(100));

        for (Map.Entry<String, FileResult> entry : allResults.entrySet()) {
            FileResult r = entry.getValue();
            System.out.printf("%-45s %8d %11.1f KB %11.1f KB %11.1f KB%n",
                    entry.getKey(),
                    r.pageCount,
                    r.totalAllocatedBytes / 1024.0,
                    r.avgAllocatedBytesPerPage / 1024.0,
                    r.totalUncompressedBytes / 1024.0);
        }

        System.out.println("-".repeat(100));
        System.out.println();

        // Print allocation ratio (allocated / uncompressed data = overhead multiplier)
        System.out.printf("%-45s %8s %14s%n", "File", "Pages", "Alloc Ratio");
        System.out.println("-".repeat(70));
        for (Map.Entry<String, FileResult> entry : allResults.entrySet()) {
            FileResult r = entry.getValue();
            double ratio = r.totalUncompressedBytes > 0
                    ? (double) r.totalAllocatedBytes / r.totalUncompressedBytes
                    : 0;
            System.out.printf("%-45s %8d %13.2fx%n",
                    entry.getKey(), r.pageCount, ratio);
        }
        System.out.println("-".repeat(70));
        System.out.println();
        System.out.println("Allocation Ratio = total bytes allocated / total uncompressed page bytes");
        System.out.println("Ideal ratio is ~1.0x (one allocation for the output array only).");
        System.out.println("Ratios > 2.0x indicate redundant copies in the pipeline.");
    }

    private FileResult profileFile(Path file, ThreadMXBean threadMXBean, long threadId) throws Exception {
        FileMetaData fileMetaData;
        FileSchema schema;

        MappedInputFile inputFile = new MappedInputFile(file);
        inputFile.open();

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        List<PageProfile> pageProfiles = new ArrayList<>();
        long totalAllocated = 0;
        long totalUncompressed = 0;

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {

            List<RowGroup> rowGroupList = fileMetaData.rowGroups();
            for (int rgIdx = 0; rgIdx < rowGroupList.size(); rgIdx++) {
                RowGroup rowGroup = rowGroupList.get(rgIdx);
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                    ColumnSchema columnSchema = schema.getColumn(colIdx);
                    ColumnMetaData meta = columnChunk.metaData();
                    Long dictOffset = meta.dictionaryPageOffset();
                    long chunkStart = (dictOffset != null && dictOffset > 0) ? dictOffset : meta.dataPageOffset();
                    int chunkLen = Math.toIntExact(meta.totalCompressedSize());
                    ByteBuffer chunkData = inputFile.readRange(chunkStart, chunkLen);

                    SequentialFetchPlan plan = SequentialFetchPlan.build(
                            inputFile, columnSchema, columnChunk,
                            context, rgIdx, inputFile.name(), 0);
                    List<PageInfo> pages = new ArrayList<>();
                    PageIterator iter = plan.pages();
                    while (iter.hasNext()) {
                        pages.add(iter.next());
                    }

                    InPlacePageReads reads = new InPlacePageReads();
                    for (PageInfo pageInfo : pages) {
                        // Warm up: read once to load classes and JIT, and to size the reused arrays
                        PageDecoder warmupReader = new PageDecoder(
                                pageInfo.columnMetaData(), pageInfo.columnSchema(),
                                context.decompressorFactory());
                        reads.read(warmupReader, columnSchema, pageInfo.pageData(), pageInfo.dictionary(),
                                buffers);

                        // Measure: read multiple times and take the minimum
                        // to filter out JVM noise (JIT recompilation, TLAB refills, etc.)
                        long minAllocated = Long.MAX_VALUE;
                        Object values = null;

                        for (int m = 0; m < MEASUREMENT_ITERATIONS; m++) {
                            long beforeBytes = threadMXBean.getThreadAllocatedBytes(threadId);

                            PageDecoder pageDecoder = new PageDecoder(
                                    pageInfo.columnMetaData(), pageInfo.columnSchema(),
                                    context.decompressorFactory());
                            values = reads.read(pageDecoder, columnSchema, pageInfo.pageData(),
                                    pageInfo.dictionary(), buffers);

                            long afterBytes = threadMXBean.getThreadAllocatedBytes(threadId);
                            long allocated = afterBytes - beforeBytes;
                            minAllocated = Math.min(minAllocated, allocated);
                        }

                        long allocated = minAllocated;
                        int numValues = pageInfo.records();
                        int uncompressedSize = estimatePageDataSize(values, numValues);

                        pageProfiles.add(new PageProfile(
                                columnSchema.name(),
                                pageInfo.columnMetaData().codec().name(),
                                values.getClass().getSimpleName(),
                                numValues,
                                allocated,
                                uncompressedSize));

                        totalAllocated += allocated;
                        totalUncompressed += uncompressedSize;
                    }
                }
            }
        }

        // Print per-column breakdown
        System.out.println("--- " + file.getFileName() + " ---");
        System.out.printf("  %-20s %-15s %-15s %8s %12s %12s %8s%n",
                "Column", "Codec", "PageType", "Values", "Allocated", "DataSize", "Ratio");

        Map<String, long[]> columnTotals = new LinkedHashMap<>();

        for (PageProfile p : pageProfiles) {
            String key = p.columnName;
            columnTotals.computeIfAbsent(key, k -> new long[3]);
            long[] totals = columnTotals.get(key);
            totals[0] += p.allocatedBytes;
            totals[1] += p.uncompressedDataSize;
            totals[2]++;
        }

        for (Map.Entry<String, long[]> entry : columnTotals.entrySet()) {
            long[] totals = entry.getValue();
            double ratio = totals[1] > 0 ? (double) totals[0] / totals[1] : 0;
            PageProfile sample = pageProfiles.stream()
                    .filter(p -> p.columnName.equals(entry.getKey()))
                    .findFirst().orElseThrow();
            System.out.printf("  %-20s %-15s %-15s %8d %9.1f KB %9.1f KB %7.2fx%n",
                    entry.getKey(),
                    sample.codec,
                    sample.pageType,
                    totals[2],
                    totals[0] / 1024.0,
                    totals[1] / 1024.0,
                    ratio);
        }

        System.out.println();

        return new FileResult(
                pageProfiles.size(),
                totalAllocated,
                pageProfiles.isEmpty() ? 0 : totalAllocated / pageProfiles.size(),
                totalUncompressed);
    }

    /// The bytes the page's values take in the array they were written to: `numValues` elements
    /// of a primitive array, or a plain binary page's value bytes, which the array may exceed.
    private int estimatePageDataSize(Object values, int numValues) {
        return switch (values) {
            case long[] a -> numValues * 8;
            case double[] a -> numValues * 8;
            case int[] a -> numValues * 4;
            case float[] a -> numValues * 4;
            case boolean[] a -> numValues; // 1 byte per boolean in array
            case byte[] a -> a.length;
            default -> throw new IllegalArgumentException("Unexpected values: " + values.getClass());
        };
    }

    record PageProfile(
            String columnName,
            String codec,
            String pageType,
            int numValues,
            long allocatedBytes,
            int uncompressedDataSize) {
    }

    record FileResult(
            int pageCount,
            long totalAllocatedBytes,
            long avgAllocatedBytesPerPage,
            long totalUncompressedBytes) {
    }
}
