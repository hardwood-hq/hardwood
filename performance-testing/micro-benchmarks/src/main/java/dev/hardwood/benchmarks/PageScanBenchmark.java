/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/**
 * Benchmarks page scanning performance, comparing sequential header-based
 * scanning against offset-index-based lookup.
 * <p>
 * JMH cross-products the {@code fileName} parameter so a single benchmark
 * method produces results for both paths:
 * <ul>
 *   <li>{@code page_scan_with_index.parquet} &rarr; {@code scanPagesFromIndex()}</li>
 *   <li>{@code page_scan_no_index.parquet} &rarr; {@code scanPagesSequential()}</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms1g", "-Xmx1g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class PageScanBenchmark {

    @Param({})
    private String dataDir;

    @Param({ "page_scan_with_index.parquet", "page_scan_no_index.parquet" })
    private String fileName;

    private FileChannel channel;
    private HardwoodContextImpl context;
    private MappedByteBuffer fileMapping;
    private long minOffset;
    private List<ScanTarget> scanTargets;

    @Setup
    public void setup() throws IOException {
        Path path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path +
                    ". Run 'python performance-testing/generate_benchmark_data.py' first.");
        }

        channel = FileChannel.open(path, StandardOpenOption.READ);
        context = HardwoodContextImpl.create();
        scanTargets = new ArrayList<>();

        try (ParquetFileReader reader = ParquetFileReader.open(path)) {
            FileSchema schema = reader.getFileSchema();
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();

            // Calculate data region bounds
            long min = Long.MAX_VALUE;
            long maxEnd = 0;
            for (RowGroup rowGroup : rowGroups) {
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnMetaData metaData = rowGroup.columns().get(colIdx).metaData();
                    Long dictOffset = metaData.dictionaryPageOffset();
                    long chunkStart = (dictOffset != null && dictOffset > 0) ? dictOffset : metaData.dataPageOffset();
                    long chunkEnd = chunkStart + metaData.totalCompressedSize();
                    min = Math.min(min, chunkStart);
                    maxEnd = Math.max(maxEnd, chunkEnd);
                }
            }

            // Include offset index region if present
            for (RowGroup rowGroup : rowGroups) {
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnChunk cc = rowGroup.columns().get(colIdx);
                    if (cc.offsetIndexOffset() != null) {
                        long indexEnd = cc.offsetIndexOffset() + cc.offsetIndexLength();
                        maxEnd = Math.max(maxEnd, indexEnd);
                    }
                }
            }

            minOffset = min;
            fileMapping = channel.map(FileChannel.MapMode.READ_ONLY, minOffset, maxEnd - minOffset);

            for (RowGroup rowGroup : rowGroups) {
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                    ColumnSchema columnSchema = schema.getColumn(colIdx);
                    scanTargets.add(new ScanTarget(columnSchema, columnChunk));
                }
            }
        }

        System.out.println("Prepared " + scanTargets.size() + " scan targets from " + path.getFileName());
    }

    @TearDown
    public void tearDown() throws IOException {
        if (channel != null) {
            channel.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @Benchmark
    public void scanPages(Blackhole blackhole) throws IOException {
        for (ScanTarget target : scanTargets) {
            PageScanner scanner = new PageScanner(
                    target.columnSchema, target.columnChunk, context, fileMapping, minOffset);
            List<PageInfo> pages = scanner.scanPages();
            blackhole.consume(pages);
        }
    }

    private record ScanTarget(ColumnSchema columnSchema, ColumnChunk columnChunk) {}
}
