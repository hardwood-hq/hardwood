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

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.Page;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageReader;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms1g", "-Xmx1g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class PageHandlingBenchmark {

    @Param({})
    private String dataDir;

    @Param("yellow_tripdata_2025-05.parquet")
    private String fileName;

    private Path path;
    private FileChannel channel;
    private HardwoodContextImpl context;
    private List<PageInfo> allPages;

    @Setup
    public void setup() throws IOException {
        path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path +
                    ". Run './mvnw verify -Pperformance-test' first to download test data.");
        }

        channel = FileChannel.open(path, StandardOpenOption.READ);
        context = HardwoodContextImpl.create();
        allPages = new ArrayList<>();

        // Scan all pages from all columns in all row groups
        try (ParquetFileReader reader = ParquetFileReader.open(path)) {
            FileSchema schema = reader.getFileSchema();
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();

            // Calculate data region bounds
            long minOffset = Long.MAX_VALUE;
            long maxEnd = 0;
            for (RowGroup rowGroup : rowGroups) {
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnMetaData metaData = rowGroup.columns().get(colIdx).metaData();
                    Long dictOffset = metaData.dictionaryPageOffset();
                    long chunkStart = (dictOffset != null && dictOffset > 0) ? dictOffset : metaData.dataPageOffset();
                    long chunkEnd = chunkStart + metaData.totalCompressedSize();
                    minOffset = Math.min(minOffset, chunkStart);
                    maxEnd = Math.max(maxEnd, chunkEnd);
                }
            }

            // Create single file mapping
            MappedByteBuffer fileMapping = channel.map(FileChannel.MapMode.READ_ONLY, minOffset, maxEnd - minOffset);

            for (RowGroup rowGroup : rowGroups) {
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                    ColumnSchema columnSchema = schema.getColumn(colIdx);

                    PageScanner scanner = new PageScanner(columnSchema, columnChunk, context, fileMapping, minOffset);
                    allPages.addAll(scanner.scanPages());
                }
            }
        }

        System.out.println("Scanned " + allPages.size() + " pages from " + path.getFileName());
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
    public void a_decompressPages(Blackhole blackhole) throws IOException {
        for (PageInfo pageInfo : allPages) {
            MappedByteBuffer pageData = pageInfo.pageData();

            // Parse page header to get compressed/uncompressed sizes
            ThriftCompactReader headerReader = new ThriftCompactReader(pageData, 0);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            int compressedSize = header.compressedPageSize();
            int uncompressedSize = header.uncompressedPageSize();

            // Slice compressed data
            MappedByteBuffer compressedData = pageData.slice(headerSize, compressedSize);

            // Decompress using the file's actual codec
            Decompressor decompressor = context.decompressorFactory().getDecompressor(pageInfo.columnMetaData().codec());
            byte[] decompressed = decompressor.decompress(compressedData, uncompressedSize);
            blackhole.consume(decompressed);
        }
    }

    @Benchmark
    public void b_decodePages(Blackhole blackhole) throws IOException {
        for (PageInfo pageInfo : allPages) {
            PageReader pageReader = new PageReader(pageInfo.columnMetaData(), pageInfo.columnSchema(), context.decompressorFactory());
            Page page = pageReader.decodePage(pageInfo.pageData(), pageInfo.dictionary());
            blackhole.consume(page);
        }
    }
}
