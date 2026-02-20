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

import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/**
 * Benchmark measuring single-threaded performance for later stages of the Parquet reading pipeline.
 * <p>
 * Earlier stages (decompression, page decoding) are covered by {@link PageHandlingBenchmark}.
 * This benchmark adds:
 * <ul>
 *   <li>{@link #a_assembleColumns} - Synchronous page decoding + assembly into TypedColumnData batches</li>
 *   <li>{@link #b_consumeRows} - Full pipeline with row-oriented access and value conversion</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-XX:+UseZGC", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class PipelineBenchmark {

    private static final int BATCH_SIZE = 8192;

    @Param({})
    private String dataDir;

    @Param("yellow_tripdata_2025-05.parquet")
    private String fileName;

    private Path path;
    private FileChannel channel;
    private HardwoodContext context;
    private FileSchema schema;
    private List<RowGroup> rowGroups;
    private List<List<PageInfo>> pagesByColumn;
    private List<String> columnNames;

    @Setup
    public void setup() throws IOException {
        path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path +
                    ". Run './mvnw verify -Pperformance-test' first to download test data.");
        }

        channel = FileChannel.open(path, StandardOpenOption.READ);
        context = HardwoodContext.create();
        pagesByColumn = new ArrayList<>();
        columnNames = new ArrayList<>();

        int totalPages = 0;
        long totalRows = 0;

        try (ParquetFileReader reader = ParquetFileReader.open(path)) {
            schema = reader.getFileSchema();
            rowGroups = reader.getFileMetaData().rowGroups();

            // Initialize per-column page lists and collect column names
            for (int i = 0; i < schema.getColumnCount(); i++) {
                pagesByColumn.add(new ArrayList<>());
                columnNames.add(schema.getColumn(i).name());
            }

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

            // Scan all pages
            for (RowGroup rowGroup : rowGroups) {
                totalRows += rowGroup.numRows();
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                    ColumnSchema columnSchema = schema.getColumn(colIdx);

                    PageScanner scanner = new PageScanner(columnSchema, columnChunk, context, fileMapping, minOffset);
                    List<PageInfo> pages = scanner.scanPages();
                    totalPages += pages.size();
                    pagesByColumn.get(colIdx).addAll(pages);
                }
            }
        }

        System.out.println("Setup complete: " + totalPages + " pages, " +
                schema.getColumnCount() + " columns, " + totalRows + " rows from " + path.getFileName());
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

    /**
     * Synchronous page decoding + column assembly into TypedColumnData batches.
     * Uses a minimal synchronous assembler to measure pure single-threaded performance
     * without any prefetch queue overhead.
     */
    @Benchmark
    public void a_assembleColumns(Blackhole blackhole) {
        for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
            List<PageInfo> columnPages = pagesByColumn.get(colIdx);
            if (columnPages.isEmpty()) {
                continue;
            }

            ColumnSchema columnSchema = schema.getColumn(colIdx);
            SyncColumnAssembler assembler = new SyncColumnAssembler(columnPages, columnSchema, context.decompressorFactory());

            while (assembler.hasMore()) {
                TypedColumnData batch = assembler.nextBatch(BATCH_SIZE);
                blackhole.consume(batch);
            }
        }
    }

    /**
     * Full pipeline with row-oriented access and value conversion.
     * Uses SyncColumnAssembler for data, BenchmarkRowReader for value conversion.
     * Calls typed accessors based on column type.
     */
    @Benchmark
    public void b_consumeRows(Blackhole blackhole) {
        int columnCount = schema.getColumnCount();

        // Create assemblers for all columns
        SyncColumnAssembler[] assemblers = new SyncColumnAssembler[columnCount];
        for (int i = 0; i < columnCount; i++) {
            assemblers[i] = new SyncColumnAssembler(pagesByColumn.get(i), schema.getColumn(i), context.decompressorFactory());
        }

        BenchmarkRowReader rowReader = new BenchmarkRowReader(schema);

        // Process batches
        while (assemblers[0].hasMore()) {
            // Fetch batch from each column
            TypedColumnData[] batch = new TypedColumnData[columnCount];
            for (int i = 0; i < columnCount; i++) {
                batch[i] = assemblers[i].nextBatch(BATCH_SIZE);
            }

            // Feed to row reader and consume using typed accessors (index-based for performance)
            rowReader.loadBatch(batch);
            while (rowReader.hasNext()) {
                rowReader.next();
                for (int i = 0; i < columnCount; i++) {
                    if (rowReader.isNull(i)) {
                        blackhole.consume(null);
                        continue;
                    }

                    // Use typed accessor based on logical/physical type
                    ColumnSchema col = schema.getColumn(i);
                    LogicalType logicalType = col.logicalType();
                    if (logicalType instanceof LogicalType.StringType) {
                        blackhole.consume(rowReader.getString(i));
                    }
                    else if (logicalType instanceof LogicalType.TimestampType) {
                        blackhole.consume(rowReader.getTimestamp(i));
                    }
                    else if (logicalType instanceof LogicalType.DateType) {
                        blackhole.consume(rowReader.getDate(i));
                    }
                    else if (logicalType instanceof LogicalType.TimeType) {
                        blackhole.consume(rowReader.getTime(i));
                    }
                    else if (logicalType instanceof LogicalType.DecimalType) {
                        blackhole.consume(rowReader.getDecimal(i));
                    }
                    else if (logicalType instanceof LogicalType.UuidType) {
                        blackhole.consume(rowReader.getUuid(i));
                    }
                    else {
                        // Fall back to physical type
                        switch (col.type()) {
                            case INT32 -> blackhole.consume(rowReader.getInt(i));
                            case INT64 -> blackhole.consume(rowReader.getLong(i));
                            case FLOAT -> blackhole.consume(rowReader.getFloat(i));
                            case DOUBLE -> blackhole.consume(rowReader.getDouble(i));
                            case BOOLEAN -> blackhole.consume(rowReader.getBoolean(i));
                            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> blackhole.consume(rowReader.getBinary(i));
                        }
                    }
                }
            }
        }
    }
}
