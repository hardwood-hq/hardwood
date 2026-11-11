/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.PagePruningSteps;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.plan.RowGroupIndexBuffers;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

/// What the ordered page search saves when pruning one row group.
///
/// Fixture: one row group of a single sorted `INT64` column `id`, written in setup with
/// [#ROWS_PER_PAGE] rows per page, so the row group holds exactly `pages` pages and its Column
/// Index records `ASCENDING`.
///
/// The `predicate` parameter picks how much of the row group may match:
///
/// - `point` — `id == v`, one page
/// - `tenPercent` — `id < 10%`, the first tenth of the pages
///
/// `decidePages` against `decidePagesLinear` decides the same parsed bounds with and without the
/// ordered search; `computeMatchingRows` adds the Column Index and Offset Index parse a filtered
/// read pays first, which the search does not change.
///
/// Setup fails unless the Column Index records `ASCENDING` and every benchmarked path keeps
/// exactly the pages whose value run can match.
///
/// Run:
/// ```shell
/// ./mvnw -pl core install -DskipTests
/// ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
/// java -jar performance-testing/micro-benchmarks/target/benchmarks.jar PagePruningBenchmark
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class PagePruningBenchmark {

    private static final int ROWS_PER_PAGE = 20;
    private static final int WRITE_BATCH_ROWS = 64 * 1024;

    @Param({ "1000", "100000" })
    private int pages;

    @Param({ "point", "tenPercent" })
    private String predicate;

    private Path dir;
    private InputFile inputFile;
    private RowGroup rowGroup;
    private RowGroupIndexBuffers indexBuffers;
    private PagePruningSteps.ParsedIndexes parsed;
    private PagePruningSteps.ParsedIndexes parsedLinear;
    private ResolvedPredicate resolved;
    private LogContext logContext;
    private long rowCount;

    @Setup
    public void setup() throws IOException {
        rowCount = (long) pages * ROWS_PER_PAGE;
        dir = Files.createTempDirectory("page-pruning-benchmark");
        Path path = dir.resolve("pages.parquet");
        write(path);

        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            schema = reader.getFileSchema();
            if (reader.getFileMetaData().rowGroups().size() != 1) {
                throw new IllegalStateException("Expected one row group but the fixture has "
                        + reader.getFileMetaData().rowGroups().size());
            }
            rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        }
        inputFile = InputFile.of(path);
        inputFile.open();
        indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);
        parsed = PagePruningSteps.parse(indexBuffers.forColumn(0), 0, rowGroup);
        parsedLinear = parsed.unordered();
        checkFixture();

        Query query = query();
        resolved = FilterPredicateResolver.resolve(query.filter(), schema);
        logContext = new LogContext(path.toString(), 0);
        long expected = expectedKeptRows(query);
        checkKeptRows("decidePages", decidePages(), expected);
        checkKeptRows("decidePagesLinear", decidePagesLinear(), expected);
        checkKeptRows("computeMatchingRows", computeMatchingRows(), expected);
    }

    @TearDown
    public void tearDown() throws IOException {
        inputFile.close();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                Files.delete(file);
            }
        }
        Files.delete(dir);
    }

    /// Deciding the predicate over indexes already parsed, with the ordered search.
    @Benchmark
    public long decidePages() {
        return PagePruningSteps.decide(resolved, parsed, rowCount);
    }

    /// The same decision over the same bounds declared `UNORDERED`, so every page is decided.
    @Benchmark
    public long decidePagesLinear() {
        return PagePruningSteps.decide(resolved, parsedLinear, rowCount);
    }

    /// Parse and decision together, through the entry point a filtered read calls per row group.
    @Benchmark
    public long computeMatchingRows() {
        return PageFilterEvaluator.computeMatchingRows(resolved, rowGroup, indexBuffers, logContext,
                BoundsReadability.ALL).rowCount(rowCount);
    }

    /// A filter, and the values `[from, to)` it matches.
    private record Query(FilterPredicate filter, long from, long to) {
    }

    private Query query() {
        long middle = rowCount / 2;
        return switch (predicate) {
            case "point" -> new Query(FilterPredicate.eq("id", middle + 7), middle + 7, middle + 8);
            case "tenPercent" -> new Query(FilterPredicate.lt("id", rowCount / 10), Long.MIN_VALUE, rowCount / 10);
            default -> throw new IllegalStateException("Unknown predicate: " + predicate);
        };
    }

    /// Fails unless the fixture has one page per [#ROWS_PER_PAGE] rows and its Column Index
    /// records `ASCENDING`, so that `decidePages` does not silently measure the per-page path.
    private void checkFixture() {
        int pageCount = parsed.offsetIndex().pageLocations().size();
        if (pageCount != pages) {
            throw new IllegalStateException("Expected " + pages + " pages but the fixture has " + pageCount);
        }
        ColumnIndex.BoundaryOrder order = parsed.columnIndex().boundaryOrder();
        if (order != ColumnIndex.BoundaryOrder.ASCENDING) {
            throw new IllegalStateException("Expected the fixture to record ASCENDING but its Column Index records "
                    + order);
        }
    }

    /// The rows of every page holding a value in `[query.from, query.to)`: page `i` holds the run
    /// `[i * ROWS_PER_PAGE, (i + 1) * ROWS_PER_PAGE)`.
    private long expectedKeptRows(Query query) {
        long kept = 0;
        for (int page = 0; page < pages; page++) {
            long min = (long) page * ROWS_PER_PAGE;
            long max = min + ROWS_PER_PAGE - 1;
            if (min < query.to() && max >= query.from()) {
                kept += ROWS_PER_PAGE;
            }
        }
        return kept;
    }

    private void checkKeptRows(String path, long kept, long expected) {
        if (kept != expected) {
            throw new IllegalStateException(path + " keeps " + kept + " rows of " + rowCount + " for "
                    + predicate + ", but " + expected + " can match");
        }
    }

    private void write(Path path) throws IOException {
        FileSchema schema = FileSchema.builder("pages")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .pageTargetRows(ROWS_PER_PAGE)
                .pageTargetBytes(1024 * 1024)
                .rowGroupTargetRows(Long.MAX_VALUE)
                .rowGroupBufferTargetBytes(1L << 30)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
            ColumnWriter columns = writer.columnWriter();
            int total = Math.toIntExact(rowCount);
            for (int start = 0; start < total; start += WRITE_BATCH_ROWS) {
                long[] ids = new long[Math.min(WRITE_BATCH_ROWS, total - start)];
                for (int i = 0; i < ids.length; i++) {
                    ids[i] = start + i;
                }
                columns.writeBatch(batch -> batch.longs("id", ids));
            }
        }
    }
}
