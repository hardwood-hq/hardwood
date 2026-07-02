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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.FixedSizeListDetector;
import dev.hardwood.internal.reader.FixedSizeListShape;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.SequentialFetchPlan;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Measures the reference points for the fixed-size-list read fast path over a
/// sweep of vector lengths `k`:
///
/// - `fastPathList` — the LIST-encoded vectors decoded via the fast path.
/// - `naiveList` — the same LIST files with the fast path disabled (the
///   record-reconstruction baseline being beaten).
/// - `flatFloor` — the identical values as a plain float32 column (the floor).
/// - `repScanOnly` — the fixed-size-list detector alone, run over each page's
///   raw repetition/definition level regions with no value decode. This
///   isolates the level-scan cost from the rest of the fast path, so the
///   detector's share of `fastPathList` is visible directly (e.g. to judge
///   whether vectorizing the small-k bit-packed scan is worthwhile).
///
/// The gap between `naiveList` and `flatFloor` is the reconstruction overhead;
/// the position of `fastPathList` between them is the fraction recovered. All
/// files hold the same number of leaf values, so times are directly comparable
/// across `k`.
///
/// Generate the corpus first:
/// ```
/// python performance-testing/generate_fixed_size_list_data.py <dataDir>
/// ```
/// then run with `-p dataDir=<dataDir>`.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2g", "-Xmx2g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class FixedSizeListDecodeBenchmark {

    private static final String LIST_COLUMN = "vec.list.element";
    private static final String FLAT_COLUMN = "value";

    @Param({})
    private String dataDir;

    // k=3 covers the multi-byte-period bit-packed regime (RGB / 3D vectors);
    // 4 and 8 are single-byte-period; 16 and up carry an RLE interior.
    @Param({ "3", "4", "8", "16", "128", "768", "1536" })
    private int k;

    private Path listPath;
    private Path listPathV1;
    private Path flatPath;
    private List<LevelRegion> levelRegions;

    /// One shared context (thread pool + native pools) backs both configs. The
    /// fast path explicitly enabled and disabled (the reconstruction baseline)
    /// differ only in [ReaderConfig], measured on the same files. The fast path is
    /// opt-in (off by default), so `fastConfig` sets the option rather than
    /// relying on the defaults.
    private HardwoodContext context;
    private ReaderConfig fastConfig;
    private ReaderConfig noFastConfig;

    /// One page's raw, uncompressed level regions plus the counts the detector
    /// needs — copied out of the file so timing touches only in-memory bytes.
    private record LevelRegion(byte[] rep, byte[] def, int numValues, int numRows,
                               int maxRepetitionLevel, int maxDefinitionLevel) {}

    @Setup(Level.Trial)
    public void setup() throws IOException {
        listPath = resolve("fixed_size_list_k" + k + ".parquet");
        listPathV1 = resolve("fixed_size_list_k" + k + "_v1.parquet");
        flatPath = resolve("fixed_size_list_k" + k + "_flat.parquet");
        levelRegions = extractLevelRegions(listPath);
        context = HardwoodContext.create();
        fastConfig = ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "true").build();
        noFastConfig = ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "false").build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
    }

    @Benchmark
    public double fastPathList() throws IOException {
        return sumColumn(listPath, LIST_COLUMN, context, fastConfig);
    }

    @Benchmark
    public double naiveList() throws IOException {
        return sumColumn(listPath, LIST_COLUMN, context, noFastConfig);
    }

    @Benchmark
    public double fastPathListV1() throws IOException {
        return sumColumn(listPathV1, LIST_COLUMN, context, fastConfig);
    }

    @Benchmark
    public double naiveListV1() throws IOException {
        return sumColumn(listPathV1, LIST_COLUMN, context, noFastConfig);
    }

    @Benchmark
    public double flatFloor() throws IOException {
        return sumColumn(flatPath, FLAT_COLUMN, context, fastConfig);
    }

    @Benchmark
    public long repScanOnly() {
        long acc = 0;
        for (LevelRegion region : levelRegions) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    region.rep(), 0, region.rep().length,
                    region.def(), 0, region.def().length,
                    region.numValues(), region.numRows(),
                    region.maxRepetitionLevel(), region.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.FixedWidth fixedWidth) {
                acc += fixedWidth.k();
            }
        }
        return acc;
    }

    private static double sumColumn(Path path, String column, HardwoodContext context,
                                    ReaderConfig config) throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context, config);
             ColumnReader col = reader.columnReader(column)) {
            while (col.nextBatch()) {
                float[] values = (float[]) col.getFloats();
                int n = col.getValueCount();
                for (int i = 0; i < n; i++) {
                    sum += values[i];
                }
            }
        }
        return sum;
    }

    /// Reads every DataPageV2 page of the LIST column and copies out its
    /// (uncompressed) repetition/definition level regions. Done once in setup so
    /// the `repScanOnly` benchmark times only the detector, not I/O or header
    /// parsing.
    private static List<LevelRegion> extractLevelRegions(Path path) throws IOException {
        List<LevelRegion> regions = new ArrayList<>();
        InputFile inputFile = InputFile.of(path);
        inputFile.open();
        HardwoodContextImpl context = HardwoodContextImpl.create();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            FileSchema schema = reader.getFileSchema();
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();
            for (int rg = 0; rg < rowGroups.size(); rg++) {
                RowGroup rowGroup = rowGroups.get(rg);
                for (int col = 0; col < rowGroup.columns().size(); col++) {
                    ColumnChunk chunk = rowGroup.columns().get(col);
                    ColumnSchema columnSchema = schema.getColumn(col);
                    SequentialFetchPlan plan = SequentialFetchPlan.build(
                            inputFile, columnSchema, chunk, context, rg, inputFile.name(), 0);
                    Iterator<PageInfo> pages = plan.pages();
                    while (pages.hasNext()) {
                        PageInfo pageInfo = pages.next();
                        ByteBuffer pageBuffer = pageInfo.pageData();
                        if (pageBuffer == null) {
                            continue;
                        }
                        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
                        PageHeader header = PageHeaderReader.read(headerReader);
                        if (header.type() != PageHeader.PageType.DATA_PAGE_V2) {
                            continue;
                        }
                        ByteBuffer body = pageBuffer.slice(headerReader.getBytesRead(), header.compressedPageSize());
                        DataPageHeaderV2 v2 = header.dataPageHeaderV2();
                        int repLen = v2.repetitionLevelsByteLength();
                        int defLen = v2.definitionLevelsByteLength();
                        byte[] rep = new byte[repLen];
                        body.slice(0, repLen).get(rep);
                        byte[] def = new byte[defLen];
                        body.slice(repLen, defLen).get(def);
                        regions.add(new LevelRegion(rep, def, v2.numValues(), v2.numRows(),
                                columnSchema.maxRepetitionLevel(), columnSchema.maxDefinitionLevel()));
                    }
                }
            }
        }
        inputFile.close();
        context.close();
        return regions;
    }

    private Path resolve(String fileName) {
        Path path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Benchmark file not found: " + path
                    + ". Run 'python performance-testing/generate_fixed_size_list_data.py " + dataDir + "' first.");
        }
        return path;
    }
}
