/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.sun.management.OperatingSystemMXBean;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// SPIKE harness: `ColumnReaders` against [ScatterScan] over all columns of one file.
///
/// Args: `file threads warmup iterations [window] [modes]`, modes a comma list of `base`, `scatter`.
public final class ScatterSpikeBench {

    private static final OperatingSystemMXBean OS =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public static void main(String[] args) throws Exception {
        Path path = Path.of(args[0]);
        int threads = Integer.parseInt(args[1]);
        int warmup = Integer.parseInt(args[2]);
        int iterations = Integer.parseInt(args[3]);
        int window = args.length > 4 ? Integer.parseInt(args[4]) : 4;
        List<String> modes = args.length > 5 ? Arrays.asList(args[5].split(",")) : List.of("base", "scatter");

        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads)) {
            FileMetaData md = footer(InputFile.of(path));
            FileSchema schema = FileSchema.fromSchemaElements(md.schema());
            List<String> names = new ArrayList<>();
            for (ColumnSchema c : schema.getColumns()) {
                names.add(c.name());
            }
            int batchSize = defaultBatchSize(path, names, context);
            System.out.printf("file=%s rows=%d rgs=%d cols=%d threads=%d batch=%d window=%d%n",
                    path.getFileName(), md.numRows(), md.rowGroups().size(), names.size(), threads,
                    batchSize, window);

            long expected = base(path, names, batchSize, context);
            for (String mode : modes) {
                long[] wall = new long[iterations];
                long[] cpu = new long[iterations];
                for (int i = 0; i < warmup + iterations; i++) {
                    long c0 = OS.getProcessCpuTime();
                    long t0 = System.nanoTime();
                    long sum = mode.equals("base")
                            ? base(path, names, batchSize, context)
                            : scatter(path, names, batchSize, window, context);
                    long t1 = System.nanoTime();
                    long c1 = OS.getProcessCpuTime();
                    if (sum != expected) {
                        throw new AssertionError(mode + ": checksum " + sum + " != " + expected);
                    }
                    if (i >= warmup) {
                        wall[i - warmup] = t1 - t0;
                        cpu[i - warmup] = c1 - c0;
                    }
                }
                Arrays.sort(wall);
                Arrays.sort(cpu);
                System.out.printf("  %-8s wall min %7.1f med %7.1f ms   cpu med %8.1f ms%n", mode,
                        wall[0] / 1e6, wall[iterations / 2] / 1e6, cpu[iterations / 2] / 1e6);
            }
        }
    }

    private static FileMetaData footer(InputFile in) throws Exception {
        try (in) {
            in.open();
            return ParquetMetadataReader.readMetadata(in);
        }
    }

    private static int defaultBatchSize(Path path, List<String> names, HardwoodContextImpl context)
            throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
                ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns(names.toArray(new String[0])))
                        .build()) {
            cols.nextBatch();
            return cols.getRecordCount();
        }
    }

    static long base(Path path, List<String> names, int batchSize, HardwoodContextImpl context)
            throws Exception {
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
                ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns(names.toArray(new String[0])))
                        .batchSize(batchSize).build()) {
            int n = names.size();
            while (cols.nextBatch()) {
                int count = cols.getRecordCount();
                for (int c = 0; c < n; c++) {
                    ColumnReader r = cols.getColumnReader(c);
                    Object values = switch (r.getColumnSchema().type()) {
                        case INT64 -> r.getLongs();
                        case DOUBLE -> r.getDoubles();
                        case INT32 -> r.getInts();
                        case FLOAT -> r.getFloats();
                        case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> r.getDictionaryIds();
                        default -> throw new UnsupportedOperationException();
                    };
                    if (values == null) {
                        throw new IllegalStateException("no dictionary ids for " + names.get(c));
                    }
                    long[] words = r.getLeafValidity().hasNulls() ? r.getLeafValidity().words() : null;
                    sum += consume(values, words, count) * (c + 1);
                }
            }
        }
        return sum;
    }

    static long scatter(Path path, List<String> names, int batchSize, int window, HardwoodContextImpl context)
            throws Exception {
        long sum = 0;
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer file = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
            FileMetaData md = footer(InputFile.of(file));
            try (ScatterScan scan = new ScatterScan(file, md, names, batchSize, window, context)) {
                int n = names.size();
                ScatterScan.Batch b;
                while ((b = scan.next()) != null) {
                    for (int c = 0; c < n; c++) {
                        sum += consume(b.values[c], b.validity[c], b.rowCount) * (c + 1);
                    }
                }
            }
        }
        return sum;
    }

    private static long consume(Object values, long[] words, int count) {
        long sum = 0;
        switch (values) {
            case long[] v -> {
                for (int i = 0; i < count; i++) {
                    if (present(words, i)) {
                        sum += v[i];
                    }
                }
            }
            case double[] v -> {
                for (int i = 0; i < count; i++) {
                    if (present(words, i)) {
                        sum += Double.doubleToRawLongBits(v[i]);
                    }
                }
            }
            case int[] v -> {
                for (int i = 0; i < count; i++) {
                    if (present(words, i)) {
                        sum += v[i];
                    }
                }
            }
            case float[] v -> {
                for (int i = 0; i < count; i++) {
                    if (present(words, i)) {
                        sum += Float.floatToRawIntBits(v[i]);
                    }
                }
            }
            default -> throw new IllegalArgumentException();
        }
        return sum;
    }

    private static boolean present(long[] words, int i) {
        return words == null || (words[i >>> 6] & (1L << i)) != 0;
    }
}
