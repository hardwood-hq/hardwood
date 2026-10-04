/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.BinaryDictionary;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.schema.ColumnProjection;

/// SPIKE harness: 1BRC over `measurements.parquet`, aggregating by dictionary id on one consumer
/// thread, through `ColumnReaders` (`base`) or [ScatterScan] (`scatter`).
///
/// Modes: `base`, `scatter` (one consumer), `basesplit` (one consumer per byte-range split,
/// `threads` of them), `scatterpar` (aggregation on the pool thread completing each batch).
///
/// Args: `file mode threads runs [window]`. Prints the result on stdout, per-run times on stderr.
public final class ScatterOneBrc {

    public static void main(String[] args) throws Exception {
        Path file = Path.of(args[0]);
        String mode = args[1];
        int threads = Integer.parseInt(args[2]);
        int runs = Integer.parseInt(args[3]);
        int window = args.length > 4 ? Integer.parseInt(args[4]) : 16;
        String result = null;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads)) {
            for (int run = 0; run < runs; run++) {
                long t0 = System.nanoTime();
                StationTable table = switch (mode) {
                    case "base" -> base(file, context);
                    case "scatter" -> scatter(file, window, context);
                    case "basesplit" -> baseSplit(file, threads, context);
                    case "scatterpar" -> scatterParallel(file, window, context);
                    default -> throw new IllegalArgumentException(mode);
                };
                result = table.format();
                System.err.printf("%s run %d: %d ms%n", mode, run + 1, (System.nanoTime() - t0) / 1_000_000);
            }
        }
        System.out.println(result);
    }

    static StationTable base(Path file, HardwoodContextImpl context) throws Exception {
        return base(file, context, null);
    }

    private static ParquetFileReader.ColumnReadersBuilder builder(ParquetFileReader reader, RowGroupPredicate split) {
        ParquetFileReader.ColumnReadersBuilder builder =
                reader.buildColumnReaders(ColumnProjection.columns("station", "measurement"));
        return split == null ? builder : builder.filter(split);
    }

    static StationTable baseSplit(Path file, int splits, HardwoodContextImpl context) throws Exception {
        long size = file.toFile().length();
        ExecutorService consumers = Executors.newFixedThreadPool(splits);
        try {
            List<Future<StationTable>> parts = new ArrayList<>();
            for (int s = 0; s < splits; s++) {
                long from = size * s / splits;
                long to = size * (s + 1) / splits;
                parts.add(consumers.submit(() -> base(file, context, RowGroupPredicate.byteRange(from, to))));
            }
            StationTable merged = new StationTable();
            for (Future<StationTable> part : parts) {
                part.get().mergeInto(merged);
            }
            return merged;
        }
        finally {
            consumers.shutdown();
        }
    }

    static StationTable base(Path file, HardwoodContextImpl context, RowGroupPredicate split) throws Exception {
        StationTable table = new StationTable();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
                ColumnReaders columns = builder(reader, split).build()) {
            ColumnReader station = columns.getColumnReader("station");
            ColumnReader measurement = columns.getColumnReader("measurement");
            BinaryDictionary current = null;
            int[] slotOfEntry = null;
            while (columns.nextBatch()) {
                int count = columns.getRecordCount();
                double[] values = measurement.getDoubles();
                int[] ids = station.getDictionaryIds();
                BinaryDictionary dictionary = station.getBinaryDictionary();
                if (dictionary != current) {
                    slotOfEntry = new int[dictionary.size()];
                    for (int e = 0; e < dictionary.size(); e++) {
                        byte[] entry = dictionary.getBinary(e);
                        slotOfEntry[e] = table.slot(entry, 0, entry.length);
                    }
                    current = dictionary;
                }
                table.addAll(slotOfEntry, ids, values, count);
            }
        }
        return table;
    }

    static StationTable scatter(Path path, int window, HardwoodContextImpl context) throws Exception {
        StationTable table = new StationTable();
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer file = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
            FileMetaData md;
            try (InputFile in = InputFile.of(file)) {
                in.open();
                md = ParquetMetadataReader.readMetadata(in);
            }
            try (ScatterScan scan = new ScatterScan(file, md, List.of("station", "measurement"), 131066,
                    window, context)) {
                Object current = null;
                int[] slotOfEntry = null;
                ScatterScan.Batch b;
                while ((b = scan.next()) != null) {
                    Dictionary.ByteArrayDictionary dictionary = (Dictionary.ByteArrayDictionary) b.dictionaries[0];
                    if (dictionary != current) {
                        byte[] bytes = dictionary.entryBytes();
                        int[] offsets = dictionary.entryOffsets();
                        slotOfEntry = new int[dictionary.size()];
                        for (int e = 0; e < dictionary.size(); e++) {
                            slotOfEntry[e] = table.slot(bytes, offsets[e], offsets[e + 1]);
                        }
                        current = dictionary;
                    }
                    table.addAll(slotOfEntry, (int[]) b.values[0], (double[]) b.values[1], b.rowCount);
                }
            }
        }
        return table;
    }

    static StationTable scatterParallel(Path path, int window, HardwoodContextImpl context) throws Exception {
        ConcurrentLinkedQueue<StationTable> tables = new ConcurrentLinkedQueue<>();
        ThreadLocal<StationTable> local = ThreadLocal.withInitial(() -> {
            StationTable t = new StationTable();
            tables.add(t);
            return t;
        });
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer file = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());
            FileMetaData md;
            try (InputFile in = InputFile.of(file)) {
                in.open();
                md = ParquetMetadataReader.readMetadata(in);
            }
            try (ScatterScan scan = new ScatterScan(file, md, List.of("station", "measurement"), 131066,
                    window, context, b -> {
                        StationTable table = local.get();
                        int[] slotOfEntry = table.slotsFor((Dictionary.ByteArrayDictionary) b.dictionaries[0]);
                        table.addAll(slotOfEntry, (int[]) b.values[0], (double[]) b.values[1], b.rowCount);
                    })) {
                scan.awaitCompletion();
            }
        }
        StationTable merged = new StationTable();
        for (StationTable t : tables) {
            t.mergeInto(merged);
        }
        return merged;
    }

    /// Open-addressed table, as in the 1BRC harness's `CalculateAverageColumnReaderDictionary`.
    static final class StationTable {
        private static final int CAPACITY = 1 << 15;
        private static final int MASK = CAPACITY - 1;
        private final byte[][] keys = new byte[CAPACITY][];
        private final long[] min = new long[CAPACITY];
        private final long[] max = new long[CAPACITY];
        private final long[] sum = new long[CAPACITY];
        private final long[] count = new long[CAPACITY];

        int slot(byte[] buffer, int from, int to) {
            int index = hash(buffer, from, to) & MASK;
            while (true) {
                byte[] key = keys[index];
                if (key == null) {
                    keys[index] = Arrays.copyOfRange(buffer, from, to);
                    min[index] = Long.MAX_VALUE;
                    max[index] = Long.MIN_VALUE;
                    return index;
                }
                if (Arrays.equals(key, 0, key.length, buffer, from, to)) {
                    return index;
                }
                index = (index + 1) & MASK;
            }
        }

        private Object lastDictionary;
        private int[] lastSlots;

        /// Slot per dictionary entry, cached for the dictionary seen last.
        int[] slotsFor(Dictionary.ByteArrayDictionary dictionary) {
            if (dictionary != lastDictionary) {
                byte[] bytes = dictionary.entryBytes();
                int[] offsets = dictionary.entryOffsets();
                lastSlots = new int[dictionary.size()];
                for (int e = 0; e < lastSlots.length; e++) {
                    lastSlots[e] = slot(bytes, offsets[e], offsets[e + 1]);
                }
                lastDictionary = dictionary;
            }
            return lastSlots;
        }

        void mergeInto(StationTable target) {
            for (int i = 0; i < CAPACITY; i++) {
                if (keys[i] == null) {
                    continue;
                }
                int t = target.slot(keys[i], 0, keys[i].length);
                target.min[t] = Math.min(target.min[t], min[i]);
                target.max[t] = Math.max(target.max[t], max[i]);
                target.sum[t] += sum[i];
                target.count[t] += count[i];
            }
        }

        void addAll(int[] slotOfEntry, int[] ids, double[] values, int n) {
            for (int i = 0; i < n; i++) {
                int slot = slotOfEntry[ids[i]];
                long tenths = Math.round(values[i] * 10.0);
                if (tenths < min[slot]) {
                    min[slot] = tenths;
                }
                if (tenths > max[slot]) {
                    max[slot] = tenths;
                }
                sum[slot] += tenths;
                count[slot]++;
            }
        }

        private static int hash(byte[] buffer, int from, int to) {
            int hash = 0x811c9dc5;
            for (int i = from; i < to; i++) {
                hash = (hash ^ buffer[i]) * 0x01000193;
            }
            return hash ^ (hash >>> 15);
        }

        String format() {
            Map<String, String> sorted = new TreeMap<>();
            for (int i = 0; i < CAPACITY; i++) {
                if (keys[i] != null) {
                    sorted.put(new String(keys[i], StandardCharsets.UTF_8),
                            round(min[i] / 10.0) + "/" + round(sum[i] / 10.0 / count[i]) + "/" + round(max[i] / 10.0));
                }
            }
            StringJoiner joiner = new StringJoiner(", ", "{", "}");
            sorted.forEach((k, v) -> joiner.add(k + "=" + v));
            return joiner.toString();
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
