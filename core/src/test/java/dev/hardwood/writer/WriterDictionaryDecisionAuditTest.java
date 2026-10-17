/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.LongUnaryOperator;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Audits what `AUTO` decides for each chunk against the exact decision: the flush comparison
/// over every value of the chunk, computed here from the values written.
///
/// `AUTO` decides on a prefix wherever a probe gives a dictionary up, so it can miss a dictionary
/// that would win over the chunk. It must never keep one that loses: a kept dictionary is always
/// decided exactly at flush, and a losing one fails this test outright. Misses are pinned in
/// `dictionary-decision-audit.tsv`, one line per case with its missed chunks and the bytes they
/// cost, so that a change to the probes, the extrapolation or the recovery shows up as a changed
/// line. A case without misses has no line, and no case has one.
class WriterDictionaryDecisionAuditTest {

    private static final int VALUES = 131_072;
    private static final String BASELINE = "dictionary-decision-audit.tsv";

    /// A distribution of `INT64` keys, which each type maps onto its own values.
    private record Distribution(String name, KeyGenerator keys) {
    }

    @FunctionalInterface
    private interface KeyGenerator {
        long[] keys(int count, Random random);
    }

    private static List<Distribution> distributions() {
        long[] zipf1m = zipfCdf(1_000_000, 0.8);
        long[] zipf100k = zipfCdf(100_000, 1.1);
        return List.of(
                new Distribution("all-distinct", (n, r) -> keys(n, i -> i)),
                new Distribution("uniform-1k", (n, r) -> keys(n, i -> r.nextInt(1_000))),
                new Distribution("uniform-10k", (n, r) -> keys(n, i -> r.nextInt(10_000))),
                new Distribution("uniform-100k", (n, r) -> keys(n, i -> r.nextInt(100_000))),
                new Distribution("zipf-1m-0.8", (n, r) -> keys(n, i -> zipf(zipf1m, r))),
                new Distribution("zipf-100k-1.1", (n, r) -> keys(n, i -> zipf(zipf100k, r))),
                // Distinct through 40% of the values, then repeating the first ones.
                new Distribution("front-loaded", (n, r) -> keys(n, i -> i < n * 4L / 10 ? i : i % 5_000)),
                // A clock of second-resolution events: rising, each second repeated a few times,
                // interleaved within a small window.
                new Distribution("time-ordered", (n, r) -> keys(n, i -> i / 3 + r.nextInt(40))),
                // Distinct through 30% of the values, then a fresh set of values that repeat among
                // themselves: neither the prefix nor its repeats foretell it.
                new Distribution("late-local-repeats", (n, r) -> keys(n, i -> i < n * 3L / 10 ? i : 10_000_000L + r.nextInt(10_000))));
    }

    @Test
    void autoNeverKeepsALosingDictionaryAndMissesOnlyWhatTheBaselineExpects() throws IOException {
        List<String> findings = new ArrayList<>();
        List<String> losing = new ArrayList<>();
        Random seeds = new Random(1521);
        for (Distribution distribution : distributions()) {
            long[] keys = distribution.keys().keys(VALUES, new Random(seeds.nextLong()));
            for (int rows : new int[] { 4_096, 16_384, 65_536 }) {
                audit(distribution.name() + "/int64/required/rows-" + rows, Shape.REQUIRED, PhysicalType.INT64, keys,
                        WriterConfig.builder().rowGroupTargetRows(rows), findings, losing);
            }
            // Chunks cut by the byte target, where the chunk's share of it sizes the probes.
            audit(distribution.name() + "/int64/required/bytes-1m", Shape.REQUIRED, PhysicalType.INT64, keys,
                    WriterConfig.builder().rowGroupBufferTargetBytes(1 << 20), findings, losing);
            for (PhysicalType type : new PhysicalType[] { PhysicalType.INT32, PhysicalType.FLOAT, PhysicalType.DOUBLE,
                    PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY }) {
                audit(distribution.name() + "/" + type.name().toLowerCase() + "/required/rows-16384", Shape.REQUIRED, type,
                        keys, WriterConfig.builder().rowGroupTargetRows(16_384), findings, losing);
            }
            for (Shape shape : new Shape[] { Shape.OPTIONAL, Shape.REPEATED }) {
                audit(distribution.name() + "/int64/" + shape.name().toLowerCase() + "/rows-16384", shape, PhysicalType.INT64,
                        keys, WriterConfig.builder().rowGroupTargetRows(16_384), findings, losing);
            }
            // Ten values a record, so a chunk holds ten times the records the row target counts.
            audit(distribution.name() + "/int64/lists-of-ten/rows-4096", Shape.LISTS_OF_TEN, PhysicalType.INT64, keys,
                    WriterConfig.builder().rowGroupTargetRows(4_096), findings, losing);
        }
        auditMixed(findings, losing, seeds);
        auditWide(findings, losing, seeds);

        assertThat(losing).as("chunks written with a dictionary that loses").isEmpty();
        Set<String> expected = baseline();
        Set<String> actual = new LinkedHashSet<>(findings);
        Set<String> unexpected = new LinkedHashSet<>(actual);
        unexpected.removeAll(expected);
        Set<String> missing = new LinkedHashSet<>(expected);
        missing.removeAll(actual);
        assertThat(unexpected).as("findings not in %s (all findings:%n%s)", BASELINE, String.join("\n", actual)).isEmpty();
        assertThat(missing).as("findings in %s that no longer occur", BASELINE).isEmpty();
    }

    /// Columns of unlike cost sharing one byte target, which sizes each column's probes from an
    /// equal share of it: an all-distinct column costs most per value and holds more values than
    /// its share, a column of few values costs least and holds fewer.
    private static void auditMixed(List<String> findings, List<String> losing, Random seeds) throws IOException {
        Distribution[] columns = {
                distributions().get(0), distributions().get(1), distributions().get(4), distributions().get(3) };
        PhysicalType[] types = { PhysicalType.INT64, PhysicalType.BYTE_ARRAY, PhysicalType.INT64, PhysicalType.DOUBLE };
        long[][] keys = new long[columns.length][];
        String[] names = new String[columns.length];
        for (int c = 0; c < columns.length; c++) {
            keys[c] = columns[c].keys().keys(VALUES, new Random(seeds.nextLong()));
            names[c] = "mixed/" + columns[c].name() + "/" + types[c].name().toLowerCase() + "/bytes-2m";
        }
        auditColumns(names, types, keys, 2 << 20, findings, losing);
    }

    /// The same column first and last in a wide schema: its verdict must not depend on where it
    /// sits, though the slice is appended column by column.
    private static void auditWide(List<String> findings, List<String> losing, Random seeds) throws IOException {
        int width = 100;
        Random random = new Random(seeds.nextLong());
        long[] shared = keys(VALUES / 4, i -> random.nextInt(25_000));
        long[] distinct = keys(VALUES / 4, i -> i);
        long[] few = keys(VALUES / 4, i -> i % 1_000);
        PhysicalType[] types = new PhysicalType[width];
        long[][] keys = new long[width][];
        String[] names = new String[width];
        for (int c = 0; c < width; c++) {
            boolean audited = c == 0 || c == width - 1;
            types[c] = audited || c % 2 == 0 ? PhysicalType.INT64 : PhysicalType.BYTE_ARRAY;
            keys[c] = audited ? shared : c % 2 == 0 ? distinct : few;
            names[c] = audited ? "wide/uniform-25k/int64/column-" + c + "/bytes-16m" : null;
        }
        auditColumns(names, types, keys, 16 << 20, findings, losing);
    }

    /// Writes one `REQUIRED` column per entry under a byte target, and compares the chunks of each
    /// named column with the exact decision; a column without a name is written, not audited.
    private static void auditColumns(String[] names, PhysicalType[] types, long[][] keys, long targetBytes,
                                     List<String> findings, List<String> losing) throws IOException {
        FileSchema.Builder schema = FileSchema.builder("m");
        for (int c = 0; c < names.length; c++) {
            schema.addColumn("c" + c, types[c], RepetitionType.REQUIRED);
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema.build(), WriterConfig.builder()
                .rowGroupBufferTargetBytes(targetBytes).codec(CompressionCodec.UNCOMPRESSED).build())) {
            writer.columnWriter().writeBatch(batch -> {
                for (int c = 0; c < names.length; c++) {
                    new Column(Shape.REQUIRED, keys[c], null, null).set(batch, "c" + c, types[c], null);
                }
            });
        }
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            for (int c = 0; c < names.length; c++) {
                String name = names[c];
                if (name == null) {
                    continue;
                }
                int chunks = 0;
                int missed = 0;
                long missedBytes = 0;
                int record = 0;
                for (RowGroup group : reader.getFileMetaData().rowGroups()) {
                    int records = Math.toIntExact(group.numRows());
                    Sizes exact = Sizes.of(types[c], Arrays.copyOfRange(keys[c], record, record + records));
                    record += records;
                    boolean dictionary = group.columns().get(c).metaData().encodings().contains(Encoding.RLE_DICTIONARY);
                    if (dictionary && !exact.dictionaryWins()) {
                        losing.add(name + " chunk " + chunks);
                    }
                    if (!dictionary && exact.dictionaryWins()) {
                        missed++;
                        missedBytes += exact.plain() - exact.dictionary();
                    }
                    chunks++;
                }
                if (missed > 0) {
                    findings.add(name + "\tmissed " + missed + " of " + chunks + "\t" + missedBytes + " bytes");
                }
            }
        }
    }

    // ==================== One case ====================

    private enum Shape { REQUIRED, OPTIONAL, REPEATED, LISTS_OF_TEN }

    /// Writes `keys` as a column of `type` in `shape`, and compares each chunk's encoding with the
    /// exact decision over the chunk's present values.
    private static void audit(String name, Shape shape, PhysicalType type, long[] keys, WriterConfig.Builder config,
                              List<String> findings, List<String> losing) throws IOException {
        Column column = Column.of(shape, keys);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema(shape, type),
                config.codec(CompressionCodec.UNCOMPRESSED).build())) {
            writer.columnWriter().writeBatch(batch -> column.fill(batch, type));
        }
        int chunks = 0;
        int missed = 0;
        long missedBytes = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            int record = 0;
            for (RowGroup group : reader.getFileMetaData().rowGroups()) {
                int records = Math.toIntExact(group.numRows());
                long[] present = column.presentKeys(record, records);
                record += records;
                Sizes exact = Sizes.of(type, present);
                ColumnMetaData meta = group.columns().get(0).metaData();
                boolean dictionary = meta.encodings().contains(Encoding.RLE_DICTIONARY);
                chunks++;
                if (dictionary && !exact.dictionaryWins()) {
                    losing.add(name + " chunk " + (chunks - 1));
                }
                if (!dictionary && exact.dictionaryWins()) {
                    missed++;
                    missedBytes += exact.plain() - exact.dictionary();
                }
            }
        }
        if (missed > 0) {
            findings.add(name + "\tmissed " + missed + " of " + chunks + "\t" + missedBytes + " bytes");
        }
    }

    private static FileSchema schema(Shape shape, PhysicalType type) {
        FileSchema.Builder schema = FileSchema.builder("m");
        if (shape == Shape.REPEATED || shape == Shape.LISTS_OF_TEN) {
            return schema.list("v", RepetitionType.REQUIRED, el -> el.primitive(type, RepetitionType.REQUIRED)).build();
        }
        RepetitionType repetition = shape == Shape.OPTIONAL ? RepetitionType.OPTIONAL : RepetitionType.REQUIRED;
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return schema.addColumn("v", type, repetition, Long.BYTES).build();
        }
        return schema.addColumn("v", type, repetition).build();
    }

    /// The keys as records: one each, one or none (a fifth null), or a list of up to three.
    private record Column(Shape shape, long[] keys, boolean[] nulls, int[] offsets) {

        static Column of(Shape shape, long[] keys) {
            Random random = new Random(keys.length);
            return switch (shape) {
                case REQUIRED -> new Column(shape, keys, null, null);
                case OPTIONAL -> {
                    boolean[] nulls = new boolean[keys.length];
                    for (int i = 0; i < nulls.length; i++) {
                        nulls[i] = random.nextInt(5) == 0;
                    }
                    yield new Column(shape, keys, nulls, null);
                }
                case LISTS_OF_TEN -> {
                    int[] offsets = new int[keys.length / 10 + 1];
                    for (int r = 0; r < offsets.length; r++) {
                        offsets[r] = r * 10;
                    }
                    yield new Column(shape, Arrays.copyOf(keys, offsets[offsets.length - 1]), null, offsets);
                }
                case REPEATED -> {
                    List<Integer> bounds = new ArrayList<>();
                    bounds.add(0);
                    int at = 0;
                    while (at < keys.length) {
                        at = Math.min(keys.length, at + random.nextInt(4));
                        bounds.add(at);
                    }
                    yield new Column(shape, keys, null, bounds.stream().mapToInt(Integer::intValue).toArray());
                }
            };
        }

        void fill(ColumnBatch batch, PhysicalType type) {
            if (shape == Shape.REPEATED || shape == Shape.LISTS_OF_TEN) {
                batch.list("v", offsets);
                set(batch, "v.list.element", type, null);
            }
            else {
                set(batch, "v", type, nulls);
            }
        }

        void set(ColumnBatch batch, String path, PhysicalType type, boolean[] nulls) {
            switch (type) {
                case INT32 -> {
                    int[] v = new int[keys.length];
                    for (int i = 0; i < v.length; i++) {
                        v[i] = (int) int32(keys[i]);
                    }
                    if (nulls == null) {
                        batch.ints(path, v);
                    }
                    else {
                        batch.ints(path, v, nulls);
                    }
                }
                case INT64 -> {
                    long[] v = new long[keys.length];
                    for (int i = 0; i < v.length; i++) {
                        v[i] = keys[i] * 7_919;
                    }
                    if (nulls == null) {
                        batch.longs(path, v);
                    }
                    else {
                        batch.longs(path, v, nulls);
                    }
                }
                case FLOAT -> {
                    float[] v = new float[keys.length];
                    for (int i = 0; i < v.length; i++) {
                        v[i] = (float) float32(keys[i]);
                    }
                    batch.floats(path, v);
                }
                case DOUBLE -> {
                    double[] v = new double[keys.length];
                    for (int i = 0; i < v.length; i++) {
                        v[i] = keys[i] * 0.25;
                    }
                    batch.doubles(path, v);
                }
                case BYTE_ARRAY -> {
                    byte[][] v = new byte[keys.length][];
                    for (int i = 0; i < v.length; i++) {
                        v[i] = string(keys[i]);
                    }
                    batch.bytes(path, v);
                }
                case FIXED_LEN_BYTE_ARRAY -> {
                    byte[][] v = new byte[keys.length][];
                    for (int i = 0; i < v.length; i++) {
                        v[i] = ByteBuffer.allocate(Long.BYTES).putLong(keys[i]).array();
                    }
                    batch.fixed(path, v);
                }
                default -> throw new IllegalArgumentException("Not audited: " + type);
            }
        }

        /// The keys of the present values of records `[from, from + count)`.
        long[] presentKeys(int from, int count) {
            return switch (shape) {
                case REQUIRED -> Arrays.copyOfRange(keys, from, from + count);
                case OPTIONAL -> {
                    long[] present = new long[count];
                    int n = 0;
                    for (int i = from; i < from + count; i++) {
                        if (!nulls[i]) {
                            present[n++] = keys[i];
                        }
                    }
                    yield Arrays.copyOf(present, n);
                }
                case REPEATED, LISTS_OF_TEN -> Arrays.copyOfRange(keys, offsets[from], offsets[from + count]);
            };
        }
    }

    // ==================== The exact decision ====================

    /// A chunk's `PLAIN` size and its dictionary's, as the flush comparison weighs them: the
    /// dictionary page body over the distinct values plus the index stream at the bit width of the
    /// largest index.
    private record Sizes(long plain, long dictionary) {

        boolean dictionaryWins() {
            return dictionary < plain;
        }

        static Sizes of(PhysicalType type, long[] keys) {
            Set<Long> distinct = new HashSet<>();
            for (long key : keys) {
                distinct.add(type == PhysicalType.INT32 ? int32(key)
                        : type == PhysicalType.FLOAT ? Float.floatToRawIntBits((float) float32(key)) : key);
            }
            long n = keys.length;
            long k = distinct.size();
            long plain;
            long entries;
            switch (type) {
                case INT32, FLOAT -> {
                    plain = n * Integer.BYTES;
                    entries = k * Integer.BYTES;
                }
                case INT64, DOUBLE, FIXED_LEN_BYTE_ARRAY -> {
                    plain = n * Long.BYTES;
                    entries = k * Long.BYTES;
                }
                case BYTE_ARRAY -> {
                    plain = 0;
                    for (long key : keys) {
                        plain += Integer.BYTES + string(key).length;
                    }
                    entries = 0;
                    for (long key : distinct) {
                        entries += Integer.BYTES + string(key).length;
                    }
                }
                default -> throw new IllegalArgumentException("Not audited: " + type);
            }
            int bitWidth = k <= 1 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(k - 1);
            return new Sizes(plain, entries + Math.ceilDiv(n * bitWidth, Byte.SIZE));
        }
    }

    /// A key as an `INT32`: wrapped into the type's range, which can map two keys onto one value;
    /// the exact decision counts the values written.
    private static long int32(long key) {
        return Math.floorMod(key * 31, 1L << 32) - (1L << 31);
    }

    private static double float32(long key) {
        return key * 0.5;
    }

    private static byte[] string(long key) {
        return ("key-" + key).getBytes(StandardCharsets.UTF_8);
    }

    // ==================== Generators ====================

    private static long[] keys(int count, LongUnaryOperator key) {
        long[] keys = new long[count];
        for (int i = 0; i < count; i++) {
            keys[i] = key.applyAsLong(i);
        }
        return keys;
    }

    /// The cumulative weights of ranks `1..n` under Zipf's law with exponent `s`, scaled to
    /// `Long.MAX_VALUE`, ranks shuffled onto keys by the caller's draws.
    private static long[] zipfCdf(int n, double s) {
        double[] weights = new double[n];
        double total = 0;
        for (int rank = 0; rank < n; rank++) {
            weights[rank] = 1 / Math.pow(rank + 1, s);
            total += weights[rank];
        }
        long[] cdf = new long[n];
        double sum = 0;
        for (int rank = 0; rank < n; rank++) {
            sum += weights[rank];
            cdf[rank] = (long) (sum / total * Long.MAX_VALUE);
        }
        return cdf;
    }

    private static long zipf(long[] cdf, Random random) {
        int rank = Arrays.binarySearch(cdf, random.nextLong() & Long.MAX_VALUE);
        rank = rank < 0 ? -rank - 1 : rank;
        // A rank's key: spread over the key space so ranks do not sit next to each other.
        return Math.min(rank, cdf.length - 1) * 1_000_003L % 1_000_000_007L;
    }

    private static Set<String> baseline() throws IOException {
        Set<String> expected = new LinkedHashSet<>();
        try (InputStream in = WriterDictionaryDecisionAuditTest.class.getResourceAsStream("/" + BASELINE)) {
            assertThat(in).as(BASELINE).isNotNull();
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    expected.add(line.strip());
                }
            }
        }
        return expected;
    }
}
