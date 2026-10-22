/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Early abandonment of a dictionary the chunk's prefix already shows is losing.
///
/// The probe weighs the same thing the flush-time comparison weighs, so it changes no produced
/// encoding: a chunk it abandons is one flush would have rejected. What it does change is
/// observable in two places, and both are asserted here — the chunk stops interning, which only
/// shows in `distinct_count` going absent, and a column whose distinct values are merely
/// front-loaded must keep its dictionary rather than be judged on its first few thousand values.
class WriterDictionaryProbeTest {

    /// Present values before the first probe of a chunk with room for many values, and the
    /// doubling step after it. Mirrors the constants in `ColumnChunkBuffer`; a test that
    /// hard-codes them is the point, since moving them changes where a chunk decides.
    private static final int FIRST_PROBE = 8_192;
    private static final int SECOND_PROBE = 2 * FIRST_PROBE;

    @Test
    void allDistinctColumnGivesUpItsDictionaryBeforeTheChunkEnds() throws Exception {
        // Every value distinct, so the dictionary loses at both probes and is gone at the second
        // — well before the 200 000 values that follow it are interned.
        int[] values = new int[200_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 31 + 5;
        }

        ColumnMetaData meta = writeAndRead(values);

        assertThat(meta.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
        assertThat(meta.dictionaryPageOffset()).isNull();
        // The chunk released what it was counting with at the probe, so it can no longer state
        // its cardinality. A chunk that reaches flush still holding its dictionary does, which is
        // what this column used to do — the statistic is the only visible cost of abandoning
        // early, and it is the least informative case of it: this count is the value count.
        assertThat(meta.statistics().distinctCount()).as("distinct_count").isNull();
    }

    @Test
    void frontLoadedDistinctColumnKeepsItsDictionary() throws Exception {
        // Distinct through the first probe and repeating afterwards: the shape a single-probe
        // rule would misjudge. At the first probe every value is new and the dictionary is
        // losing; by the second the ratio has halved and it is winning again, so the chunk keeps
        // what it has and is decided at flush — where it wins outright.
        int[] values = new int[200_000];
        for (int i = 0; i < FIRST_PROBE; i++) {
            values[i] = i * 31 + 5;
        }
        for (int i = FIRST_PROBE; i < values.length; i++) {
            values[i] = (i % 8) * 31 + 5;
        }

        ColumnMetaData meta = writeAndRead(values);

        assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY);
        assertThat(meta.dictionaryPageOffset()).isNotNull();
        assertThat(meta.statistics().distinctCount()).as("distinct_count").isEqualTo(FIRST_PROBE);
    }

    @Test
    void lowCardinalityColumnIsNeverAbandoned() throws Exception {
        // Far past both probes, and the dictionary wins at every one of them.
        int[] values = new int[4 * SECOND_PROBE];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 8;
        }

        ColumnMetaData meta = writeAndRead(values);

        assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY);
        assertThat(meta.statistics().distinctCount()).as("distinct_count").isEqualTo(8L);
    }

    @Test
    void chunkShorterThanTheFirstProbeIsDecidedAtFlushAsBefore() throws Exception {
        // No probe fires, so the chunk still holds its dictionary at flush and states the count
        // it counted — the behaviour every fixture below the probe threshold keeps.
        int[] values = new int[FIRST_PROBE - 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 31 + 5;
        }

        ColumnMetaData meta = writeAndRead(values);

        assertThat(meta.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
        assertThat(meta.statistics().distinctCount()).as("distinct_count")
                .isEqualTo((long) values.length);
    }

    @Test
    void abandonedChunkRoundTripsItsValues() throws Exception {
        // The values a chunk abandons mid-flight are the ones already interned, resolved back out
        // of the dictionary, followed by the ones stored directly. Both halves must come back.
        int[] values = new int[SECOND_PROBE + 5_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 31 + 5;
        }

        InMemoryOutputFile out = write(values);
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(out.buffer()))) {
            int[] read = readInts(reader, values.length);
            assertThat(read).containsExactly(values);
        }
    }

    /// Each row group decides for itself: the probe state resets with the chunk, so a second row
    /// group of repeating values keeps the dictionary the first one abandoned.
    @Test
    void probeStateDoesNotCarryIntoTheNextRowGroup() throws Exception {
        int[] allDistinct = new int[SECOND_PROBE + 1_000];
        for (int i = 0; i < allDistinct.length; i++) {
            allDistinct[i] = i * 31 + 5;
        }
        int[] repeating = new int[SECOND_PROBE + 1_000];
        for (int i = 0; i < repeating.length; i++) {
            repeating[i] = i % 4;
        }

        InMemoryOutputFile out = OutputFile.inMemory();
        // A row-group target small enough that each batch lands in a row group of its own.
        WriterConfig config = WriterConfig.builder().rowGroupBufferTargetBytes(64 * 1024).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneIntColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, allDistinct));
            writer.columnWriter().writeBatch(batch -> batch.ints(0, repeating));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(out.buffer()))) {
            boolean anyDictionary = reader.getFileMetaData().rowGroups().stream()
                    .anyMatch(group -> group.columns().get(0).metaData().encodings()
                            .contains(Encoding.RLE_DICTIONARY));
            assertThat(anyDictionary)
                    .as("a later row group of repeating values still earns a dictionary")
                    .isTrue();
        }
    }

    /// A chunk the row target holds to 6,000 records probes at a quarter of them and gives an
    /// all-distinct dictionary up at half, where a fixed schedule would not probe at all.
    @Test
    void chunkCutByTheRowTargetProbesWithinIt() throws Exception {
        int[] values = new int[6_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 31 + 5;
        }

        ColumnMetaData meta = writeAndRead(values, WriterConfig.builder().rowGroupTargetRows(values.length).build());

        assertThat(meta.encodings()).doesNotContain(Encoding.RLE_DICTIONARY);
        assertThat(meta.statistics().distinctCount()).as("distinct_count").isNull();
    }

    /// Interning an all-distinct `INT32` retains about six times its four bytes, so under a small
    /// byte target a row group that never probes is cut at about 11,000 records. Probing within
    /// the chunk's budget gives the dictionary up early, after which a value costs its width, and
    /// the same target holds several times the records.
    @Test
    void chunkUnderASmallByteTargetStopsInterningAndHoldsMoreRecords() throws Exception {
        int[] values = new int[200_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 31 + 5;
        }
        InMemoryOutputFile out = write(values, WriterConfig.builder().rowGroupBufferTargetBytes(256 * 1024).build());

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            RowGroup first = reader.getFileMetaData().rowGroups().get(0);
            assertThat(first.numRows()).isGreaterThan(SECOND_PROBE);
            assertThat(first.columns().get(0).metaData().statistics().distinctCount()).as("distinct_count").isNull();
            // The first differing index rather than containsExactly, which takes seconds over
            // 200,000 values, or isEqualTo, whose message shows neither the index nor the value.
            assertThat(Arrays.mismatch(readInts(reader, values.length), values))
                    .as("first index where the read values differ from the written ones")
                    .isEqualTo(-1);
        }
    }

    /// Distinct through both probes, so the dictionary is given up, and repeating afterwards. The
    /// sketch at flush counts few enough distinct values for a dictionary to win, so flush interns
    /// the chunk again and the exact comparison keeps the dictionary, which states the exact count.
    @Test
    void frontLoadedColumnGivenUpAtAProbeIsRecoveredAtFlush() throws Exception {
        // 60,000 records would probe at 15,000; the cap holds the probes to 8,192 and 16,384, both
        // inside the 30,500 distinct values.
        long[] values = new long[60_000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i < 30_500 ? i : i % 30_500) * 7919L + 3;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(values.length).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneLongColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY);
            assertThat(meta.statistics().distinctCount()).as("distinct_count").isEqualTo(30_500L);
            long[] read = new long[values.length];
            int written = 0;
            try (ColumnReader column = reader.columnReader(0)) {
                while (column.nextBatch()) {
                    System.arraycopy(column.getLongs(), 0, read, written, column.getValueCount());
                    written += column.getValueCount();
                }
            }
            assertThat(read).containsExactly(values);
        }
    }

    /// Values drawn uniformly from 14,000, in chunks of 8,192: a dictionary loses on the chunk's
    /// first few thousand values, nearly all new, and wins over the chunk, whose distinct ratio
    /// falls as it fills. The probes see the count growing ever more slowly and keep interning,
    /// so every chunk is decided at flush and keeps its dictionary.
    @Test
    void columnWhoseDistinctRatioFallsKeepsItsDictionaryInSmallChunks() throws Exception {
        int rows = 8_192;
        long[] values = new long[8 * rows];
        Random random = new Random(7);
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(14_000) * 7919L;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(rows).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneLongColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs(0, values));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(8).allSatisfy(group -> {
                ColumnMetaData meta = group.columns().get(0).metaData();
                assertThat(meta.encodings()).contains(Encoding.RLE_DICTIONARY);
                assertThat(meta.statistics().distinctCount()).as("distinct_count").isNotNull();
            });
        }
    }

    // ==================== Helpers ====================

    private static FileSchema oneLongColumn() {
        return FileSchema.builder("m")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
    }

    private static FileSchema oneIntColumn() {
        return FileSchema.builder("m")
                .addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    private static InMemoryOutputFile write(int[] values) throws Exception {
        return write(values, WriterConfig.defaults());
    }

    private static InMemoryOutputFile write(int[] values, WriterConfig config) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneIntColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, values));
        }
        return out;
    }

    private static ColumnMetaData writeAndRead(int[] values) throws Exception {
        return writeAndRead(values, WriterConfig.defaults());
    }

    private static ColumnMetaData writeAndRead(int[] values, WriterConfig config) throws Exception {
        InMemoryOutputFile out = write(values, config);
        try (ParquetFileReader reader = ParquetFileReader.open(
                InputFile.of(out.buffer()))) {
            assertThat(reader.getFileMetaData().rowGroups())
                    .as("one row group, so the chunk under test is the whole column").hasSize(1);
            return reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
        }
    }

    private static int[] readInts(ParquetFileReader reader, int expected) throws Exception {
        int[] all = new int[expected];
        int written = 0;
        try (ColumnReader column = reader.columnReader(0)) {
            while (column.nextBatch()) {
                int count = column.getValueCount();
                System.arraycopy(column.getInts(), 0, all, written, count);
                written += count;
            }
        }
        assertThat(written).isEqualTo(expected);
        return all;
    }
}
