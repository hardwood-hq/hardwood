/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The segments of one binary cell filled at the same time, each by its own thread, write their
/// entry ids, views and validity at their own positions, and their plain bytes into chunks of
/// their own ([BinaryFill]).
///
/// Each segment comes from a page of its own, of one of the two kinds a binary page is read as:
/// `PLAIN` values and dictionary ids, both in place. Each has nulls at random rows unless it is
/// every fifth.
class ParallelBinaryFillTest {

    private static final int[] LENGTHS = { 3, 7, 1, 5, 11, 2, 9, 13, 70, 4, 6, 1, 1, 8, 3, 129, 5, 2, 10, 7, 1, 3 };

    private static final int THREADS = Math.max(2, Math.min(7, Runtime.getRuntime().availableProcessors() - 1));

    private static final int ROUNDS = 1_000;

    private static final ColumnSchema COLUMN = FileSchema.builder("binary")
            .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
            .build()
            .getColumn(0);

    private static final String[] ENTRIES = { "alpha", "", "charlie", "delta-delta", "e" };

    private static final Dictionary.ByteArrayDictionary DICTIONARY = dictionary();

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void concurrentFillsWriteEverySegmentInPlace() throws Exception {
        AtomicInteger round = new AtomicInteger(-1);
        AtomicInteger finished = new AtomicInteger();
        AtomicReference<Round> current = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> fillers = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            int thread = t;
            fillers.add(Thread.ofPlatform().daemon().start(() -> fillRounds(thread, round, finished, current, failure)));
        }
        try {
            Random random = new Random(1287);
            for (int r = 0; r < ROUNDS; r++) {
                Round prepared = prepareRound(random);
                current.set(prepared);
                finished.set(0);
                round.set(r);
                while (finished.get() < THREADS) {
                    Thread.onSpinWait();
                }
                assertThat(failure.get()).isNull();
                FlatFill.backfillValidity(prepared.cell);
                BinaryFill.finish(prepared.cell, prepared.out, new BinaryBatchValues.ViewDemand());

                assertFilled(prepared, r);
            }
        }
        finally {
            round.set(ROUNDS);
            for (Thread filler : fillers) {
                filler.join();
            }
        }
    }

    /// A segment's plain bytes go into chunks of at most [BinaryBatchValues#CHUNK_BYTES], a value
    /// never spanning two; a value longer than a chunk gets a buffer of its own length.
    @Test
    void plainBytesFillChunksNoLargerThanAChunk() {
        int chunk = BinaryBatchValues.CHUNK_BYTES;
        List<String> page = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            page.add(String.valueOf((char) ('a' + i)).repeat(100_000));
        }
        page.add(null);
        page.add("z".repeat(chunk + 1));
        page.add("tail");
        Cell cell = new Cell("f.parquet", false, page.size(), page.size(), 0);
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(BatchExchange.detachingFlat("v", page.size()), COLUMN)
                .values;
        BatchPlan.RowGroupBatches rowGroup = new BatchPlan(page.size(), false, false, 0)
                .plan(0, 0, "f.parquet", false, page.size());
        Segment segment = place(cell, rowGroup, 0, page.size());
        BinaryFill.fill(segment, plain(page), out, 1);
        FlatFill.backfillValidity(cell);
        BinaryFill.finish(cell, out, new BinaryBatchValues.ViewDemand());

        byte[][] buffers = out.buffers();
        int[] bufferIds = out.bufferIds();
        int[] starts = out.starts();
        int[] ends = out.ends();
        for (int i = 0; i < page.size(); i++) {
            byte[] buffer = buffers[bufferIds[i]];
            assertThat(ends[i]).as("value %d within its buffer", i).isBetween(starts[i], buffer.length);
            String expected = page.get(i);
            assertThat(new String(buffer, starts[i], ends[i] - starts[i], StandardCharsets.UTF_8))
                    .as("value %d", i).isEqualTo(expected == null ? "" : expected);
        }
        // Two 100,000-byte values per chunk: four chunks for seven, then the long value's own
        assertThat(out.bufferCount()).isEqualTo(1 + 4 + 1 + 1);
        for (int b = 1; b < out.bufferCount(); b++) {
            byte[] buffer = buffers[b];
            assertThat(buffer.length).as("buffer %d", b).isLessThanOrEqualTo(buffer == buffers[bufferIds[8]] ? chunk + 1 : chunk);
        }
        assertThat(buffers[bufferIds[8]]).hasSize(chunk + 1);
    }

    private static void fillRounds(int thread, AtomicInteger round, AtomicInteger finished,
                                   AtomicReference<Round> current, AtomicReference<Throwable> failure) {
        int next = 0;
        while (next < ROUNDS) {
            int r = round.get();
            if (r < next) {
                Thread.onSpinWait();
                continue;
            }
            if (r >= ROUNDS) {
                return;
            }
            try {
                Round prepared = current.get();
                for (int s = thread; s < prepared.segments.size(); s += THREADS) {
                    BinaryFill.fill(prepared.segments.get(s), prepared.values.get(s), prepared.out, 1);
                }
            }
            catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
            next = r + 1;
            finished.incrementAndGet();
        }
    }

    /// One round: a cell, its segments, sealed, and what they must read.
    private record Round(Cell cell, BinaryBatchValues out, List<Segment> segments, List<BinaryPageValues> values,
                         List<String> expected, List<Boolean> fromDictionary) {
    }

    private static Round prepareRound(Random random) {
        int total = 0;
        for (int length : LENGTHS) {
            total += length;
        }
        BatchPlan.RowGroupBatches rowGroup = new BatchPlan(total, false, false, 0).plan(0, 0, "f.parquet", false, total);
        Cell cell = new Cell("f.parquet", false, total, total, 0);
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(BatchExchange.detachingFlat("v", total), COLUMN).values;
        List<Segment> segments = new ArrayList<>();
        List<BinaryPageValues> values = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        List<Boolean> fromDictionary = new ArrayList<>();
        for (int s = 0; s < LENGTHS.length; s++) {
            int length = LENGTHS[s];
            boolean ids = random.nextBoolean();
            List<String> page = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                boolean isNull = s % 5 != 4 && random.nextInt(3) == 0;
                page.add(isNull ? null : ids ? ENTRIES[random.nextInt(ENTRIES.length)] : "v" + random.nextInt(100_000));
                fromDictionary.add(ids && !isNull);
            }
            BinaryPageValues pageValues = ids ? ids(page) : plain(page);
            Segment segment = place(cell, rowGroup, s, length);
            segments.add(segment);
            values.add(pageValues);
            expected.addAll(page);
        }
        return new Round(cell, out, segments, values, expected, fromDictionary);
    }

    private static void assertFilled(Round round, int r) {
        BinaryBatchValues out = round.out;
        long[] validity = round.cell.validity();
        int[] starts = out.starts();
        int[] ends = out.ends();
        for (int i = 0; i < round.expected.size(); i++) {
            String expected = round.expected.get(i);
            boolean present = validity == null || (validity[i >>> 6] & 1L << i) != 0;
            assertThat(present).as("round %d, value %d present", r, i).isEqualTo(expected != null);
            assertThat(out.dictionary != null && out.dictIndices[i] >= 0).as("round %d, value %d has an id", r, i)
                    .isEqualTo(round.fromDictionary.get(i));
            if (expected == null) {
                assertThat(ends[i] - starts[i]).as("round %d, null %d has an empty view", r, i).isZero();
                continue;
            }
            assertThat(out.stringAt(i)).as("round %d, value %d", r, i).isEqualTo(expected);
            assertThat(new String(out.buffer(i), starts[i], ends[i] - starts[i], StandardCharsets.UTF_8))
                    .as("round %d, view %d", r, i).isEqualTo(expected);
        }
    }

    /// Places one page of `length` values, page ordinal `index`, into `cell`.
    private static Segment place(Cell cell, BatchPlan.RowGroupBatches rowGroup, int index, int length) {
        PageTask task = new PageTask(new PageInfo(null, null, null, null, PageRowMask.ALL, length),
                "f.parquet", 0, index, rowGroup, null, cell.records());
        Segment segment = cell.add(task, 0, length);
        task.segments.add(segment);
        return segment;
    }

    private static int[] definitionLevels(List<String> page) {
        int[] levels = new int[page.size()];
        for (int i = 0; i < levels.length; i++) {
            levels[i] = page.get(i) == null ? 0 : 1;
        }
        return levels;
    }

    /// The page as an in-place `PLAIN` page: length-prefixed values of its non-null rows.
    private static BinaryPageValues plain(List<String> page) {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        for (String value : page) {
            if (value != null) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                data.write(bytes.length);
                data.write(bytes.length >>> 8);
                data.write(bytes.length >>> 16);
                data.write(bytes.length >>> 24);
                data.writeBytes(bytes);
            }
        }
        byte[] bytes = data.toByteArray();
        return BinaryPageValues.plain(PhysicalType.BYTE_ARRAY, 0, definitionLevels(page), 1, page.size(), bytes, 0,
                bytes.length, new PageDecoder.LevelScratch());
    }

    /// The page as an in-place dictionary page: one id per non-null row.
    private static BinaryPageValues ids(List<String> page) {
        int[] ids = new int[page.size()];
        int count = 0;
        for (String value : page) {
            if (value != null) {
                ids[count++] = List.of(ENTRIES).indexOf(value);
            }
        }
        return BinaryPageValues.ids(definitionLevels(page), 1, page.size(), ids, count, DICTIONARY);
    }

    private static Dictionary.ByteArrayDictionary dictionary() {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        for (String entry : ENTRIES) {
            byte[] bytes = entry.getBytes(StandardCharsets.UTF_8);
            data.write(bytes.length);
            data.write(0);
            data.write(0);
            data.write(0);
            data.writeBytes(bytes);
        }
        byte[] bytes = data.toByteArray();
        return Dictionary.ByteArrayDictionary.parseLengthPrefixed(bytes, bytes.length, ENTRIES.length);
    }
}
