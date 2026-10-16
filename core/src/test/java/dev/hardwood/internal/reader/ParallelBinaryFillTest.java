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
/// entry ids, views, bytes and validity at their own positions ([BinaryFill]), while the cell is
/// sized on another thread: a plain segment filled before the sizing parks its bytes, and the
/// sizing writes them into place.
///
/// Each segment comes from a page of its own, of one of the four kinds a binary page decodes to:
/// `PLAIN` and dictionary ids read in place, a decoded page of arrays, and a decoded dictionary
/// page. Each has nulls at random rows unless it is every fifth.
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
    void concurrentFillsWhileTheCellIsSizedWriteEverySegmentInPlace() throws Exception {
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
                // The sizing races the fills: some segments are filled before it and park
                for (int spin = random.nextInt(2_000); spin > 0; spin--) {
                    Thread.onSpinWait();
                }
                List<Segment> parked = BinaryFill.size(prepared.cell, prepared.out);
                if (parked != null) {
                    for (Segment segment : parked) {
                        BinaryFill.unpark(segment, prepared.out);
                    }
                }
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

    /// A plain segment filled before its cell is sized parks with its bytes; the sizing hands it
    /// back, and writing it into place moves its views by the offset the sizing gave it.
    @Test
    void aParkedSegmentIsWrittenWhenItsCellIsSized() {
        List<String> first = List.of("one", "two");
        List<String> second = new ArrayList<>(List.of("three", "four", "five"));
        second.set(1, null);
        Cell cell = new Cell("f.parquet", false, 5, 5, 0, true);
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(BatchExchange.detachingFlat("v", 8), COLUMN).values;
        BatchPlan.RowGroupBatches rowGroup = new BatchPlan(5, false, false, 0).plan(0, 0, "f.parquet", false, 5);
        Segment a = place(cell, rowGroup, 0, first.size());
        Segment b = place(cell, rowGroup, 1, second.size());
        BinaryPageValues aValues = plain(first);
        BinaryPageValues bValues = arrays(second);
        a.bytes = BinaryFill.measure(a, aValues);
        b.bytes = BinaryFill.measure(b, bValues);

        assertThat(BinaryFill.fill(b, bValues, out, 1)).as("filled before its cell is sized").isFalse();
        assertThat(b.parkedBytes).hasSize("threefive".length());
        List<Segment> parked = BinaryFill.size(cell, out);
        assertThat(parked).containsExactly(b);
        BinaryFill.unpark(b, out);
        assertThat(BinaryFill.fill(a, aValues, out, 1)).as("filled once its cell is sized").isTrue();
        FlatFill.backfillValidity(cell);
        BinaryFill.finish(cell, out, new BinaryBatchValues.ViewDemand());

        assertThat(b.byteOffset).isEqualTo("onetwo".length());
        assertThat(out.byteCount()).isEqualTo("onetwothreefive".length());
        List<String> read = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            read.add(i == 3 ? null : out.stringAt(i));
        }
        assertThat(read).containsExactly("one", "two", "three", null, "five");
        assertThat(cell.validity()).containsExactly(0b10111L);
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

    /// One round: a measured cell, its segments, measured and sealed, and what they must read.
    private record Round(Cell cell, BinaryBatchValues out, List<Segment> segments, List<BinaryPageValues> values,
                         List<String> expected, List<Boolean> fromDictionary) {
    }

    private static Round prepareRound(Random random) {
        int total = 0;
        for (int length : LENGTHS) {
            total += length;
        }
        BatchPlan.RowGroupBatches rowGroup = new BatchPlan(total, false, false, 0).plan(0, 0, "f.parquet", false, total);
        Cell cell = new Cell("f.parquet", false, total, total, 0, true);
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(BatchExchange.detachingFlat("v", total), COLUMN).values;
        List<Segment> segments = new ArrayList<>();
        List<BinaryPageValues> values = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        List<Boolean> fromDictionary = new ArrayList<>();
        for (int s = 0; s < LENGTHS.length; s++) {
            int length = LENGTHS[s];
            int kind = random.nextInt(4);
            boolean ids = kind == 1 || kind == 3;
            List<String> page = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                boolean isNull = s % 5 != 4 && random.nextInt(3) == 0;
                page.add(isNull ? null : ids ? ENTRIES[random.nextInt(ENTRIES.length)] : "v" + random.nextInt(100_000));
                fromDictionary.add(ids && !isNull);
            }
            BinaryPageValues pageValues = switch (kind) {
                case 0 -> plain(page);
                case 1 -> ids(page);
                case 2 -> arrays(page);
                default -> rowIds(page);
            };
            Segment segment = place(cell, rowGroup, s, length);
            segment.bytes = BinaryFill.measure(segment, pageValues);
            cell.unsized.decrementAndGet();
            segments.add(segment);
            values.add(pageValues);
            expected.addAll(page);
        }
        return new Round(cell, out, segments, values, expected, fromDictionary);
    }

    private static void assertFilled(Round round, int r) {
        BinaryBatchValues out = round.out;
        long[] validity = round.cell.validity();
        byte[] bytes = out.bytes();
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
            assertThat(new String(bytes, starts[i], ends[i] - starts[i], StandardCharsets.UTF_8))
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
                data.write(0);
                data.write(0);
                data.write(0);
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

    /// The page as a decoded page of arrays.
    private static BinaryPageValues arrays(List<String> page) {
        byte[][] values = new byte[page.size()][];
        for (int i = 0; i < values.length; i++) {
            values[i] = page.get(i) == null ? null : page.get(i).getBytes(StandardCharsets.UTF_8);
        }
        return BinaryPageValues.of(new Page.ByteArrayPage(values, definitionLevels(page), null, 1, values.length),
                new PageDecoder.LevelScratch());
    }

    /// The page as a decoded dictionary page: one id per row, `-1` for a null.
    private static BinaryPageValues rowIds(List<String> page) {
        int[] ids = new int[page.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = page.get(i) == null ? -1 : List.of(ENTRIES).indexOf(page.get(i));
        }
        return BinaryPageValues.of(new Page.DictionaryByteArrayPage(DICTIONARY, ids, definitionLevels(page), null, 1,
                ids.length), new PageDecoder.LevelScratch());
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
