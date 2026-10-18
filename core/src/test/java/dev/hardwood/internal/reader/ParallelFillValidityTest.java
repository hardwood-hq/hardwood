/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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

/// The segments of one flat cell filled at the same time, each by its own thread, merge their
/// validity bits into one bitmap: segments whose edges fall inside a word share it, and each
/// writes the words at its edges with an atomic OR ([FlatFill]).
///
/// The segments are short, so most words are shared by several of them, and no segment edge but
/// the cell's start falls on a word boundary. A segment with nulls has its first null at a random
/// value, so the present values before it are written into the segment's first word, which a
/// neighbour may be writing too, and a null at its last value, so it writes both its edge words;
/// every fourth segment has no null and writes nothing, and the cell's completion sets its bits. Neighbouring segments are filled by different
/// threads, which spin on a shared round counter so that they start their fills together.
class ParallelFillValidityTest {

    private static final int[] LENGTHS = { 3, 7, 1, 5, 11, 2, 9, 13, 70, 4, 6, 1, 1, 8, 3, 129, 5, 2, 10, 7, 1, 3 };

    private static final int THREADS = Math.max(2, Math.min(7, Runtime.getRuntime().availableProcessors() - 1));

    private static final int ROUNDS = 2_000;

    private static final ColumnSchema COLUMN = FileSchema.builder("validity")
            .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
            .build()
            .getColumn(0);

    private static final BatchExchange<BatchExchange.Batch> EXCHANGE = BatchExchange.detachingFlat("v", 1024);

    /// The decoded page of each segment's page task in the current round.
    private static final AtomicReference<Map<PageTask, FlatPageValues>> PAGES = new AtomicReference<>();

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void concurrentFillsOfAdjacentSegmentsMergeTheirValidity() throws Exception {
        AtomicInteger round = new AtomicInteger(-1);
        AtomicInteger finished = new AtomicInteger();
        AtomicReference<List<Segment>> current = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> fillers = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            int thread = t;
            fillers.add(Thread.ofPlatform().daemon().start(() -> fillRounds(thread, round, finished, current, failure)));
        }
        try {
            Random random = new Random(1287);
            for (int r = 0; r < ROUNDS; r++) {
                long[] expected = prepareRound(random, current);
                finished.set(0);
                round.set(r);
                while (finished.get() < THREADS) {
                    Thread.onSpinWait();
                }
                assertThat(failure.get()).isNull();
                Cell cell = current.get().getFirst().cell;
                FlatFill.backfillValidity(cell);

                assertThat(cell.validity()).as("round %d", r).containsExactly(expected);
            }
        }
        finally {
            round.set(ROUNDS);
            for (Thread filler : fillers) {
                filler.join();
            }
        }
    }

    /// Fills every segment `s` with `s % THREADS == thread`, once per round.
    private static void fillRounds(int thread, AtomicInteger round, AtomicInteger finished,
                                   AtomicReference<List<Segment>> current,
                                   AtomicReference<Throwable> failure) {
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
                List<Segment> segments = current.get();
                for (int s = thread; s < segments.size(); s += THREADS) {
                    Segment segment = segments.get(s);
                    FlatFill.fill(segment, PAGES.get().get(segment.page), segment.cell.allocate(EXCHANGE, COLUMN).values, 1);
                }
            }
            catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
            next = r + 1;
            finished.incrementAndGet();
        }
    }

    /// Builds a cell and its segments for one round, and returns the bitmap they must produce.
    private static long[] prepareRound(Random random, AtomicReference<List<Segment>> current) {
        int total = 0;
        for (int length : LENGTHS) {
            total += length;
        }
        BatchPlan.RowGroupBatches rowGroup = new BatchPlan(total, false, false, 0).plan(0, 0, "f.parquet", false, total);
        Cell cell = new Cell("f.parquet", false, total, total, 0, false);
        cell.allocate(EXCHANGE, COLUMN);
        Map<PageTask, FlatPageValues> pages = new IdentityHashMap<>();

        long[] expected = new long[(total + 63) >>> 6];
        List<Segment> segments = new ArrayList<>();
        int offset = 0;
        for (int s = 0; s < LENGTHS.length; s++) {
            int length = LENGTHS[s];
            int[] defLevels = new int[length];
            boolean withNulls = s % 4 != 3;
            int firstNull = random.nextInt(length);
            for (int i = 0; i < length; i++) {
                boolean isNull = withNulls
                        && (i == firstNull || i == length - 1 || (i > firstNull && random.nextInt(4) == 0));
                defLevels[i] = isNull ? 0 : 1;
                if (!isNull) {
                    expected[(offset + i) >>> 6] |= 1L << (offset + i);
                }
            }
            PageTask task = new PageTask(new PageInfo(null, null, null, null, PageRowMask.ALL, length),
                    "f.parquet", 0, s, rowGroup, null, offset);
            int present = FlatPageValues.countNonNull(defLevels, 1, length);
            pages.put(task, FlatPageValues.plain(PhysicalType.INT32, defLevels, 1, length, new byte[present * 4], 0,
                    present * 4));
            segments.add(cell.add(task, 0, length));
            offset += length;
        }
        PAGES.set(pages);
        current.set(segments);
        return expected;
    }
}
