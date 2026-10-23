/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/// How a walk takes bytes from the entries that hold its chunk (READ_PATH_CONSOLIDATION.md, rules 5
/// and 6).
class EntryCursorTest {

    /// Rule 5: a range the walk's entries hold is sliced from them, and only the part no entry holds
    /// is inserted, up to the next entry; a range straddling a cut is copied from both.
    @Test
    void aHeldRangeIsSlicedAndOnlyTheGapIsInserted() throws Exception {
        ByteBuffer bytes = ByteBuffer.allocate(300);
        for (int i = 0; i < 300; i++) {
            bytes.put(i, (byte) i);
        }
        CountingInputFile file = new CountingInputFile(bytes);
        file.open();
        FetchSequence sequence = new FetchSequence(new PrefetchTasks(Runnable::run), new ReadAheadBuffer(0), 0, false,
                () -> {});
        List<FetchSequence.Entry> held = sequence.insert(0, file, List.of(
                new FetchSequence.Span(0, 100, "rg=0 col=a", 1),
                new FetchSequence.Span(150, 50, "rg=0 col=a col=b", 2)));
        List<long[]> inserted = new ArrayList<>();
        EntryCursor cursor = new EntryCursor(held, (start, limit) -> {
            inserted.add(new long[] { start, limit });
            return sequence.insertDemanded(0, file, start, Math.toIntExact(limit - start), "rg=0 col=a");
        });

        ByteBuffer read = read(cursor, 90, 80);

        assertThat(inserted).singleElement().satisfies(gap -> assertThat(gap).containsExactly(100, 150));
        assertThat(file.reads()).extracting(CountingInputFile.Read::offset, CountingInputFile.Read::length)
                .containsExactlyInAnyOrder(tuple(0L, 100),
                        tuple(150L, 50), tuple(100L, 50));
        assertThat(read.remaining()).isEqualTo(80);
        for (int i = 0; i < 80; i++) {
            assertThat(read.get(i)).isEqualTo((byte) (90 + i));
        }
        read(cursor, 160, 20);
        assertThat(file.readCount()).as("the held entry sliced again").isEqualTo(3);
    }

    /// Rule 6: a walk passes the entries that end before where it reads next, once each.
    @Test
    void aWalkPassesTheEntriesBehindIt() throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.allocate(300));
        file.open();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        FetchSequence sequence = new FetchSequence(new PrefetchTasks(Runnable::run), buffer, 1 << 20, true, () -> {});
        List<FetchSequence.Entry> held = sequence.insert(0, file, List.of(
                new FetchSequence.Span(0, 100, "rg=0 col=a", 1),
                new FetchSequence.Span(100, 100, "rg=0 col=a", 1)));
        EntryCursor cursor = new EntryCursor(held, null);
        assertThat(buffer.held()).isEqualTo(200);

        cursor.releaseBefore(150);
        assertThat(buffer.held()).as("the first entry passed").isEqualTo(100);
        cursor.releaseBefore(150);
        assertThat(buffer.held()).as("passed once").isEqualTo(100);
        cursor.releaseAll();
        assertThat(buffer.held()).isZero();
    }

    /// The bytes `cursor` reads at `start`, demanding the entries it stops on.
    private static ByteBuffer read(EntryCursor cursor, long start, int length) throws Exception {
        while (true) {
            ByteBuffer bytes = cursor.read(start, length);
            if (bytes != null) {
                return bytes;
            }
            cursor.pending().issue().await();
        }
    }
}
