/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.io.CountingInputFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/// How a walk takes bytes from the requests that hold its chunk (FETCH_PLANNING.md, Fetch plans).
class SectionCursorTest {

    /// A range the walk's requests hold is sliced from them, and only the part no request holds is
    /// inserted, up to the next request; a range straddling a cut is copied from both.
    @Test
    void aHeldRangeIsSlicedAndOnlyTheGapIsInserted() throws Exception {
        ByteBuffer bytes = ByteBuffer.allocate(300);
        for (int i = 0; i < 300; i++) {
            bytes.put(i, (byte) i);
        }
        CountingInputFile file = new CountingInputFile(bytes);
        file.open();
        InPlaceSections sections = new InPlaceSections();
        List<Section> held = new ArrayList<>(sections.insert(0, 0, file, List.of(
                new Span(0, 100, "rg=0 col=a", 1),
                new Span(150, 50, "rg=0 col=a col=b", 2))));
        List<long[]> inserted = new ArrayList<>();
        SectionCursor cursor = new SectionCursor(held, (start, limit) -> {
            inserted.add(new long[] { start, limit });
            return sections.insertDemanded(0, file, start, Math.toIntExact(limit - start), "rg=0 col=a");
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
        assertThat(file.readCount()).as("the held section sliced again").isEqualTo(3);
    }

    /// A walk passes the requests that end before where it reads next, once each.
    @Test
    void aWalkPassesTheRequestsBehindIt() {
        ReleaseCounting first = new ReleaseCounting(0, 100);
        ReleaseCounting second = new ReleaseCounting(100, 200);
        SectionCursor cursor = new SectionCursor(new ArrayList<>(List.of(first, second)), null);

        cursor.releaseBefore(150);
        assertThat(first.releases).as("the first section passed").isEqualTo(1);
        assertThat(second.releases).isZero();
        cursor.releaseBefore(150);
        assertThat(first.releases).as("passed once").isEqualTo(1);
        cursor.releaseAll();
        assertThat(first.releases).isEqualTo(1);
        assertThat(second.releases).isEqualTo(1);
    }

    /// The bytes `cursor` reads at `start`, demanding the sections it stops on.
    private static ByteBuffer read(SectionCursor cursor, long start, int length) throws Exception {
        while (true) {
            ByteBuffer bytes = cursor.read(start, length);
            if (bytes != null) {
                return bytes;
            }
            cursor.pending().demand();
        }
    }

    /// A section of `[offset, end)` holding no bytes, counting its releases.
    private static final class ReleaseCounting implements Section {

        private final long offset;
        private final long end;
        private int releases;

        ReleaseCounting(long offset, long end) {
            this.offset = offset;
            this.end = end;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long end() {
            return end;
        }

        @Override
        public ByteBuffer poll() throws IOException {
            return null;
        }

        @Override
        public ByteBuffer slice(ByteBuffer polled, long fileOffset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release() {
            releases++;
        }

        @Override
        public void demand() {
        }

        @Override
        public void discard() {
        }

        @Override
        public void whenMet(Runnable resume) {
            throw new UnsupportedOperationException();
        }
    }
}
