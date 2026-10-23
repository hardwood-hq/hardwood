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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The rules by which a [FetchSequence] starts and charges a read's footer entries
/// (READ_PATH_CONSOLIDATION.md, rules 4 and 10).
class FetchSequenceFooterTest {

    private static final int TAIL = 1_000;

    private String tailBytes;

    @BeforeEach
    void setTail() {
        tailBytes = System.getProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, Integer.toString(TAIL));
    }

    @AfterEach
    void restoreTail() {
        if (tailBytes == null) {
            System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        }
        else {
            System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
        }
    }

    /// Rule 4: the footers of the files after the one reached start, up to the files ahead; the
    /// next file's is not charged, each one past it is charged the tail its read takes.
    @Test
    void footersAheadStartWithTheOnesPastTheNextFileCharged() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FetchSequence sequence = sequence(buffer, footers, 3, 10);

        sequence.pump();

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isEqualTo(2L * TAIL);

        sequence.reachedFile(2);
        assertThat(footers.started).containsExactly(1, 2, 3, 4, 5);
        assertThat(buffer.held()).as("files 2 to 5 charged; 2 and 3 started past the next file")
                .isEqualTo(4L * TAIL);
    }

    /// Rule 4: a footer past the next file starts only while the planning chain has room to plan
    /// its file: once the entries planned and not started reach F, only the next file's starts.
    @Test
    void noFooterPastTheNextFileStartsWhilePlanningHasNoRoom() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FetchSequence sequence = new FetchSequence(new PrefetchTasks(Runnable::run), buffer, 100, true, () -> {},
                footers, 3, 10);
        CountingInputFile file = new CountingInputFile(ByteBuffer.allocate(1_000));
        // Planned, larger than F, so it does not start speculatively and fills the chain's room
        sequence.insert(5, file, List.of(new FetchSequence.Span(0, 200, "rg=0 col=a", 1)));

        assertThat(footers.started).containsExactly(1);

        sequence.forget(5);

        assertThat(footers.started).containsExactly(1, 2, 3);
    }

    /// Rule 4: a footer R has no room for waits for no one, and starts on a later release.
    @Test
    void aFooterThatDoesNotFitStartsOnALaterRelease() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(2 * TAIL);
        // Another read holds half of R
        assertThat(buffer.tryCharge(TAIL)).isTrue();
        Footers footers = new Footers();
        FetchSequence sequence = sequence(buffer, footers, 3, 10);

        sequence.pump();
        assertThat(footers.started).as("no room for file 3").containsExactly(1, 2);

        buffer.release(TAIL);

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isEqualTo(2L * TAIL);
    }

    /// Rule 4: under a row limit only the next file's footer starts, uncharged.
    @Test
    void aReadWithARowLimitStartsOnlyTheNextFilesFooter() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FetchSequence sequence = sequence(buffer, footers, 1, 10);

        sequence.pump();
        sequence.reachedFile(1);

        assertThat(footers.started).containsExactly(1, 2);
        assertThat(buffer.held()).isZero();
    }

    /// Rule 4: a file the read is done with drops its tail and releases its footer's charge.
    @Test
    void aFileTheReadIsDoneWithDropsItsTailAndItsCharge() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FetchSequence sequence = sequence(buffer, footers, 3, 4);
        sequence.pump();
        footers.completeAll();

        sequence.reachedFile(3);
        sequence.filesDoneBefore(3);
        assertThat(footers.dropped).containsExactly(0, 1, 2);
        assertThat(buffer.held()).as("file 3's charge").isEqualTo(TAIL);

        sequence.filesDoneBefore(4);
        assertThat(footers.dropped).containsExactly(0, 1, 2, 3);
        assertThat(buffer.held()).isZero();
    }

    /// Rule 4: a footer whose entry another read started, or that is read in place, holds no charge.
    @Test
    void aFooterNotStartedByTheReadHoldsNoCharge() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        footers.alreadyRead.add(2);
        FetchSequence sequence = sequence(buffer, footers, 3, 10);

        sequence.pump();

        assertThat(buffer.held()).as("file 3's charge alone").isEqualTo(TAIL);
    }

    /// Rule 10: a footer in flight when the read closes keeps its charge until it completes, since
    /// the reader's cache shares its entry; one that has completed releases its charge at once.
    @Test
    void aFooterInFlightAtCloseKeepsItsChargeUntilItCompletes() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FetchSequence sequence = sequence(buffer, footers, 3, 10);
        sequence.pump();
        footers.complete(2);

        sequence.close();
        assertThat(buffer.held()).as("file 3's footer still in flight").isEqualTo(TAIL);

        footers.complete(3);
        assertThat(buffer.held()).isZero();
        assertThat(footers.dropped).as("file 3's tail dropped once its footer is read").endsWith(3);
    }

    /// A read that plans no further file starts no further footer, and releases the charges of
    /// those read ahead, keeping those in flight until they complete.
    @Test
    void aStoppedReadStartsNoFurtherFooter() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FetchSequence sequence = sequence(buffer, footers, 3, 10);
        sequence.pump();
        footers.complete(2);

        sequence.stopFooters();
        sequence.reachedFile(2);

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isEqualTo(TAIL);
        footers.complete(3);
        assertThat(buffer.held()).isZero();
    }

    private static FetchSequence sequence(ReadAheadBuffer buffer, Footers footers, int filesAhead, int fileCount) {
        return new FetchSequence(new PrefetchTasks(Runnable::run), buffer, 1 << 20, true, () -> {}, footers,
                filesAhead, fileCount);
    }

    /// Footer entries whose reads complete when a test says so.
    private static final class Footers implements FetchSequence.Footers {

        final List<Integer> started = new ArrayList<>();
        final List<Integer> dropped = new ArrayList<>();
        final List<Integer> alreadyRead = new ArrayList<>();
        private final Map<Integer, CompletableFuture<Void>> reads = new HashMap<>();

        @Override
        public CompletableFuture<?> start(int fileIndex) {
            started.add(fileIndex);
            if (alreadyRead.contains(fileIndex)) {
                return null;
            }
            CompletableFuture<Void> read = new CompletableFuture<>();
            reads.put(fileIndex, read);
            return read;
        }

        @Override
        public void drop(int fileIndex) {
            dropped.add(fileIndex);
        }

        void complete(int fileIndex) {
            reads.get(fileIndex).complete(null);
        }

        void completeAll() {
            reads.values().forEach(read -> read.complete(null));
        }
    }
}
