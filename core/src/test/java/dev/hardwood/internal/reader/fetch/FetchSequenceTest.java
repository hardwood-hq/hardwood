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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The rules by which a [FetchSequence] starts, releases and drops its entries
/// (FETCH_PLANNING.md, Entries).
class FetchSequenceTest {

    private static final int LENGTH = 100;

    /// A demanded entry starts at once, whatever F and R hold, and is charged to neither.
    @Test
    void aDemandedEntryStartsWhateverFAndRHold() throws Exception {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(0);
        FetchSequence sequence = sequence(buffer, 0, true);
        FetchSequence.Entry entry = insert(sequence, file, 0, 1).getFirst();
        assertThat(file.readCount()).as("speculative start with no room").isZero();

        assertThat(fetch(entry).remaining()).isEqualTo(LENGTH);
        assertThat(sequence.speculativeBytes()).isZero();
        assertThat(buffer.held()).isZero();
    }

    /// The next unit's first entry starts without F and is not charged to R; the entries
    /// after it wait for room.
    @Test
    void theNextUnitsFirstEntryStartsUncharged() {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(0);
        FetchSequence sequence = sequence(buffer, 0, true);
        sequence.nextUnit(1);
        insert(sequence, file, 1, 2);

        assertThat(file.reads()).extracting(CountingInputFile.Read::offset).containsExactly(0L);
        assertThat(sequence.speculativeBytes()).isZero();
        assertThat(buffer.held()).isZero();
    }

    /// Speculative entries start in position order while their bytes stay within F, and
    /// each one passed starts the next.
    @Test
    void speculativeEntriesStartInPositionOrderWithinF() {
        CountingInputFile file = file();
        FetchSequence sequence = sequence(new ReadAheadBuffer(1 << 20), 2 * LENGTH, true);
        List<FetchSequence.Entry> later = insert(sequence, file, 1, 2);
        List<FetchSequence.Entry> earlier = insert(sequence, file, 0, 2);

        // Work item 1's entries were inserted first, and took F before work item 0's arrived
        assertThat(file.reads()).extracting(CountingInputFile.Read::offset).containsExactly(0L, 100L);
        later.getFirst().release();
        assertThat(file.reads()).extracting(CountingInputFile.Read::offset).containsExactly(0L, 100L, 0L);
        assertThat(earlier.getFirst().workItemIndex()).isZero();
        assertThat(sequence.speculativeBytes()).isEqualTo(2 * LENGTH);
    }

    /// An entry R has no room for waits for no one; a release of R wakes its waiters in
    /// registration order, a decode task registered before the entry first.
    @Test
    void aReleaseOfRWakesItsWaitersInRegistrationOrder() {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(LENGTH);
        assertThat(buffer.tryCharge(LENGTH)).isTrue();
        List<String> woken = new ArrayList<>();
        buffer.whenReleased(buffer.releases(), () -> woken.add("decode task"));
        FetchSequence sequence = new FetchSequence(new PrefetchTasks(Runnable::run), buffer, 1 << 20, true,
                () -> woken.add("entry"));
        insert(sequence, file, 0, 1);
        buffer.whenReleased(buffer.releases(), () -> woken.add("later task"));
        assertThat(file.readCount()).isZero();

        buffer.release(LENGTH);

        assertThat(woken).containsExactly("decode task", "entry", "later task");
        assertThat(file.readCount()).as("the entry started on the release").isEqualTo(1);
        assertThat(buffer.held()).isEqualTo(LENGTH);
    }

    /// A read with a row limit starts no speculative entry.
    @Test
    void aReadThatDoesNotSpeculateStartsOnlyOnDemand() {
        CountingInputFile file = file();
        FetchSequence sequence = sequence(new ReadAheadBuffer(1 << 20), 1 << 20, false);
        insert(sequence, file, 0, 3);

        assertThat(file.readCount()).isZero();
    }

    /// An entry is released once every column holding it has passed it, which returns its
    /// charges and drops its bytes; a later demand for it is a fault.
    @Test
    void anEntryIsReleasedOnceEveryHolderHasPassedIt() {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        FetchSequence sequence = sequence(buffer, 1 << 20, true);
        FetchSequence.Entry entry = sequence.insert(0, file,
                List.of(new FetchSequence.Span(0, LENGTH, "rg=0 col=a col=b", 2))).getFirst();
        assertThat(buffer.held()).isEqualTo(LENGTH);

        entry.release();
        assertThat(buffer.held()).as("held by the second column").isEqualTo(LENGTH);
        entry.release();
        assertThat(buffer.held()).isZero();
        assertThat(sequence.speculativeBytes()).isZero();
        assertThatThrownBy(entry::issue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Entry at offset 0 (length 100) of <memory> was released before it was read");
    }

    /// A speculative entry that fails is dropped, its charges returned, and started again
    /// when demanded.
    @Test
    void aFailedSpeculativeEntryIsDroppedAndReadAgainOnDemand() throws Exception {
        FailingOnce file = new FailingOnce();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        FetchSequence sequence = sequence(buffer, 1 << 20, true);
        FetchSequence.Entry entry = insert(sequence, file, 0, 1).getFirst();

        assertThat(entry.poll()).isNull();
        assertThat(buffer.held()).isZero();
        assertThat(sequence.speculativeBytes()).isZero();
        assertThat(fetch(entry).remaining()).isEqualTo(LENGTH);
        assertThat(file.reads).isEqualTo(2);
    }

    /// Close drops the entries not started, waits for those in flight without
    /// interrupting them, and releases every charge.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeWaitsForTheEntriesInFlightAndReleasesEveryCharge() throws Exception {
        HeldFile file = new HeldFile();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        PrefetchTasks tasks = new PrefetchTasks(task -> Thread.ofVirtual().start(task));
        FetchSequence sequence = new FetchSequence(tasks, buffer, LENGTH, true, () -> {});
        insert(sequence, file, 0, 3);
        assertThat(file.entered.await(10, TimeUnit.SECONDS)).isTrue();

        sequence.close();
        assertThat(buffer.held()).isZero();
        Thread closer = Thread.ofPlatform().start(tasks::awaitAll);
        closer.join(300);
        assertThat(closer.isAlive()).as("close waiting for the read in flight").isTrue();

        file.release.countDown();
        closer.join(10_000);
        assertThat(closer.isAlive()).isFalse();
        assertThat(file.reads).as("the entries not started").isEqualTo(1);
        assertThat(file.interrupted).isFalse();
    }

    private static CountingInputFile file() {
        CountingInputFile file = new CountingInputFile(ByteBuffer.allocate(10 * LENGTH));
        try {
            file.open();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return file;
    }

    /// A sequence whose reads run on the calling thread.
    private static FetchSequence sequence(ReadAheadBuffer buffer, long fetchAheadBytes, boolean speculates) {
        return new FetchSequence(new PrefetchTasks(Runnable::run), buffer, fetchAheadBytes, speculates, () -> {});
    }

    /// Inserts `count` entries of [#LENGTH] bytes, back to back from offset 0, for work item
    /// `workItemIndex`, each held by one column.
    private static List<FetchSequence.Entry> insert(FetchSequence sequence, InputFile file, int workItemIndex,
                                                    int count) {
        List<FetchSequence.Span> spans = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            spans.add(new FetchSequence.Span((long) i * LENGTH, LENGTH, "rg=" + workItemIndex + " col=a", 1));
        }
        return sequence.insert(workItemIndex, file, spans);
    }

    /// `entry`'s bytes, demanding them and waiting for them as a thread off the pool does.
    private static ByteBuffer fetch(FetchSequence.Entry entry) throws IOException {
        while (true) {
            ByteBuffer bytes = entry.poll();
            if (bytes != null) {
                return bytes;
            }
            entry.issue().await();
        }
    }

    /// Fails its first read.
    private static final class FailingOnce extends RemoteFile {

        private int reads;

        @Override
        public synchronized ByteBuffer readRange(long offset, int length) throws IOException {
            if (++reads == 1) {
                throw new IOException("connection reset");
            }
            return ByteBuffer.allocate(length);
        }
    }

    /// Holds its first read until released; every later read passes.
    private static final class HeldFile extends RemoteFile {

        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile int reads;
        volatile boolean interrupted;

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            reads++;
            entered.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                interrupted = true;
                Thread.currentThread().interrupt();
            }
            return ByteBuffer.allocate(length);
        }
    }

    /// A file the reader does not hold in memory.
    private abstract static class RemoteFile implements InputFile {

        @Override
        public void open() {
        }

        @Override
        public long length() {
            return 10 * LENGTH;
        }

        @Override
        public String name() {
            return "remote";
        }

        @Override
        public void close() {
        }
    }
}
