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

/// The rules by which a [FetchSequence] starts, releases and drops its section requests
/// (FETCH_PLANNING.md, Section requests).
class FetchSequenceTest {

    private static final int LENGTH = 100;

    /// A demanded section request starts at once, whatever F and R hold, and is charged to neither.
    @Test
    void aDemandedSectionRequestStartsWhateverFAndRHold() throws Exception {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(0);
        FetchSequence sequence = sequence(buffer, 0);
        SectionRequest section = insert(sequence, file, 0, 1).getFirst();
        assertThat(file.readCount()).as("speculative start with no room").isZero();

        assertThat(fetch(section).remaining()).isEqualTo(LENGTH);
        assertThat(sequence.speculativeBytes()).isZero();
        assertThat(buffer.held()).isZero();
    }

    /// The next unit's first section request starts without F and is not charged to R; the section
    /// requests after it wait for room.
    @Test
    void theNextUnitsFirstSectionRequestStartsUncharged() {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(0);
        FetchSequence sequence = sequence(buffer, 0);
        sequence.nextUnit(1);
        insert(sequence, file, 1, 2);

        assertThat(file.reads()).extracting(CountingInputFile.Read::offset).containsExactly(0L);
        assertThat(sequence.speculativeBytes()).isZero();
        assertThat(buffer.held()).isZero();
    }

    /// Speculative section requests start in position order while their bytes stay within F, and
    /// each one passed starts the next.
    @Test
    void speculativeSectionRequestsStartInPositionOrderWithinF() {
        CountingInputFile file = file();
        FetchSequence sequence = sequence(new ReadAheadBuffer(1 << 20), 2 * LENGTH);
        List<SectionRequest> later = insert(sequence, file, 1, 2);
        List<SectionRequest> earlier = insert(sequence, file, 0, 2);

        // Work item 1's section requests were inserted first, and took F before work item 0's
        // arrived
        assertThat(file.reads()).extracting(CountingInputFile.Read::offset).containsExactly(0L, 100L);
        later.getFirst().release();
        assertThat(file.reads()).extracting(CountingInputFile.Read::offset).containsExactly(0L, 100L, 0L);
        assertThat(earlier.getFirst().workItemIndex()).isZero();
        assertThat(sequence.speculativeBytes()).isEqualTo(2 * LENGTH);
    }

    /// A section request R has no room for waits for no one; a release of R wakes its waiters in
    /// registration order, a decode task registered before the section request first.
    @Test
    void aReleaseOfRWakesItsWaitersInRegistrationOrder() {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(LENGTH);
        assertThat(buffer.tryCharge(LENGTH)).isTrue();
        List<String> woken = new ArrayList<>();
        buffer.whenReleased(buffer.releases(), () -> woken.add("decode task"));
        FetchSequence sequence = new FetchSequence(new PrefetchTasks(Runnable::run), buffer, 1 << 20,
                () -> woken.add("section"));
        insert(sequence, file, 0, 1);
        buffer.whenReleased(buffer.releases(), () -> woken.add("later task"));
        assertThat(file.readCount()).isZero();

        buffer.release(LENGTH);

        assertThat(woken).containsExactly("decode task", "section", "later task");
        assertThat(file.readCount()).as("the section started on the release").isEqualTo(1);
        assertThat(buffer.held()).isEqualTo(LENGTH);
    }

    /// A read whose F is zero, as one with a row limit, starts section requests only on demand.
    @Test
    void aReadThatDoesNotSpeculateStartsOnlyOnDemand() {
        CountingInputFile file = file();
        FetchSequence sequence = sequence(new ReadAheadBuffer(1 << 20), 0);
        insert(sequence, file, 0, 3);

        assertThat(file.readCount()).isZero();
    }

    /// A section request is released once every column holding it has passed it, which returns its
    /// charges and drops its bytes; a later demand for it is a fault.
    @Test
    void aSectionRequestIsReleasedOnceEveryHolderHasPassedIt() {
        CountingInputFile file = file();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        FetchSequence sequence = sequence(buffer, 1 << 20);
        SectionRequest section = sequence.insert(0, 0, file,
                List.of(new FetchSequence.Span(0, LENGTH, "rg=0 col=a col=b", 2))).getFirst();
        assertThat(buffer.held()).isEqualTo(LENGTH);

        section.release();
        assertThat(buffer.held()).as("held by the second column").isEqualTo(LENGTH);
        section.release();
        assertThat(buffer.held()).isZero();
        assertThat(sequence.speculativeBytes()).isZero();
        assertThatThrownBy(section::issue)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Section request at offset 0 (length 100) of <memory> was released before it was read");
    }

    /// A speculative section request that fails is dropped, its charges returned, and started again
    /// when demanded.
    @Test
    void aFailedSpeculativeSectionRequestIsDroppedAndReadAgainOnDemand() throws Exception {
        FailingOnce file = new FailingOnce();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        FetchSequence sequence = sequence(buffer, 1 << 20);
        SectionRequest section = insert(sequence, file, 0, 1).getFirst();

        assertThat(section.poll()).isNull();
        assertThat(buffer.held()).isZero();
        assertThat(sequence.speculativeBytes()).isZero();
        assertThat(fetch(section).remaining()).isEqualTo(LENGTH);
        assertThat(file.reads).isEqualTo(2);
    }

    /// Close drops the section requests not started, waits for those in flight without
    /// interrupting them, and releases every charge.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeWaitsForTheSectionRequestsInFlightAndReleasesEveryCharge() throws Exception {
        HeldFile file = new HeldFile();
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        PrefetchTasks tasks = new PrefetchTasks(task -> Thread.ofVirtual().start(task));
        FetchSequence sequence = new FetchSequence(tasks, buffer, LENGTH, () -> {});
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
        assertThat(file.reads).as("the sections not started").isEqualTo(1);
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
    private static FetchSequence sequence(ReadAheadBuffer buffer, long fetchAheadBytes) {
        return new FetchSequence(new PrefetchTasks(Runnable::run), buffer, fetchAheadBytes, () -> {});
    }

    /// Inserts `count` section requests of [#LENGTH] bytes, back to back from offset 0, for work
    /// item `workItemIndex`, each held by one column.
    private static List<SectionRequest> insert(FetchSequence sequence, InputFile file, int workItemIndex,
                                                    int count) {
        List<FetchSequence.Span> spans = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            spans.add(new FetchSequence.Span((long) i * LENGTH, LENGTH, "rg=" + workItemIndex + " col=a", 1));
        }
        return sequence.insert(0, workItemIndex, file, spans);
    }

    /// `section`'s bytes, demanding them and waiting for them as a thread off the pool does.
    private static ByteBuffer fetch(SectionRequest section) throws IOException {
        while (true) {
            ByteBuffer bytes = section.poll();
            if (bytes != null) {
                return bytes;
            }
            section.issue().await();
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
