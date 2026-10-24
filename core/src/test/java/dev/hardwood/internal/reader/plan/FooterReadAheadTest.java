/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.PrefetchTasks;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/// The rules by which a [FooterReadAhead] starts and charges a read's footers (FETCH_PLANNING.md,
/// Opening files ahead).
class FooterReadAheadTest {

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

    /// The footers of the files after the one reached start, up to the files ahead; the
    /// next file's is not charged, each one past it is charged the tail its read takes.
    @Test
    void footersAheadStartWithTheOnesPastTheNextFileCharged() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 10);

        readAhead.reached(0);

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isEqualTo(2L * TAIL);

        readAhead.reached(2);
        assertThat(footers.started).containsExactly(1, 2, 3, 4, 5);
        assertThat(buffer.held()).as("files 2 to 5 charged; 2 and 3 started past the next file")
                .isEqualTo(4L * TAIL);
    }

    /// A footer past the next file waits behind the read's data of earlier files: while a section
    /// request planned and not started waits for F, only the next file's footer starts.
    @Test
    void noFooterPastTheNextFileStartsWhileTheReadsDataWaits() {
        Footers footers = new Footers();
        FetchSequence sequence = sequence(new ReadAheadBuffer(1 << 20), 100);
        FooterReadAhead readAhead = readAhead(sequence, footers, 3, 10);
        CountingInputFile file = new CountingInputFile(ByteBuffer.allocate(1_000));
        // Planned, larger than F, so it does not start speculatively
        sequence.insert(0, 5, file, List.of(new Span(0, 200, "rg=0 col=a", 1)));

        readAhead.reached(0);
        assertThat(footers.started).containsExactly(1);

        sequence.forget(5);
        assertThat(footers.started).containsExactly(1, 2, 3);
    }

    /// A footer R has no room for waits for no one, and starts on a later release.
    @Test
    void aFooterThatDoesNotFitStartsOnALaterRelease() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(2 * TAIL);
        // Another read holds half of R
        assertThat(buffer.tryCharge(TAIL)).isTrue();
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 10);

        readAhead.reached(0);
        assertThat(footers.started).as("no room for file 3").containsExactly(1, 2);

        buffer.release(TAIL);

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isEqualTo(2L * TAIL);
    }

    /// A read that passes files whose footers wait for R, as when statistics prune them, starts
    /// those footers at once and uncharged: none of them takes room the read's data waits for.
    @Test
    void footersOfFilesTheReadPassedStartUncharged() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(2 * TAIL);
        // Another read holds all of R
        assertThat(buffer.tryCharge(2 * TAIL)).isTrue();
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 10);
        readAhead.reached(0);
        assertThat(footers.started).containsExactly(1);

        readAhead.reached(3);

        assertThat(footers.started).containsExactly(1, 2, 3, 4);
        assertThat(buffer.held()).as("the other read's charge alone").isEqualTo(2L * TAIL);
    }

    /// Under a row limit only the next file's footer starts, uncharged.
    @Test
    void aReadWithARowLimitStartsOnlyTheNextFilesFooter() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 0), footers, 1, 10);

        readAhead.reached(0);
        readAhead.reached(1);

        assertThat(footers.started).containsExactly(1, 2);
        assertThat(buffer.held()).isZero();
    }

    /// A file the read is done with drops its tail and returns its footer's charge.
    @Test
    void aFileTheReadIsDoneWithDropsItsTailAndItsCharge() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 4);
        readAhead.reached(0);

        readAhead.reached(3);
        readAhead.filesDoneBefore(3);
        assertThat(footers.dropped).containsExactly(0, 1, 2);
        assertThat(buffer.held()).as("file 3's charge").isEqualTo(TAIL);

        readAhead.filesDoneBefore(4);
        assertThat(footers.dropped).containsExactly(0, 1, 2, 3);
        assertThat(buffer.held()).isZero();
    }

    /// A read that plans no further file starts no further footer, and returns the charges of
    /// those read ahead.
    @Test
    void aStoppedReadStartsNoFurtherFooter() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 10);
        readAhead.reached(0);

        readAhead.stop();
        readAhead.reached(2);

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isZero();
        assertThat(footers.dropped).as("the tails of the footers read ahead").containsExactlyInAnyOrder(1, 2, 3);
    }

    /// A read that stops as it requests footers starts none of those it had not started, and holds
    /// no charge for them.
    @Test
    void footersRequestedAsTheReadStopsStartNone() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 10);
        // Planning fails as the next file's footer starts, before the footers ahead are queued
        footers.onStart.put(1, readAhead::stop);

        readAhead.reached(0);

        assertThat(footers.started).containsExactly(1);
        assertThat(buffer.held()).isZero();
    }

    /// A closed read starts no further footer, returns every footer's charge and drops the footers
    /// it requested.
    @Test
    void aClosedReadReturnsItsChargesAndDropsItsFooters() {
        ReadAheadBuffer buffer = new ReadAheadBuffer(1 << 20);
        Footers footers = new Footers();
        FooterReadAhead readAhead = readAhead(sequence(buffer, 1 << 20), footers, 3, 10);
        readAhead.reached(0);

        readAhead.close();
        readAhead.reached(2);

        assertThat(footers.started).containsExactly(1, 2, 3);
        assertThat(buffer.held()).isZero();
        assertThat(footers.dropped).containsExactly(0, 1, 2, 3);
    }

    private static FetchSequence sequence(ReadAheadBuffer buffer, long fetchAheadBytes) {
        return new FetchSequence(new PrefetchTasks(Runnable::run), buffer, fetchAheadBytes, () -> {});
    }

    private static FooterReadAhead readAhead(FetchSequence sequence, Footers footers, int filesAhead, int fileCount) {
        return new FooterReadAhead(sequence, footers, filesAhead, fileCount);
    }

    /// Footer reads that record what the read starts and drops.
    private static final class Footers implements FooterReadAhead.Footers {

        final List<Integer> started = new ArrayList<>();
        final List<Integer> dropped = new ArrayList<>();
        /// Run as the read of a file's footer starts.
        final Map<Integer, Runnable> onStart = new HashMap<>();

        @Override
        public void start(int fileIndex) {
            started.add(fileIndex);
            onStart.getOrDefault(fileIndex, () -> {}).run();
        }

        @Override
        public void drop(int fileIndex) {
            dropped.add(fileIndex);
        }
    }
}
