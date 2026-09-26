/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

class CoalescedRangesTest {

    private static final int GAP = CoalescingPolicy.GAP_BYTES;
    private static final long MAX = CoalescingPolicy.MAX_SPAN_BYTES;

    @Test
    void rangesWithinTheGapAreFetchedInOneRequest() throws IOException {
        CountingInputFile file = countingFile(4096);
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(2000, 100)
                .add(100, 50)
                .add(1000, 10)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(1);
        assertThat(ranges.fetchedBytes()).isEqualTo(2000);

        ranges.fetch(file);

        assertThat(file.reads()).extracting(CountingInputFile.Read::offset, CountingInputFile.Read::length)
                .containsExactly(tuple(100L, 2000));
        assertThat(ranges.slice(1000, 10).get(0)).isEqualTo(byteAt(1000));
        assertThat(ranges.slice(2000, 100).get(99)).isEqualTo(byteAt(2099));
    }

    @Test
    void aGapBeyondTheLimitSplitsTheRequests() {
        CoalescedRanges bridged = CoalescedRanges.builder().add(0, 100).add(100 + GAP, 100).build();
        CoalescedRanges split = CoalescedRanges.builder().add(0, 100).add(100 + GAP + 1, 100).build();

        assertThat(bridged.requestCount()).isEqualTo(1);
        assertThat(split.requestCount()).isEqualTo(2);
        assertThat(split.fetchedBytes()).isEqualTo(200);
    }

    @Test
    void aRequestSpansAtMostTheLimit() {
        // Gaps of 100 bytes each; the third range would take the span to MAX + 100.
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(0, Math.toIntExact(MAX - 1000))
                .add(MAX - 900, 100)
                .add(MAX - 700, 800)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(2);
        assertThat(ranges.fetchedBytes()).isEqualTo(MAX - 800 + 800);
    }

    @Test
    void touchingRangesSplitAtTheLimit() {
        // Back to back, as one row group's index slices are: together they span MAX + 100.
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(0, Math.toIntExact(MAX - 100))
                .add(MAX - 100, 100)
                .add(MAX, 100)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(2);
        assertThat(ranges.fetchedBytes()).isEqualTo(MAX + 100);
    }

    @Test
    void aRangeLongerThanTheLimitIsARequestOfItsOwn() {
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(0, 100)
                .add(200, Math.toIntExact(MAX + 1))
                .add(MAX + 300, 100)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(3);
        assertThat(ranges.fetchedBytes()).isEqualTo(MAX + 201);
    }

    @Test
    void rangesFarApartAreNeverOneRequest() {
        // Two ranges 3 GiB apart need no buffer spanning both.
        long threeGiB = 3L * 1024 * 1024 * 1024;
        CoalescedRanges ranges = CoalescedRanges.builder().add(0, 100).add(threeGiB, 100).build();

        assertThat(ranges.requestCount()).isEqualTo(2);
        assertThat(ranges.fetchedBytes()).isEqualTo(200);
    }

    @Test
    void overlappingAndTouchingRangesAreOneRequest() throws IOException {
        CountingInputFile file = countingFile(8192);
        // [0, 150) overlaps [50, 60) and [100, 200), which touches [200, 300): one request.
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(100, 100)
                .add(0, 150)
                .add(200, 100)
                .add(50, 10)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(1);
        assertThat(ranges.fetchedBytes()).isEqualTo(300);

        ranges.fetch(file);

        assertThat(file.bytesRead()).isEqualTo(300);
        assertThat(ranges.slice(50, 10).get(0)).isEqualTo(byteAt(50));
        assertThat(ranges.slice(0, 300).get(299)).isEqualTo(byteAt(299));
    }

    @Test
    void anEmptySetFetchesNothing() throws IOException {
        CountingInputFile file = countingFile(16);
        CoalescedRanges ranges = CoalescedRanges.builder().build();

        ranges.fetch(file);

        assertThat(ranges.requestCount()).isZero();
        assertThat(file.readCount()).isZero();
    }

    @Test
    void sliceBeforeFetchFails() {
        CoalescedRanges ranges = CoalescedRanges.builder().add(0, 10).build();

        assertThatThrownBy(() -> ranges.slice(0, 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Ranges not fetched yet");
    }

    @Test
    void aRangeNoRequestHoldsIsRejected() throws IOException {
        CoalescedRanges ranges = CoalescedRanges.builder().add(100, 10).build();
        ranges.fetch(countingFile(256));

        assertThatThrownBy(() -> ranges.slice(105, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No request holds the range [105, 115)");
        assertThatThrownBy(() -> ranges.slice(50, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No request holds the range [50, 60)");
    }

    @Test
    void fetchingTwiceFails() throws IOException {
        CoalescedRanges ranges = CoalescedRanges.builder().add(0, 10).build();
        ranges.fetch(countingFile(16));

        assertThatThrownBy(() -> ranges.fetch(countingFile(16)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Ranges already fetched");
    }

    @Test
    void aFailedFetchCanBeRepeated() throws IOException {
        CoalescedRanges ranges = CoalescedRanges.builder().add(0, 10).add(10 + GAP + 1, 10).build();
        // The second request lies past the end of this file and fails.
        assertThatThrownBy(() -> ranges.fetch(countingFile(64)))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("[<memory>] readRange(" + (10 + GAP + 1) + ", 10) out of bounds (64 bytes)");
        assertThat(ranges.isFetched()).isFalse();

        ranges.fetch(countingFile(GAP + 64));

        assertThat(ranges.slice(10 + GAP + 1, 10).get(0)).isEqualTo(byteAt(10 + GAP + 1));
    }

    @Test
    void aNegativeRangeIsRejected() {
        assertThatThrownBy(() -> CoalescedRanges.builder().add(-1, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid range: offset -1, length 10");
        assertThatThrownBy(() -> CoalescedRanges.builder().add(0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid range: offset 0, length -1");
    }

    /// A file whose byte at offset `i` is `byteAt(i)`.
    private static CountingInputFile countingFile(int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            buffer.put(i, byteAt(i));
        }
        CountingInputFile file = new CountingInputFile(InputFile.of(buffer));
        file.open();
        return file;
    }

    private static byte byteAt(long offset) {
        return (byte) (offset * 31);
    }
}
