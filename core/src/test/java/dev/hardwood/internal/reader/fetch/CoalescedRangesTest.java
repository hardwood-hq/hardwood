/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoalescedRangesTest {

    private static final int GAP = CoalescingPolicy.GAP_BYTES;
    private static final long MAX = CoalescingPolicy.maxSpanBytes();

    @Test
    void rangesWithinTheGapAreFetchedInOneRequest() {
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(2000, 100)
                .add(100, 50)
                .add(1000, 10)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(1);
        assertThat(ranges.fetchedBytes()).isEqualTo(2000);
        assertThat(ranges.requestStart(0)).isEqualTo(100);
        assertThat(ranges.requestEnd(0)).isEqualTo(2100);
        assertThat(ranges.requestHolding(1000, 10)).isZero();
        assertThat(ranges.requestHolding(2000, 100)).isZero();
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
    void overlappingAndTouchingRangesAreOneRequest() {
        // [0, 150) overlaps [50, 60) and [100, 200), which touches [200, 300): one request.
        CoalescedRanges ranges = CoalescedRanges.builder()
                .add(100, 100)
                .add(0, 150)
                .add(200, 100)
                .add(50, 10)
                .build();

        assertThat(ranges.requestCount()).isEqualTo(1);
        assertThat(ranges.fetchedBytes()).isEqualTo(300);
        assertThat(ranges.requestHolding(50, 10)).isZero();
        assertThat(ranges.requestHolding(0, 300)).isZero();
    }

    @Test
    void anEmptySetHasNoRequest() {
        assertThat(CoalescedRanges.builder().build().requestCount()).isZero();
    }

    @Test
    void aRangeNoRequestHoldsIsRejected() {
        CoalescedRanges ranges = CoalescedRanges.builder().add(100, 10).build();

        assertThatThrownBy(() -> ranges.requestHolding(105, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No request holds the range [105, 115)");
        assertThatThrownBy(() -> ranges.requestHolding(50, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No request holds the range [50, 60)");
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
}
