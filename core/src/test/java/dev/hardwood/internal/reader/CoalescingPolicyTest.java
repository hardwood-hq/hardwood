/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PageLocation;

import static org.assertj.core.api.Assertions.assertThat;

/// The coalescing rule, and the merges within a column that apply it. The merge across columns is
/// covered end to end by [CrossColumnCoalesceTest].
class CoalescingPolicyTest {

    private static final int GAP = CoalescingPolicy.GAP_BYTES;

    @Test
    void aGapUpToTheLimitIsBridged() {
        assertThat(CoalescingPolicy.merges(0, 100, 100 + GAP, 200 + GAP)).isTrue();
        assertThat(CoalescingPolicy.merges(0, 100, 100 + GAP + 1, 200 + GAP)).isFalse();
    }

    @Test
    void aSpanUpToTheLimitIsMerged() {
        long max = CoalescingPolicy.MAX_SPAN_BYTES;
        assertThat(CoalescingPolicy.merges(0, max - 100, max - 50, max)).isTrue();
        assertThat(CoalescingPolicy.merges(0, max - 100, max - 50, max + 1)).isFalse();
    }

    @Test
    void pagesWithinAColumnFollowTheRule() {
        assertThat(RowGroupIterator.coalescePages(pagesWithGap(GAP), 0)).hasSize(1);
        assertThat(RowGroupIterator.coalescePages(pagesWithGap(GAP + 1), 0)).hasSize(2);
    }

    @Test
    void theDictionaryPageFollowsTheRule() {
        // Dictionary page [4, 1004), first data page at 1004.
        assertThat(RowGroupIterator.foldsDictionary(4, 1004, new PageLocation(1004 + GAP, 100, 0))).isTrue();
        assertThat(RowGroupIterator.foldsDictionary(4, 1004, new PageLocation(1004 + GAP + 1, 100, 0))).isFalse();
    }

    /// Two 100-byte pages, `gap` bytes apart.
    private static List<RowGroupIterator.NeededPage> pagesWithGap(int gap) {
        return List.of(
                new RowGroupIterator.NeededPage(new PageLocation(4, 100, 0), PageRowMask.ALL, 0),
                new RowGroupIterator.NeededPage(new PageLocation(104 + gap, 100, 10), PageRowMask.ALL, 1));
    }
}
