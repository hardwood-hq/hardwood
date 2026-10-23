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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

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
        long max = CoalescingPolicy.maxSpanBytes();
        assertThat(CoalescingPolicy.merges(0, max - 100, max - 50, max)).isTrue();
        assertThat(CoalescingPolicy.merges(0, max - 100, max - 50, max + 1)).isFalse();
    }

    @Test
    void neededRangesFollowTheRuleAcrossColumns() {
        // Column 0's two pages, `gap` apart, then column 1's page right after: the gap decides
        // whether column 0 takes one request, and column 1 joins the second either way.
        assertThat(spans(GAP)).extracting(FetchSequence.Span::start, FetchSequence.Span::length)
                .containsExactly(tuple(4L, 300 + GAP));
        assertThat(spans(GAP + 1)).extracting(FetchSequence.Span::start, FetchSequence.Span::length)
                .containsExactly(tuple(4L, 100), tuple(105L + GAP, 200));
    }

    @Test
    void aSpanNamesTheColumnsItServesAndCountsTheColumnsOverlappingIt() {
        assertThat(spans(GAP + 1)).extracting(FetchSequence.Span::reason, FetchSequence.Span::holders)
                .containsExactly(tuple("rg=7 col=a", 1), tuple("rg=7 col=a col=b", 2));
    }

    /// The spans of column `a`'s pages `[4, 104)` and `[104 + gap, 204 + gap)` and column `b`'s
    /// page right after them.
    private static List<FetchSequence.Span> spans(int gap) {
        long aEnd = 204L + gap;
        List<FetchPlanBuilder.Range> ranges = List.of(
                new FetchPlanBuilder.Range(0, 4, 104),
                new FetchPlanBuilder.Range(0, 104L + gap, aEnd),
                new FetchPlanBuilder.Range(1, aEnd, aEnd + 100));
        FetchPlanBuilder.Extent[] extents = {
                new FetchPlanBuilder.Extent(0, "a", 4, aEnd),
                new FetchPlanBuilder.Extent(1, "b", aEnd, aEnd + 100) };
        return FetchPlanBuilder.coalesce(ranges, extents, new String[] { "a", "b" }, 7, CoalescingPolicy.maxSpanBytes(),
                Long.MAX_VALUE);
    }
}
