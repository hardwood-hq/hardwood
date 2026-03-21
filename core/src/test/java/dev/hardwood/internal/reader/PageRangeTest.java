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

class PageRangeTest {

    @Test
    void contiguousPagesCoalesceIntoOneRange() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(200, 100, 10),
                new PageLocation(300, 100, 20));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(300);
        assertThat(ranges.get(0).pages()).hasSize(3);
    }

    @Test
    void distantPagesBecomesSeparateRanges() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(5_000_000, 100, 10));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(100);
        assertThat(ranges.get(0).pages()).hasSize(1);
        assertThat(ranges.get(1).offset()).isEqualTo(5_000_000);
        assertThat(ranges.get(1).length()).isEqualTo(100);
        assertThat(ranges.get(1).pages()).hasSize(1);
    }

    @Test
    void smallGapPagesMerge() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(500, 100, 10));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(500); // [100, 600)
        assertThat(ranges.get(0).pages()).hasSize(2);
    }

    @Test
    void singlePageProducesOneRange() {
        var pages = List.of(new PageLocation(100, 100, 0));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(100);
    }

    @Test
    void zeroGapToleranceNeverMergesNonContiguous() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(201, 100, 10)); // 1-byte gap
        List<PageRange> ranges = PageRange.coalesce(pages, 0);

        assertThat(ranges).hasSize(2);
    }

    @Test
    void extendStartIncludesDictionaryPrefix() {
        var range = new PageRange(500, 1000, List.of(new PageLocation(500, 1000, 0)));
        PageRange extended = range.extendStart(200);

        assertThat(extended.offset()).isEqualTo(200);
        assertThat(extended.length()).isEqualTo(1300);
        assertThat(extended.pages()).isEqualTo(range.pages());
    }
}
