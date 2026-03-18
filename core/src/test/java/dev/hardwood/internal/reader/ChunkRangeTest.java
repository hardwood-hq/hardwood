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

import dev.hardwood.internal.reader.ChunkRange.ChunkEntry;

import static org.assertj.core.api.Assertions.assertThat;

class ChunkRangeTest {

    @Test
    void adjacentChunksCoalesceIntoOneRange() {
        var entries = List.of(
                new ChunkEntry(0, 100, 500),
                new ChunkEntry(1, 600, 400),
                new ChunkEntry(2, 1000, 300));
        List<ChunkRange> ranges = ChunkRange.coalesce(entries, ChunkRange.MAX_GAP_BYTES);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(1200);
        assertThat(ranges.get(0).entries()).hasSize(3);
    }

    @Test
    void distantChunksBecomesSeparateRanges() {
        var entries = List.of(
                new ChunkEntry(0, 100, 500),
                new ChunkEntry(1, 5_000_000, 500));
        List<ChunkRange> ranges = ChunkRange.coalesce(entries, ChunkRange.MAX_GAP_BYTES);

        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(500);
        assertThat(ranges.get(1).offset()).isEqualTo(5_000_000);
        assertThat(ranges.get(1).length()).isEqualTo(500);
    }

    @Test
    void smallGapBetweenNonAdjacentColumnsMerges() {
        // Columns 0 and 2 projected, column 1 (200 bytes) is the gap
        var entries = List.of(
                new ChunkEntry(0, 100, 300),
                new ChunkEntry(2, 600, 300));
        List<ChunkRange> ranges = ChunkRange.coalesce(entries, ChunkRange.MAX_GAP_BYTES);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(800); // [100, 900)
    }

    @Test
    void singleChunkProducesOneRange() {
        var entries = List.of(new ChunkEntry(5, 1000, 2000));
        List<ChunkRange> ranges = ChunkRange.coalesce(entries, ChunkRange.MAX_GAP_BYTES);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(1000);
        assertThat(ranges.get(0).length()).isEqualTo(2000);
        assertThat(ranges.get(0).entries()).hasSize(1);
    }

    @Test
    void zeroGapToleranceOnlyMergesContiguous() {
        var entries = List.of(
                new ChunkEntry(0, 100, 100),
                new ChunkEntry(1, 200, 100), // contiguous
                new ChunkEntry(2, 301, 100)); // 1-byte gap
        List<ChunkRange> ranges = ChunkRange.coalesce(entries, 0);

        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).entries()).hasSize(2);
        assertThat(ranges.get(1).entries()).hasSize(1);
    }

    @Test
    void overlappingChunksHandledCorrectly() {
        // Dictionary offset can cause a chunk to overlap with the previous one
        var entries = List.of(
                new ChunkEntry(0, 100, 600),
                new ChunkEntry(1, 500, 400)); // starts inside chunk 0
        List<ChunkRange> ranges = ChunkRange.coalesce(entries, ChunkRange.MAX_GAP_BYTES);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(800); // max(700, 900) = 900 - 100 = 800
    }
}
