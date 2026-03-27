/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PageLocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PageRangeDataTest {

    @Test
    void slicePageFromSingleRange() {
        // Range at file offset 100, 500 bytes of data
        ByteBuffer data = ByteBuffer.allocate(500);
        // Adjust marker at relative offset 200
        data.put(200, (byte) 42); 
        PageRangeData prd = new PageRangeData(List.of(
                new PageRangeData.FetchedRange(100, data)));

        // Page at absolute offset 300, size 50 → relative offset 200 in the range
        PageLocation loc = new PageLocation(300, 50, 0);
        ByteBuffer slice = prd.slicePage(loc);

        assertThat(slice.capacity()).isEqualTo(50);
        assertThat(slice.get(0)).isEqualTo((byte) 42);
    }

    @Test
    void slicePageFromCorrectRangeWhenMultiple() {
        ByteBuffer range1 = ByteBuffer.allocate(200);
        range1.put(0, (byte) 11);
        ByteBuffer range2 = ByteBuffer.allocate(200);
        range2.put(50, (byte) 22);

        PageRangeData prd = new PageRangeData(List.of(
                new PageRangeData.FetchedRange(100, range1),
                new PageRangeData.FetchedRange(5000, range2)));

        // Page in range 1
        PageLocation loc1 = new PageLocation(100, 100, 0);
        assertThat(prd.slicePage(loc1).get(0)).isEqualTo((byte) 11);

        // Page in range 2
        PageLocation loc2 = new PageLocation(5050, 100, 50);
        assertThat(prd.slicePage(loc2).get(0)).isEqualTo((byte) 22);
    }

    @Test
    void slicePageThrowsWhenNotInAnyRange() {
        ByteBuffer data = ByteBuffer.allocate(100);
        PageRangeData prd = new PageRangeData(List.of(
                new PageRangeData.FetchedRange(100, data)));

        PageLocation loc = new PageLocation(9999, 50, 0);
        assertThatThrownBy(() -> prd.slicePage(loc))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void sliceRegionForDictionary() {
        // Simulate a range starting at dict offset 50, covering dict + first data page
        ByteBuffer data = ByteBuffer.allocate(300);
        // Adjust dictionary to start at byte 0 of buffer (file offset 50)
        data.put(0, (byte) 99); 
        PageRangeData prd = new PageRangeData(List.of(
                new PageRangeData.FetchedRange(50, data)));

        // Slice the dictionary region: file offset 50, length 100
        ByteBuffer dictSlice = prd.sliceRegion(50, 100);

        assertThat(dictSlice.capacity()).isEqualTo(100);
        assertThat(dictSlice.get(0)).isEqualTo((byte) 99);
    }

    @Test
    void sliceRegionThrowsWhenNotInAnyRange() {
        ByteBuffer data = ByteBuffer.allocate(100);
        PageRangeData prd = new PageRangeData(List.of(
                new PageRangeData.FetchedRange(100, data)));

        assertThatThrownBy(() -> prd.sliceRegion(9999, 50))
                .isInstanceOf(IllegalStateException.class);
    }
}
