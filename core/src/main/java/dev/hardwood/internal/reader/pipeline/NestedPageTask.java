/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.decode.PageTrimmer;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.select.PageRowMask;

/// A page of a column decoded through levels: its job decompresses it into scratch of its own (or
/// leaves values stored uncompressed in the fetched page), decodes its levels and measures it, and
/// the page keeps the scratch until every cell it is placed into is filled.
public final class NestedPageTask extends PageTask {

    /// The page's levels, record starts and byte prefix, which outlive its task.
    final PageDecoder.LevelScratch scratch;
    /// Segments placed from the page whose cells are not filled yet; the page's scratch goes back
    /// to its column's pool once its placement has ended and the count is back at zero.
    final AtomicInteger unfilledSegments = new AtomicInteger();
    /// The page read in two passes: its levels, kept to its mask's records, and its values in
    /// place in its scratch's bytes or the fetched page; written by its task before it is measured.
    PageDecoder.InPlaceNestedPage inPlace;
    /// The page's measure; written by its task before [#measured] is set.
    PageMeasure measure;
    /// Guarded by the column lock: whether the page is measured, so that it can be placed.
    boolean measured;

    public NestedPageTask(PageInfo info, String fileName, int rowGroup, int pageIndex, BatchPlan.RowGroupBatches batches,
                          PageDecoder decoder, long firstRow, PageDecoder.LevelScratch scratch) {
        super(info, fileName, rowGroup, pageIndex, batches, decoder, firstRow);
        this.scratch = scratch;
    }

    public PageMeasure measure() {
        return measure;
    }

    /// Decodes the page's levels into its scratch, its values left in place, trims it to its mask
    /// and measures it; the page is then ready to be placed.
    public void decodeAndMeasure(DecodeBuffers buffers) {
        PageRowMask mask = info.mask();
        PageDecoder.InPlaceNestedPage page = info.isNullPlaceholder()
                ? decoder.nullsInPlace(info.placeholderNumValues(), scratch, true)
                : decoder.decodeNestedInPlace(info.dataPage(), info.dictionary(), scratch, buffers);
        if (!mask.isAll()) {
            page = PageTrimmer.trim(page, mask, scratch);
        }
        inPlace = page;
        measure = PageMeasure.of(page, info.records(), scratch);
    }

    /// Returns the page's scratch to `pool`, where it has room; the page reads it no more.
    public void releaseScratch(ArrayBlockingQueue<PageDecoder.LevelScratch> pool) {
        inPlace = null;
        measure = null;
        scratch.trim();
        pool.offer(scratch);
    }

    /// Whether every segment placed from the page is filled, so that its scratch can go back.
    public boolean allSegmentsFilled() {
        return unfilledSegments.get() == 0;
    }
}
