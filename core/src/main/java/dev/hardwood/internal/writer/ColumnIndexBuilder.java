/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dev.hardwood.metadata.ColumnIndex;

/// Assembles a chunk's `ColumnIndex` from its pages' bounds, in page order.
///
/// A chunk gets none where the format forbids its bounds: a column whose annotation names no
/// order, and a floating-point chunk with a page whose present values are all `NaN`, which has
/// no bounds under the type-defined order the footer declares. `boundary_order` is taken over
/// the bounds as written, after truncation, since truncating a text bound at a code-point
/// boundary does not preserve the order of the values it was taken from.
final class ColumnIndexBuilder {

    private static final byte[] NO_BOUND = new byte[0];

    private final ValueEncoder values;
    private boolean expressible;
    private boolean[] nullPages = new boolean[16];
    private long[] nullCounts = new long[16];
    private long[] nanCounts = new long[16];
    private final List<byte[]> mins = new ArrayList<>();
    private final List<byte[]> maxs = new ArrayList<>();
    private int pageCount;
    private boolean countsNaN;
    private byte[] previousMin;
    private byte[] previousMax;
    private boolean ascending = true;
    private boolean descending = true;

    /// @param values the chunk's value encoder, which compares its bounds
    /// @param bounded whether the column has an order its bounds are written in
    ColumnIndexBuilder(ValueEncoder values, boolean bounded) {
        this.values = values;
        this.expressible = bounded;
    }

    void add(PageBounds page) {
        if (!expressible) {
            return;
        }
        if (page.min() == null && page.valueCount() > 0) {
            // Present values that extend no bound are NaN, so the page has no bounds to state.
            expressible = false;
            return;
        }
        if (pageCount == nullPages.length) {
            nullPages = Arrays.copyOf(nullPages, pageCount * 2);
            nullCounts = Arrays.copyOf(nullCounts, pageCount * 2);
            nanCounts = Arrays.copyOf(nanCounts, pageCount * 2);
        }
        nullCounts[pageCount] = page.nullCount();
        nanCounts[pageCount] = page.nanCount();
        countsNaN = page.nanCount() != StatisticsCollector.NO_NAN_COUNT;
        if (page.min() == null) {
            nullPages[pageCount++] = true;
            mins.add(NO_BOUND);
            maxs.add(NO_BOUND);
            return;
        }
        nullPages[pageCount++] = false;
        mins.add(page.min());
        maxs.add(page.max());
        if (previousMin != null) {
            int minStep = values.compareBounds(page.min(), previousMin);
            int maxStep = values.compareBounds(page.max(), previousMax);
            ascending &= minStep >= 0 && maxStep >= 0;
            descending &= minStep <= 0 && maxStep <= 0;
        }
        previousMin = page.min();
        previousMax = page.max();
    }

    /// The chunk's `ColumnIndex`, or `null` where the chunk cannot have one.
    ColumnIndex build() {
        if (!expressible || pageCount == 0) {
            return null;
        }
        return new ColumnIndex(Arrays.copyOf(nullPages, pageCount), List.copyOf(mins), List.copyOf(maxs),
                boundaryOrder(), Arrays.copyOf(nullCounts, pageCount), null, null,
                countsNaN ? Arrays.copyOf(nanCounts, pageCount) : null);
    }

    private ColumnIndex.BoundaryOrder boundaryOrder() {
        if (ascending) {
            return ColumnIndex.BoundaryOrder.ASCENDING;
        }
        return descending ? ColumnIndex.BoundaryOrder.DESCENDING : ColumnIndex.BoundaryOrder.UNORDERED;
    }
}
