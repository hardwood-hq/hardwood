/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.Map;
import java.util.TreeMap;

/// One column's decoded pages, and the row up to which they are contiguous. Answers and records;
/// decides nothing.
final class DecodedRows {

    /// decoded pages by first row, until the batches they fill are built
    private final TreeMap<Long, long[]> pages = new TreeMap<>();
    /// the row up to which every page is decoded
    private long through;

    /// Whether every row before `row` is decoded.
    boolean decodedBefore(long row) {
        return through >= row;
    }

    /// `page` is decoded. Decodes end out of order, so the decoded rows grow only through
    /// contiguous pages.
    void decoded(Page page, long[] values) {
        pages.put(page.firstRow(), values);
        long[] next;
        while ((next = pages.get(through)) != null) {
            through += next.length;
        }
    }

    /// Takes rows `[first, end)`, every one of them decoded, and drops the pages that end there.
    long[] takeRows(long first, long end) {
        long[] rows = new long[Math.toIntExact(end - first)];
        for (long r = first; r < end; r++) {
            Map.Entry<Long, long[]> page = pages.floorEntry(r);
            rows[Math.toIntExact(r - first)] = page.getValue()[Math.toIntExact(r - page.getKey())];
        }
        pages.headMap(end, true).entrySet().removeIf(p -> p.getKey() + p.getValue().length <= end);
        return rows;
    }
}
