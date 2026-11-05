/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/// The planning stage: the fetched indexes waiting for their parse, the one parse slot, and the
/// row groups planned with their requests. Its share of the engine's poll, [#poll], parses the
/// next row group's index once it has arrived, one at a time, in order; it reads no other stage.
/// What it gives the others, through the poll, is [#plannedRows] and [#plannedRequests].
final class Planning {

    private final List<List<Page>> layout;
    private final long rowGroupRows;
    private final long maxRequestBytes;
    private final Slots slots = new Slots(1);
    private final TreeSet<Integer> fetched = new TreeSet<>();
    /// the row group whose index is parsed next
    private int nextParse;
    /// the rows of the row groups planned
    private long plannedRows;
    /// the requests of the row groups planned, row group by row group, by offset
    private final List<Request> plannedRequests = new ArrayList<>();

    /// @param maxRequestBytes the most bytes a request reads ([#coalesce])
    Planning(List<List<Page>> layout, long rowGroupRows, long maxRequestBytes) {
        this.layout = layout;
        this.rowGroupRows = rowGroupRows;
        this.maxRequestBytes = maxRequestBytes;
    }

    // --- its share of the poll ---

    void poll(List<Command> out) {
        if (slots.free() && fetched.remove(nextParse)) {
            slots.take();
            out.add(new Command.ParseIndex(nextParse++));
        }
    }

    /// The rows below which a page is planned.
    long plannedRows() {
        return plannedRows;
    }

    /// The requests of the row groups planned, in planning order; it only grows.
    List<Request> plannedRequests() {
        return Collections.unmodifiableList(plannedRequests);
    }

    // --- facts ---

    void indexFetched(int rowGroup) {
        fetched.add(rowGroup);
    }

    /// `rowGroup`'s index is parsed: its pages' extents are known, and its requests are cut.
    void parsed(int rowGroup) {
        slots.give();
        plannedRows = (rowGroup + 1) * rowGroupRows;
        plannedRequests.addAll(coalesce(rowGroup));
    }

    void parseFailed() {
        slots.give();
    }

    /// The requests of `rowGroup`: its column chunks, which lie one after the other, read as one
    /// range cut into pieces of at most `maxRequestBytes`. A request may hold the pages of several
    /// columns; a page may straddle two.
    private List<Request> coalesce(int rowGroup) {
        long first = rowGroup * rowGroupRows;
        List<Page> pages = new ArrayList<>();
        for (List<Page> column : layout) {
            for (Page page : column) {
                if (page.firstRow() >= first && page.firstRow() < first + rowGroupRows) {
                    pages.add(page);
                }
            }
        }
        pages.sort((a, b) -> Long.compare(a.offset(), b.offset()));
        List<Request> requests = new ArrayList<>();
        long start = pages.getFirst().offset();
        long end = start;
        for (Page page : pages) {
            if (page.offset() != end) {
                throw new IllegalStateException("a gap before " + page + ": the chunks of a row group touch");
            }
            end = page.end();
        }
        for (long offset = start; offset < end; offset += maxRequestBytes) {
            long to = Math.min(offset + maxRequestBytes, end);
            long firstRow = Long.MAX_VALUE;
            for (Page page : pages) {
                if (page.overlaps(offset, to)) {
                    firstRow = Math.min(firstRow, page.firstRow());
                }
            }
            requests.add(new Request(rowGroup, requests.size(), offset, to - offset, firstRow));
        }
        return requests;
    }

    @Override
    public String toString() {
        return "planning[slots=" + slots + " planned<" + plannedRows + " requests=" + plannedRequests.size() + "]";
    }
}
