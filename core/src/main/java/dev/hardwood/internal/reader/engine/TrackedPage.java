/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.List;

import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.plan.NeededPage;

/// A page of a batched unit, from the start of its tracking until it is decoded: the page, the ranges its bytes
/// lie in, its chunk's dictionary (`null` for a chunk without one), how many of those it still
/// waits for, and, once [PagePlacer] has given them, its decoder and task and whether it is ready
/// to be placed. A dictionary page (`data` false) produces its chunk's dictionary's values. Used
/// under the read's lock.
final class TrackedPage {

    private final Unit.Batched unit;
    private final int column;
    private final long key;
    private final NeededPage page;
    private final List<RangeFetch> ranges;
    private final ChunkDictionary dictionary;
    private final boolean data;
    /// the rows a page its inline statistics drop stands for, as nulls without bytes; `0` for a
    /// page read from its bytes
    private int placeholder;
    private PageDecoder decoder;
    private PageTask task;
    private boolean ready;
    /// how many of its ranges, its dictionary and, for a flat page, its placement it still waits for
    private int missing;

    TrackedPage(Unit.Batched unit, int column, long key, NeededPage page, List<RangeFetch> ranges,
                ChunkDictionary dictionary, boolean data) {
        this.unit = unit;
        this.column = column;
        this.key = key;
        this.page = page;
        this.ranges = ranges;
        this.dictionary = dictionary;
        this.data = data;
    }

    Unit.Batched unit() {
        return unit;
    }

    int column() {
        return column;
    }

    /// The first row of the read it serves.
    long key() {
        return key;
    }

    NeededPage page() {
        return page;
    }

    /// The ranges its bytes lie in.
    List<RangeFetch> ranges() {
        return ranges;
    }

    /// Its chunk's dictionary, `null` for a chunk without one.
    ChunkDictionary dictionary() {
        return dictionary;
    }

    /// Whether it is a data page; a dictionary page produces its chunk's dictionary's values.
    boolean data() {
        return data;
    }

    /// The rows it stands for as nulls, read from no bytes; `0` for a page read from its bytes.
    int placeholder() {
        return placeholder;
    }

    /// Its inline statistics drop it: it stands for `rows` rows of nulls.
    void standsForNulls(int rows) {
        placeholder = rows;
    }

    /// Its chunk's decoder, once [PagePlacer] has given it.
    PageDecoder decoder() {
        return decoder;
    }

    void decoder(PageDecoder decoder) {
        this.decoder = decoder;
    }

    /// Its task, once it is ready to be placed.
    PageTask task() {
        return task;
    }

    /// Whether it is ready to be placed.
    boolean ready() {
        return ready;
    }

    /// It is ready to be placed as `task`.
    void ready(PageTask task) {
        this.task = task;
        this.ready = true;
    }

    /// It waits for one thing more.
    void waitForOneMore() {
        missing++;
    }

    /// One thing it waits for is there; returns whether it waits for nothing more.
    boolean oneArrived() {
        return --missing == 0;
    }

    /// Whether it waits for nothing.
    boolean waitsForNothing() {
        return missing == 0;
    }

    /// The page's bytes, from its ranges: `null` where one has not arrived.
    ByteBuffer bytes() {
        return RangeFetch.read(ranges, page.location().offset(), page.location().compressedPageSize());
    }
}
