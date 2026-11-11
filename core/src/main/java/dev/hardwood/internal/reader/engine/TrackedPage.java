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

    final Unit.Batched unit;
    final int column;
    final long key;
    final NeededPage page;
    final List<RangeFetch> ranges;
    final ChunkDictionary dictionary;
    final boolean data;
    /// the rows a page its inline statistics drop stands for, as nulls without bytes; `0` for a
    /// page read from its bytes
    int placeholder;
    PageDecoder decoder;
    PageTask task;
    boolean ready;
    int missing;

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

    /// The page's bytes, from its ranges: `null` where one has not arrived.
    ByteBuffer bytes() {
        return RangeFetch.read(ranges, page.location().offset(), page.location().compressedPageSize());
    }
}
