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

import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.plan.NeededPage;

/// A page of a batched unit, from the start of its tracking until it is decoded: the page, the ranges its bytes
/// lie in, its chunk's dictionary (`null` for a chunk without one), and, once [CellWriter] has
/// given them, its decoder and task and whether it is ready
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
    /// its header, parsed by the scan that found it or for its charge; `null` until then, and for
    /// a header that did not parse
    private PageFormatProbe.HeaderAt header;
    private JobScheduler.Pending pending;
    private PageDecoder decoder;
    private PageTask task;
    private boolean ready;
    /// ended once its column has placed it, which a flat page decodes after
    private final Output<Void> placement = new Output<>();

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

    /// Its chunk's decoder, once [CellWriter] has given it.
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

    /// Its placement, which ends once its column has placed it, filling a cell.
    Output<Void> placement() {
        return placement;
    }

    /// The page's bytes, from its ranges: `null` where one has not arrived.
    ByteBuffer bytes() {
        return RangeFetch.read(ranges, page.location().offset(), page.location().compressedPageSize());
    }

    /// Its header was parsed into `header`, under the read's lock.
    void headerRead(PageFormatProbe.HeaderAt header) {
        this.header = header;
    }

    /// Its entry in the read's reserve while its decode is not added, `null` for none.
    JobScheduler.Pending pending() {
        return pending;
    }

    void pending(JobScheduler.Pending pending) {
        this.pending = pending;
    }

    /// Its header, `null` until parsed and where it did not parse.
    PageFormatProbe.HeaderAt header() {
        return header;
    }

    /// The page, on its decode's job: from its header as parsed before, or parsed here where it
    /// did not parse before, which fails with what the header's defect is, in the words every
    /// reader of a header uses ([PageFormatProbe#readHeader]).
    ///
    /// @throws dev.hardwood.reader.ParquetReadException if its header does not parse
    DataPage dataPage() {
        ByteBuffer bytes = bytes();
        return header == null ? DataPage.parse(bytes, page.location().offset())
                : new DataPage(header.header(), header.length(), bytes);
    }
}
