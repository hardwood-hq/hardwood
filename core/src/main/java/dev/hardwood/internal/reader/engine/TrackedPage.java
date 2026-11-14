/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;

import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.pipeline.PageTask;
import dev.hardwood.internal.reader.plan.NeededPage;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.reader.ParquetReadException;

/// A page of a batched unit, from the start of its tracking until it is decoded: the page, the ranges its bytes
/// lie in, its chunk's dictionary (`null` for a chunk without one), its header once parsed outside
/// the read's lock, and, once [CellWriter] has given them, its decoder and task and whether it is
/// ready to be placed. A dictionary page (`data` false) produces its chunk's dictionary's values.
/// Used under the read's lock, except [#dataPage] on its decode's job.
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
    private PageFormatProbe.HeaderAt header;
    private boolean headerRead;
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

    /// Its header was parsed, outside the read's lock, as `header`: `null` where it does not parse.
    void headerRead(PageFormatProbe.HeaderAt header) {
        this.header = header;
        this.headerRead = true;
    }

    /// Whether its header was parsed, by the time all its ranges have arrived.
    boolean headerRead() {
        return headerRead;
    }

    /// Its header, `null` until parsed or where it does not parse.
    PageFormatProbe.HeaderAt header() {
        return header;
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

    /// The page, with the header parsed before, on its decode's job. A header that did not parse is
    /// parsed again from the whole page, failing as a malformed header does; one that parses so
    /// exceeded the peek a header is read with, which fails the page as it fails a scan.
    ///
    /// @throws ParquetReadException if the header does not parse, or exceeds the peek
    /// @throws IllegalStateException if a header that parses was not handed on
    DataPage dataPage() {
        if (header != null) {
            return new DataPage(header.header(), header.length(), bytes());
        }
        DataPage page = DataPage.parse(bytes());
        long offset = this.page.location().offset();
        if (page.headerSize() > PageFormatProbe.MAX_PEEK_SIZE) {
            throw new ParquetReadException("Page header at offset " + offset + " exceeds maximum peek size ("
                    + PageFormatProbe.MAX_PEEK_SIZE + " bytes)");
        }
        throw new IllegalStateException("The header of the page at offset " + offset
                + " parses, but was not handed on to its decode");
    }

    /// The header of the page of `length` bytes at `offset`, read from `bytes`, outside the read's
    /// lock; `null` where it is malformed, which the page's decode fails on ([#dataPage]).
    static PageFormatProbe.HeaderAt readHeader(ByteSource bytes, long offset, int length) {
        try {
            Parsed<PageFormatProbe.HeaderAt> read = PageFormatProbe.readHeader(bytes, offset, length,
                    PageFormatProbe.INITIAL_PEEK_SIZE, "");
            return read.needsBytes() ? null : read.value();
        }
        catch (ParquetReadException e) {
            return null;
        }
        catch (IOException e) {
            // the sources a read parses headers from slice bytes already there
            throw new UncheckedIOException(e);
        }
    }
}
