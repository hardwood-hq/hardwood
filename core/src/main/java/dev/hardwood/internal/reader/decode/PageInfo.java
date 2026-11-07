/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;

import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.schema.ColumnSchema;

/// Page metadata and resolved data buffer.
///
/// The fetch plan resolves the page bytes before creating the `PageInfo`.
/// By the time a decode task calls [#dataPage()], the buffer is ready —
/// no lazy I/O, no reference to the request it was sliced from. This keeps `PageInfo` a
/// simple data holder.
///
/// A `PageInfo` may also carry a **null-placeholder** marker, produced by
/// [dev.hardwood.internal.reader.fetch.SequentialFetchPlan] when inline page statistics prove that
/// none of the page's rows can match the active filter predicate. In that mode [#dataPage()] is
/// `null`; [#isNullPlaceholder()] is `true`; and [#placeholderNumValues()] gives the number of rows
/// the placeholder stands in for. Decoding short-circuits to an all-null typed page, preserving
/// cross-column row alignment while skipping decompression and value decoding.
///
/// The [#BOUNDARY_MARKER] stands in for a whole row group of a column that is not read there
/// ([dev.hardwood.internal.reader.fetch.SkippedColumnFetchPlan]). It carries no bytes, no metadata
/// and no rows; the column places nothing for it and only closes the batch in progress where its
/// siblings close theirs.
///
/// The optional [#mask()] selects which records of the decoded page are kept.
/// Defaults to [PageRowMask#ALL] (keep everything); the filter-pushdown path
/// attaches a tighter mask when the page only partially overlaps the matching
/// rows. A masked nested page is trimmed to its kept records when it is decoded
/// ([PageTrimmer]); a flat column's fill copies the kept intervals.
///
/// [#records()] is the number of records the page contributes after its mask, from the page's
/// metadata: its header or its offset index row span. It is known for a column without
/// repetition, whose records are its values, and [#UNKNOWN_RECORDS] otherwise: a column with
/// repetition counts its records from its repetition levels once the page is decoded.
public class PageInfo {

    /// [#records()] of a page whose records only its repetition levels give.
    public static final int UNKNOWN_RECORDS = -1;

    /// The one page of a column that is not read in a row group. See
    /// [dev.hardwood.internal.reader.fetch.SkippedColumnFetchPlan].
    public static final PageInfo BOUNDARY_MARKER = new PageInfo(null, null, null, null, 0, PageRowMask.ALL, 0);

    /// The page's bytes with its parsed header, `null` for a page without bytes.
    private final DataPage page;
    private final ColumnSchema columnSchema;
    private final ColumnMetaData columnMetaData;
    private final Dictionary dictionary;
    private final int placeholderNumValues;
    private final PageRowMask mask;
    private final int records;

    /// A data page whose header its fetch plan has parsed, so that what decoding it takes is known
    /// before it is submitted, or with `page` `null` a page without bytes, which no task decodes:
    /// the tests of placement build them.
    ///
    /// @param records the records the page contributes after `mask`, or [#UNKNOWN_RECORDS]
    public PageInfo(DataPage page, ColumnSchema columnSchema, ColumnMetaData columnMetaData, Dictionary dictionary,
                    PageRowMask mask, int records) {
        this(page, columnSchema, columnMetaData, dictionary, 0, mask, records);
    }

    private PageInfo(DataPage page, ColumnSchema columnSchema,
                     ColumnMetaData columnMetaData, Dictionary dictionary,
                     int placeholderNumValues, PageRowMask mask, int records) {
        if (mask == null) {
            throw new IllegalArgumentException("mask must not be null; use PageRowMask.ALL");
        }
        if (records < UNKNOWN_RECORDS) {
            throw new IllegalArgumentException("records must not be negative, got " + records);
        }
        this.page = page;
        this.columnSchema = columnSchema;
        this.columnMetaData = columnMetaData;
        this.dictionary = dictionary;
        this.placeholderNumValues = placeholderNumValues;
        this.mask = mask;
        this.records = records;
    }

    /// The records a page of `column` contributes after `mask`, for [#records()], where it holds
    /// `records` before it: those it keeps for a column without repetition, whose records are its
    /// values, and [#UNKNOWN_RECORDS] for a column with repetition, which counts its records from
    /// its repetition levels once decoded.
    public static int keptRecords(ColumnSchema column, PageRowMask mask, int records) {
        if (column.maxRepetitionLevel() > 0) {
            return UNKNOWN_RECORDS;
        }
        return mask.isAll() ? records : mask.totalRecords();
    }

    /// Creates a null-placeholder `PageInfo` representing `numValues` rows whose
    /// values can be substituted with nulls because inline page stats proved they
    /// cannot match the active predicate. Only valid for columns with
    /// `maxDefinitionLevel > 0`; callers must check before producing one.
    ///
    /// @param records the records the placeholder stands for, or [#UNKNOWN_RECORDS]
    public static PageInfo nullPlaceholder(int numValues, ColumnSchema columnSchema,
                                            ColumnMetaData columnMetaData, int records) {
        if (numValues <= 0) {
            throw new IllegalArgumentException("placeholder numValues must be positive: " + numValues);
        }
        return new PageInfo(null, columnSchema, columnMetaData, null, numValues, PageRowMask.ALL, records);
    }

    /// The page's bytes with its parsed header, which its decode takes, or `null` for a page
    /// without bytes.
    public DataPage dataPage() {
        return page;
    }

    /// The page's bytes, header first, or `null` for a page without bytes.
    public ByteBuffer pageData() {
        return page != null ? page.bytes() : null;
    }

    public ColumnSchema columnSchema() {
        return columnSchema;
    }

    public ColumnMetaData columnMetaData() {
        return columnMetaData;
    }

    public Dictionary dictionary() {
        return dictionary;
    }

    /// The buffers the task decoding the page takes ([WorkingSet.Column#pageBuffers]): from the
    /// header's sizes for a page with bytes, whatever value count it declares; from the values it
    /// stands for for a null placeholder; `0` for a page without either, which no task decodes.
    public long taskBytes() {
        if (isNullPlaceholder()) {
            return WorkingSet.Column.pageBuffers(columnSchema, placeholderNumValues, 0, Encoding.PLAIN);
        }
        // The decompressed bytes, which a page read where it lies does not take: the bound errs high
        return page == null ? 0
                : WorkingSet.Column.pageBuffers(columnSchema, declaredValues(), uncompressedSize(), encoding());
    }

    public boolean isNullPlaceholder() {
        return placeholderNumValues > 0;
    }

    /// Number of rows the null-placeholder stands in for. Zero for regular pages.
    public int placeholderNumValues() {
        return placeholderNumValues;
    }

    /// Whether this is the [#BOUNDARY_MARKER].
    public boolean isBoundaryMarker() {
        return this == BOUNDARY_MARKER;
    }

    /// Per-page row selection. [PageRowMask#ALL] when every row is kept;
    /// otherwise a tighter mask coming from filter pushdown.
    public PageRowMask mask() {
        return mask;
    }

    /// The records the page contributes after its [#mask()], or [#UNKNOWN_RECORDS] for a column
    /// with repetition. `0` for the [#BOUNDARY_MARKER].
    public int records() {
        return records;
    }

    /// The values the page's header declares; `0` for a page without bytes, or one that is no data
    /// page, which its decode refuses.
    public int declaredValues() {
        DataPageHeader v1 = page != null ? page.header().dataPageHeader() : null;
        DataPageHeaderV2 v2 = page != null ? page.header().dataPageHeaderV2() : null;
        return v1 != null ? v1.numValues() : v2 != null ? v2.numValues() : 0;
    }

    /// The page's uncompressed size from its header; `0` for a page without bytes.
    public int uncompressedSize() {
        return page != null ? page.header().uncompressedPageSize() : 0;
    }

    /// The encoding of the page's values from its header; `PLAIN` for a page without bytes, or one
    /// that is no data page.
    public Encoding encoding() {
        DataPageHeader v1 = page != null ? page.header().dataPageHeader() : null;
        DataPageHeaderV2 v2 = page != null ? page.header().dataPageHeaderV2() : null;
        return v1 != null ? v1.encoding() : v2 != null ? v2.encoding() : Encoding.PLAIN;
    }
}
