/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;

import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/// Page metadata and resolved data buffer.
///
/// The fetch plan resolves the page bytes before creating the `PageInfo`.
/// By the time a decode task calls [#dataPage()], the buffer is ready —
/// no lazy I/O, no reference to the request it was sliced from. This keeps `PageInfo` a
/// simple data holder.
///
/// A `PageInfo` may also carry a **null-placeholder** marker, standing for a page whose inline
/// statistics prove that none of its rows can match the active filter predicate
/// ([dev.hardwood.internal.predicate.PageDropPredicates]). In that mode [#dataPage()] is
/// `null`; [#isNullPlaceholder()] is `true`; and [#placeholderNumValues()] gives the number of rows
/// the placeholder stands in for. Decoding short-circuits to an all-null typed page, preserving
/// cross-column row alignment while skipping decompression and value decoding.
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

    public boolean isNullPlaceholder() {
        return placeholderNumValues > 0;
    }

    /// Number of rows the null-placeholder stands in for. Zero for regular pages.
    public int placeholderNumValues() {
        return placeholderNumValues;
    }

    /// Per-page row selection. [PageRowMask#ALL] when every row is kept;
    /// otherwise a tighter mask coming from filter pushdown.
    public PageRowMask mask() {
        return mask;
    }

    /// The records the page contributes after its [#mask()], or [#UNKNOWN_RECORDS] for a column
    /// with repetition.
    public int records() {
        return records;
    }
}
