/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.schema.ColumnSchema;

/// Page metadata and resolved data buffer.
///
/// The fetch plan resolves the page bytes before creating the `PageInfo`.
/// By the time a decode task calls [#pageData()], the buffer is ready —
/// no lazy I/O, no [ChunkHandle] reference. This keeps `PageInfo` a
/// simple data holder.
///
/// A `PageInfo` may also carry a **null-placeholder** marker, produced by
/// [SequentialFetchPlan] when inline page statistics prove that none of the
/// page's rows can match the active filter predicate. In that mode
/// [#pageData()] is `null`; [#isNullPlaceholder()] is `true`; and
/// [#placeholderNumValues()] gives the number of rows the placeholder stands
/// in for. Decoding short-circuits to an all-null typed page, preserving
/// cross-column row alignment while skipping decompression and value decoding.
///
/// The [#BOUNDARY_MARKER] stands in for a whole row group of a column that is
/// not read there ([SkippedColumnFetchPlan]). It carries no bytes, no metadata
/// and no rows; the worker assembles nothing for it and only closes the batch
/// in progress where its siblings close theirs.
///
/// The optional [#mask()] selects which records of the decoded page are kept.
/// Defaults to [PageRowMask#ALL] (keep everything); the filter-pushdown path
/// attaches a tighter mask when the page only partially overlaps the matching
/// rows. A masked nested page is trimmed to its kept records when it is decoded
/// ([PageTrimmer]); a flat column's assembly copies the kept intervals.
public class PageInfo {

    /// The one page of a column that is not read in a row group. See [SkippedColumnFetchPlan].
    public static final PageInfo BOUNDARY_MARKER = new PageInfo(null, null, null, null, 0, PageRowMask.ALL);

    private final ByteBuffer pageData;
    private final ColumnSchema columnSchema;
    private final ColumnMetaData columnMetaData;
    private final Dictionary dictionary;
    private final int placeholderNumValues;
    private final PageRowMask mask;

    public PageInfo(ByteBuffer pageData, ColumnSchema columnSchema,
                    ColumnMetaData columnMetaData, Dictionary dictionary) {
        this(pageData, columnSchema, columnMetaData, dictionary, 0, PageRowMask.ALL);
    }

    public PageInfo(ByteBuffer pageData, ColumnSchema columnSchema,
                    ColumnMetaData columnMetaData, Dictionary dictionary,
                    PageRowMask mask) {
        this(pageData, columnSchema, columnMetaData, dictionary, 0, mask);
    }

    private PageInfo(ByteBuffer pageData, ColumnSchema columnSchema,
                     ColumnMetaData columnMetaData, Dictionary dictionary,
                     int placeholderNumValues, PageRowMask mask) {
        if (mask == null) {
            throw new IllegalArgumentException("mask must not be null; use PageRowMask.ALL");
        }
        this.pageData = pageData;
        this.columnSchema = columnSchema;
        this.columnMetaData = columnMetaData;
        this.dictionary = dictionary;
        this.placeholderNumValues = placeholderNumValues;
        this.mask = mask;
    }

    /// Creates a null-placeholder `PageInfo` representing `numValues` rows whose
    /// values can be substituted with nulls because inline page stats proved they
    /// cannot match the active predicate. Only valid for columns with
    /// `maxDefinitionLevel > 0`; callers must check before producing one.
    public static PageInfo nullPlaceholder(int numValues, ColumnSchema columnSchema,
                                            ColumnMetaData columnMetaData) {
        if (numValues <= 0) {
            throw new IllegalArgumentException("placeholder numValues must be positive: " + numValues);
        }
        return new PageInfo(null, columnSchema, columnMetaData, null, numValues, PageRowMask.ALL);
    }

    /// Returns the page data buffer (header + compressed data), or `null` if this
    /// is a null-placeholder.
    public ByteBuffer pageData() {
        return pageData;
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

    /// Whether this is the [#BOUNDARY_MARKER].
    public boolean isBoundaryMarker() {
        return this == BOUNDARY_MARKER;
    }

    /// Per-page row selection. [PageRowMask#ALL] when every row is kept;
    /// otherwise a tighter mask coming from filter pushdown.
    public PageRowMask mask() {
        return mask;
    }
}
