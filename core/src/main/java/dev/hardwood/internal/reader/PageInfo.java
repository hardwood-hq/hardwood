/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

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
/// The optional [#mask()] selects which records of the decoded page the
/// assembler should keep. Defaults to [PageRowMask#ALL] (keep everything);
/// the filter-pushdown path attaches a tighter mask when the page only
/// partially overlaps the matching rows.
public class PageInfo {

    /// Field value for a page that was never read from the file.
    ///
    /// Private, and paired with an [OptionalLong] accessor, so no caller has to
    /// know the number or agree with anyone else about it. Only
    /// [#nullPlaceholder] produces one: every page that came off disk knows
    /// where it came from, because the plans that read it computed the position
    /// to read at.
    private static final long NO_OFFSET = -1;

    private final ByteBuffer pageData;
    private final ColumnSchema columnSchema;
    private final ColumnMetaData columnMetaData;
    private final Dictionary dictionary;
    private final int placeholderNumValues;
    private final PageRowMask mask;
    private final long fileOffset;

    public PageInfo(ByteBuffer pageData, ColumnSchema columnSchema,
                    ColumnMetaData columnMetaData, Dictionary dictionary,
                    PageRowMask mask, long fileOffset) {
        this(pageData, columnSchema, columnMetaData, dictionary, 0, mask, fileOffset);
    }

    private PageInfo(ByteBuffer pageData, ColumnSchema columnSchema,
                     ColumnMetaData columnMetaData, Dictionary dictionary,
                     int placeholderNumValues, PageRowMask mask, long fileOffset) {
        if (mask == null) {
            throw new IllegalArgumentException("mask must not be null; use PageRowMask.ALL");
        }
        this.pageData = pageData;
        this.columnSchema = columnSchema;
        this.columnMetaData = columnMetaData;
        this.dictionary = dictionary;
        this.placeholderNumValues = placeholderNumValues;
        this.mask = mask;
        this.fileOffset = fileOffset;
    }

    /// Byte offset in the file where this page's header begins, empty for a
    /// page that was never read from one.
    ///
    /// Read only when a page has failed, to say where in the file to look, so
    /// the wrapper costs nothing a passing read pays. The field behind it stays
    /// a primitive: it is written once per page, which is not a place to put an
    /// allocation.
    ///
    /// It is a file offset, not a position within the decoded page: the two are
    /// the same only for an uncompressed column, and a reader sent to the wrong
    /// one finds unrelated bytes.
    public OptionalLong fileOffset() {
        return fileOffset == NO_OFFSET ? OptionalLong.empty() : OptionalLong.of(fileOffset);
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
        return new PageInfo(null, columnSchema, columnMetaData, null, numValues,
                PageRowMask.ALL, NO_OFFSET);
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

    /// Per-page row selection. [PageRowMask#ALL] when the assembler should keep
    /// every row; otherwise a tighter mask coming from filter pushdown.
    public PageRowMask mask() {
        return mask;
    }
}
