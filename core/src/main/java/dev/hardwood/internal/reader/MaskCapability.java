/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Whether per-page row masking can be applied across every projected column
/// of a row group without breaking cross-column row alignment.
///
/// Applying a mask on some columns but not others would leave sibling columns
/// row-misaligned at decode time, so the gate is row-group-wide: every
/// projected column must be flat, have an Offset Index, or use
/// `DATA_PAGE_V2` pages so its repetition levels can be walked without
/// decompression. A nested column that lacks an Offset Index *and* uses
/// `DATA_PAGE` (v1) pages closes the gate for the whole row group.
///
/// Computed once per row group during [RowGroupIterator#getSharedMetadata]
/// and cached on [RowGroupIterator.SharedRowGroupMetadata]. Both the
/// per-row-group fetch-plan path and the tail-read fast path consult the
/// cached value rather than re-running the page-format probe.
public enum MaskCapability {
    /// Every projected column is mask-friendly — masks are applied in plans.
    YES,

    /// At least one projected column is nested-v1-without-OffsetIndex —
    /// `matchingRows` is promoted to `RowRanges.ALL` for this row group.
    NO
}
