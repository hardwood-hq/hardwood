/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

/// The largest page a read accepts: one whose declared sizes fit the arrays its decode allocates.
///
/// A page header's sizes and counts are the first values of a page the reader sees, and the
/// decode sizes buffers from them before any of the page's data is read: the decompression
/// output from `uncompressed_page_size`, the level, dictionary-id and offset scratch from
/// `num_values`. How much of that a read may hold is the working set's bound
/// (`WorkingSet`); this one keeps each size within one Java array, so that a page past it fails
/// as malformed rather than overflowing the arithmetic that sizes its arrays.
public final class PageLimit {

    /// The longest array the JVM allocates on every platform: a few elements short of
    /// [Integer#MAX_VALUE], which some reserve for the array's header.
    static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

    /// The largest `uncompressed_page_size` a reader accepts: the decompression output is one
    /// `byte` array of that length.
    public static final int MAX_PAGE_BYTES = MAX_ARRAY_LENGTH;

    /// The most values a data page may declare to a reader: one fewer than the longest array, as
    /// a binary page's offsets take one entry per value and one more.
    public static final int MAX_PAGE_VALUES = MAX_ARRAY_LENGTH - 1;

    /// How the error for a page's size names its bound.
    static final String MAX_PAGE_BYTES_BOUND = "the longest array a page decodes into";

    /// How the error for a data page's `num_values` names its bound.
    static final String MAX_PAGE_VALUES_BOUND = "the longest array a page's values decode into, less one";

    private PageLimit() {
    }
}
