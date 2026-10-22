/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

/// The largest page a read accepts, in uncompressed bytes and in values.
///
/// A page header's sizes and counts are the first values of a page the reader sees, and the
/// decode sizes buffers from them before any of the page's data is read: the decompression
/// output from `uncompressed_page_size`, the level and dictionary-id scratch from
/// `num_values`. Nothing else in the file bounds either, since an RLE run encodes any count in
/// a few bytes, so the header readers hold both to this limit.
final class PageLimit {

    static final String MAX_PAGE_BYTES_PROPERTY = "hardwood.internal.maxPageBytes";

    /// How the error for a data page's `num_values` names its bound.
    static final String MAX_PAGE_VALUES_BOUND = MAX_PAGE_BYTES_PROPERTY + " / " + Integer.BYTES;

    private static final int DEFAULT_MAX_PAGE_BYTES = 256 * 1024 * 1024;

    private PageLimit() {
    }

    /// The largest `uncompressed_page_size` a page may declare: [#DEFAULT_MAX_PAGE_BYTES] unless
    /// overridden through [#MAX_PAGE_BYTES_PROPERTY].
    ///
    /// Read on each call rather than cached in a static final, so that the override holds
    /// whenever it is set, for the same reason as `SequentialFetchPlan`'s chunk size. One lookup
    /// per page header is nowhere near a hot path.
    ///
    /// A value that is not a positive byte count is rejected rather than replaced by the
    /// default: an override that does not take effect leaves the reader refusing the very page
    /// it was set for.
    static int maxPageBytes() {
        String override = System.getProperty(MAX_PAGE_BYTES_PROPERTY);
        if (override == null) {
            return DEFAULT_MAX_PAGE_BYTES;
        }
        int bytes;
        try {
            bytes = Integer.parseInt(override);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    MAX_PAGE_BYTES_PROPERTY + " must be a positive byte count, but was: " + override, e);
        }
        if (bytes <= 0) {
            throw new IllegalArgumentException(
                    MAX_PAGE_BYTES_PROPERTY + " must be a positive byte count, but was: " + override);
        }
        return bytes;
    }

    /// The most values a data page may declare: [#maxPageBytes] at four bytes per value, the
    /// size of one entry of the `int` arrays its levels and dictionary ids are decoded into.
    ///
    /// A dictionary page is not held to it: its entries are checked against the bytes of the
    /// page as it is parsed, which [#maxPageBytes] bounds.
    static int maxPageValues() {
        return maxPageBytes() / Integer.BYTES;
    }
}
