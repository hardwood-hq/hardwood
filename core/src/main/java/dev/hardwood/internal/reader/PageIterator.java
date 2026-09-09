/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;

import dev.hardwood.internal.ExceptionContext;

/// Walks the pages of one projected column in one row group.
///
/// [java.util.Iterator] in every respect but one: both methods declare
/// [IOException], because advancing is what reaches the file. A page is located
/// by scanning headers or by the offset index, and its bytes arrive from a
/// [ChunkHandle] that may not have fetched them yet, so the first call that
/// needs a byte is a read.
///
/// That is the whole reason this exists rather than an `Iterator<PageInfo>`.
/// `Iterator` forbids a checked exception, so a failed fetch had to leave as an
/// [java.io.UncheckedIOException] and be unwrapped again at the reader — a wrap
/// that said nothing except that one interface in the middle could not carry
/// what had happened. Nothing above here needs it: [PageSource#next] already
/// declares `IOException`, and the pipeline carries a failure across its thread
/// boundary as a `Throwable`, which a checked exception is.
public interface PageIterator {

    /// Whether another page is available, fetching whatever is needed to know.
    ///
    /// @throws IOException if reading the file failed
    boolean hasNext() throws IOException;

    /// The next page.
    ///
    /// @throws IOException if reading the file failed
    /// @throws java.util.NoSuchElementException if no page is available
    PageInfo next() throws IOException;

    /// Which page of the column chunk this walk is on: an ordinal,
    /// [ExceptionContext#DICTIONARY_PAGE], or [ExceptionContext#UNKNOWN_PAGE].
    ///
    /// Read by the retriever when a page is handed on and when one is not — a header that
    /// will not parse fails before any [PageInfo] exists.
    ///
    /// @return the page this walk is on
    default int currentPage() {
        return ExceptionContext.UNKNOWN_PAGE;
    }

    /// A column the filter left with no pages in this row group.
    static PageIterator empty() {
        return new PageIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public PageInfo next() {
                throw new java.util.NoSuchElementException();
            }
        };
    }
}
