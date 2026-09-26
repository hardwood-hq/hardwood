/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;


/// Plan for fetching and iterating pages of a single projected column in a row group.
///
/// Two implementations:
///
/// - [IndexedFetchPlan]: pages pre-computed from OffsetIndex, bytes fetched lazily
///   via [ChunkHandle]s.
/// - [SequentialFetchPlan]: pages discovered lazily by scanning headers from
///   [ChunkHandle]s.
///
/// [PageSource] is agnostic of the implementation — it just drains `pages()`.
public interface FetchPlan {

    /// A plan with no pages (filter excluded all pages for this column).
    FetchPlan EMPTY = new FetchPlan() {
        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public PageIterator pages() {
            return PageIterator.empty();
        }
    };

    /// Returns true if this column has no pages in this row group.
    boolean isEmpty();

    /// Returns an iterator over the pages for this column.
    /// Pages are yielded in file order. Advancing it, and accessing `pageData()`
    /// on a `PageInfo`, may reach the file through the underlying [ChunkHandle].
    PageIterator pages();

    /// Fetches this plan's first chunk on the calling thread, a speculative task.
    /// No-op for sequential plans or empty plans.
    default void prefetch() {}
}
