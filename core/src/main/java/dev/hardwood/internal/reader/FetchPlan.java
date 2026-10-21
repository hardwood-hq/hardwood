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
/// Implementations:
///
/// - [IndexedFetchPlan]: pages pre-computed from OffsetIndex, bytes fetched lazily
///   via [ChunkHandle]s.
/// - [SequentialFetchPlan]: pages discovered lazily by scanning headers from
///   [ChunkHandle]s.
/// - [SkippedColumnFetchPlan]: a column not read in the row group; fetches nothing and
///   yields a single boundary marker.
/// - [#EMPTY]: a column with no pages to read in the row group.
///
/// [PageSource] is agnostic of the implementation — it just drains `walk()`.
public interface FetchPlan {

    /// A plan with no pages: the row group is dropped by its dictionaries, or the
    /// page index leaves no page of this column to read.
    FetchPlan EMPTY = new FetchPlan() {
        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public PageWalk walk() {
            return PageWalk.empty();
        }
    };

    /// Returns true if this column has no pages in this row group.
    boolean isEmpty();

    /// Returns a walk over the pages for this column, in file order, which never waits for I/O.
    PageWalk walk();

    /// Returns the pages for this column, in file order, waiting for the reads they need; for a
    /// thread off the pool.
    default PageIterator pages() {
        return new PageIterator(walk());
    }

    /// Fetches this plan's first chunk on the calling thread, a speculative task, into
    /// the handle its [#pages] walk reads. No-op for plans that fetch nothing.
    default void prefetch() {}
}
