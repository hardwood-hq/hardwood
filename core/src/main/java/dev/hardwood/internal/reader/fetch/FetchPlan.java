/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;


/// Plan for fetching and iterating pages of a single projected column in a row group.
///
/// Implementations:
///
/// - [IndexedFetchPlan]: pages pre-computed from OffsetIndex, their bytes sliced from the row
///   group's [Section]s.
/// - [SequentialFetchPlan]: pages discovered lazily by scanning headers in those sections.
/// - [SkippedColumnFetchPlan]: a column not read in the row group; fetches nothing and
///   yields a single boundary marker.
/// - [#EMPTY]: a column with no pages to read in the row group.
///
/// The read is agnostic of the implementation: it drains `walk()`.
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
}
