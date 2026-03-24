/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.filter2.compat;

import org.apache.parquet.filter2.predicate.FilterPredicate;

/// Shim for parquet-java's `FilterCompat`.
///
/// Wraps a [FilterPredicate] into a [Filter] for use with
/// `ParquetReader.Builder.withFilter()`.
public class FilterCompat {

    /// A no-op filter that does not filter any records.
    public static final Filter NOOP = new NoOpFilter();

    private FilterCompat() {
    }

    /// Wrap a FilterPredicate into a Filter.
    ///
    /// @param filterPredicate the predicate
    /// @return a filter wrapping the predicate
    public static Filter get(FilterPredicate filterPredicate) {
        if (filterPredicate == null) {
            return NOOP;
        }
        return new FilterPredicateCompat(filterPredicate);
    }

    /// Check whether filtering is required (i.e. the filter is not a no-op).
    ///
    /// @param filter the filter to check
    /// @return true if the filter will actually filter records
    public static boolean isFilteringRequired(Filter filter) {
        return !(filter instanceof NoOpFilter);
    }

    /// Base interface for filters.
    public interface Filter {
    }

    /// A filter backed by a [FilterPredicate].
    public static final class FilterPredicateCompat implements Filter {

        private final FilterPredicate filterPredicate;

        FilterPredicateCompat(FilterPredicate filterPredicate) {
            this.filterPredicate = filterPredicate;
        }

        public FilterPredicate getFilterPredicate() {
            return filterPredicate;
        }
    }

    /// A filter that does nothing.
    public static final class NoOpFilter implements Filter {

        NoOpFilter() {
        }
    }
}
