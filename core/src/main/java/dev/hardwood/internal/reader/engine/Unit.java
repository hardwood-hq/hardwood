/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.UnitListing;
import dev.hardwood.internal.reader.plan.UnitPlan;
import dev.hardwood.internal.reader.select.RowRanges;
import dev.hardwood.internal.reader.step.ByteSource;
import jdk.jfr.Event;

/// A row group of the read in one of its states, each holding what the row group has in it.
/// [RowGroupPlanner] moves each unit from state to state, making the next state's record as it
/// takes a step, and the parts after the planner read the [Batched] one. Each state holds the
/// unit's listing, what its file's listing decided of it.
sealed interface Unit {

    UnitListing listing();

    /// The ranges it holds a reader of, which it returns as it leaves the state, by whatever
    /// transition ([RowGroupPlanner]).
    default List<RangeFetch> holds() {
        return List.of();
    }

    /// Listed: its file's footer is parsed and its statistics keep it. Where its Bloom filters or
    /// dictionaries may drop it, it waits for them, which its window and its own requests fetch;
    /// otherwise it waits for its page index at once.
    /// It holds what listing decided its planning reads, which `BloomAndDictionaryPruning` and `AwaitingIndex` keep: `offsetIndexes` and
    /// `columnIndexes` are the file ordinals of the columns whose OffsetIndex and ColumnIndex its
    /// plan reads, from `window`, which also holds its Bloom filters; `dictionaries` are the
    /// dictionary pages its dictionary pruning reads whose extent the footer gives, fetched with
    /// the window's Bloom filters and held by it alone.
    record Listed(UnitListing listing, BitSet offsetIndexes, BitSet columnIndexes,
                  RowGroupPlanner.WindowFetch window, List<RangeFetch> dictionaries) implements Unit {

        /// Its window's Bloom filters and page index, and its dictionary pages.
        @Override
        public List<RangeFetch> holds() {
            return concat(window.bloomFilters(), dictionaries, window.index());
        }
    }

    /// Pruning by its Bloom filters and dictionaries: they are read from `bytes`, a read step at a
    /// time until none lacks anything, then evaluated once. `inputs` holds what the steps read,
    /// `fetched` the dictionary bytes fetched for it, in file order, and `missing` what the last
    /// step lacked. Only the planner sees it.
    record BloomAndDictionaryPruning(Listed listed, RowGroupPruning.Inputs inputs, List<RangeFetch> fetched, List<FileRange> missing,
                   ByteSource bytes) implements Unit {

        @Override
        public UnitListing listing() {
            return listed.listing();
        }

        /// Its window's Bloom filters and page index, and the dictionary bytes fetched for it.
        @Override
        public List<RangeFetch> holds() {
            return concat(listed.window().bloomFilters(), fetched, listed.window().index());
        }
    }

    /// Awaiting its page index: kept by its Bloom filters and dictionaries, or one neither can
    /// drop, whose decoded `dictionaries` its pages decode with (`null` for none), with the
    /// `events` its pruning reports so far.
    record AwaitingIndex(Listed listed, RowGroupDictionaryFilterSource.DecodedDictionaries dictionaries, List<Event> events)
            implements Unit {

        @Override
        public UnitListing listing() {
            return listed.listing();
        }

        /// Its window's page index.
        @Override
        public List<RangeFetch> holds() {
            return listed.window().index();
        }
    }

    /// Failed: its pruning or plan failed, which fails the read at its first row. It takes no
    /// further transition and holds nothing.
    record Failed(UnitListing listing) implements Unit {
    }

    /// Planned: `plan` gives the rows its filter keeps, each column's pages and dictionary page,
    /// and its ranges; `events` is what its pruning reports once the read reaches it.
    record Planned(UnitListing listing, UnitPlan plan, List<Event> events) implements Unit {
    }

    /// Batched: its batches are taken from the batch plan; its ranges are fetched and its pages handed to its
    /// columns.
    record Batched(UnitListing listing, UnitPlan plan, BatchPlan.RowGroupBatches batches, List<Event> events)
            implements Unit {

        /// The key of the unit's row `row`: its base and the rows its filter keeps before it.
        long key(long row) {
            RowRanges matching = plan.matching();
            return listing.base() + (matching.isAll() || row == 0 ? row
                    : matching.intersect(RowRanges.range(0, row)).rowCount(row));
        }
    }

    private static List<RangeFetch> concat(List<RangeFetch> a, List<RangeFetch> b, List<RangeFetch> c) {
        List<RangeFetch> all = new ArrayList<>(a.size() + b.size() + c.size());
        all.addAll(a);
        all.addAll(b);
        all.addAll(c);
        return all;
    }
}
