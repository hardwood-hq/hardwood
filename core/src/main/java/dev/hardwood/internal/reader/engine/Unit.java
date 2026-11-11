/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
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

/// A row group of the read in one of its states, each holding what the row group has in it. A
/// state's transitions return the next state; [RowGroupPlanner] keeps each unit's current state
/// and replaces it on each transition, and the parts after the planner read the [Batched] one.
/// Each state holds the unit's listing, what its file's listing decided of it.
sealed interface Unit {

    UnitListing listing();

    /// Listed: its file's footer is parsed and its statistics keep it. `offsetIndexes` and
    /// `columnIndexes` are the file ordinals of the columns whose OffsetIndex and ColumnIndex its
    /// plan reads, from `window`, which also holds its Bloom filters.
    record Listed(UnitListing listing, BitSet offsetIndexes, BitSet columnIndexes,
                  RowGroupPlanner.WindowFetch window) implements Unit {

        /// Its Bloom filters have arrived and it has a leaf they or its dictionaries may decide.
        Probing probing() {
            List<RangeFetch> probed = new ArrayList<>();
            List<FileRange> missing = new ArrayList<>();
            String reason = "rg=" + listing.rowGroup() + " pruning";
            ByteSource bytes = (offset, length) -> {
                ByteBuffer found = RangeFetch.read(window.bloomFilters, offset, length);
                found = found != null ? found : RangeFetch.read(probed, offset, length);
                if (found == null) {
                    missing.add(new FileRange(offset, length, reason));
                }
                return found;
            };
            return new Probing(this, new RowGroupPruning.Probe(), probed, missing, bytes);
        }

        /// It has nothing to probe: its page index is waited for.
        AwaitingIndex nothingToProbe() {
            return new AwaitingIndex(this, null, List.of());
        }
    }

    /// Probing: its Bloom filters and dictionaries are probed, a pass at a time, from `bytes`. The
    /// probe outlives its passes and keeps reading the source it was first given, so the state
    /// accumulates in place: `probe` holds what the passes so far decided and decoded, `probed` the
    /// dictionary bytes they fetched, in file order, and `missing` what the last pass lacked. Only
    /// the planner sees it.
    record Probing(Listed listed, RowGroupPruning.Probe probe, List<RangeFetch> probed, List<FileRange> missing,
                   ByteSource bytes) implements Unit {

        @Override
        public UnitListing listing() {
            return listed.listing();
        }

        /// A pass lacked bytes, `more`, which the next reads once fetched.
        void fetching(List<RangeFetch> more) {
            probed.addAll(more);
            probed.sort(Comparator.comparingLong(r -> r.range.start()));
        }

        /// The probe kept it, reporting `event` unless it is `null`: its page index is waited for.
        AwaitingIndex kept(Event event) {
            return new AwaitingIndex(listed, probe.dictionaries(), event == null ? List.of() : List.of(event));
        }

        /// The probe dropped it, reporting `event` unless it is `null`: it is planned with no row
        /// of its `columns`.
        Planned dropped(Event event, int columns) {
            return new Planned(listing(), UnitPlan.empty(columns, RowRanges.NONE),
                    event == null ? List.of() : List.of(event));
        }
    }

    /// Awaiting its page index: kept by its probe, whose decoded `dictionaries` its pages decode
    /// with (`null` for none), with the `events` its pruning reports so far.
    record AwaitingIndex(Listed listed, RowGroupDictionaryFilterSource dictionaries, List<Event> events)
            implements Unit {

        @Override
        public UnitListing listing() {
            return listed.listing();
        }

        /// Its page index arrived and gave `plan`, reporting `pageEvents`.
        Planned planned(UnitPlan plan, List<Event> pageEvents) {
            List<Event> all = new ArrayList<>(events);
            all.addAll(pageEvents);
            return new Planned(listing(), plan, List.copyOf(all));
        }
    }

    /// Planned: `plan` gives the rows its filter keeps, each column's pages and dictionary page,
    /// and its ranges; `events` is what its pruning reports once the read reaches it.
    record Planned(UnitListing listing, UnitPlan plan, List<Event> events) implements Unit {

        /// It and every unit before it are planned: the batch plan gives it `batches`, and a unit
        /// of which they read no row (dropped, or past the read's row limit) reads no page.
        Batched batched(BatchPlan.RowGroupBatches batches) {
            return new Batched(listing, batches.segmentCount() == 0
                    ? UnitPlan.empty(plan.pages().length, plan.matching()) : plan, batches, events);
        }
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
}
