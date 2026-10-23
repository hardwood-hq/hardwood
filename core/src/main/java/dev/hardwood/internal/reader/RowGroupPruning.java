/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.predicate.BloomFilterColumnRecorder;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.RowGroupIterator.WorkItem;
import dev.hardwood.jfr.RowGroupBloomFilterEvent;
import dev.hardwood.jfr.RowGroupDictionaryFilterEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.RowGroup;

/// The pruning of a read's row groups by metadata: by statistics when a file is planned
/// ([#filterRowGroups]), by Bloom filters and dictionaries once the read reaches a row group
/// ([#probeOnArrival]), and whether the read narrows a row group's rows by the ColumnIndex
/// ([#pageFiltering]).
final class RowGroupPruning {

    private final HardwoodContextImpl context;
    private final ResolvedPredicate filterPredicate;
    private final boolean metadataFilteringEnabled;

    RowGroupPruning(HardwoodContextImpl context, ResolvedPredicate filterPredicate, boolean metadataFilteringEnabled) {
        this.context = context;
        this.filterPredicate = filterPredicate;
        this.metadataFilteringEnabled = metadataFilteringEnabled;
    }

    /// Whether the read prunes by metadata: under a filter, with metadata filtering on.
    boolean prunesByMetadata() {
        return filterPredicate != null && metadataFilteringEnabled;
    }

    /// Whether `workItem`'s bloom filters and dictionaries may still drop it once the read reaches
    /// it: under a filter, in a row group statistics left undecided. One they proved to match in
    /// full is not probed, since neither a bloom filter nor a dictionary can contradict them, and a
    /// filter-only column is not read there at all.
    static boolean droppableOnArrival(WorkItem workItem) {
        return workItem.leafDecisions() != null && !workItem.filterAlwaysMatches();
    }

    /// Sharpens the decisions planning recorded with the row group's bloom filters, then with its
    /// dictionaries for every leaf they leave undecided.
    Arrival probeOnArrival(WorkItem workItem) throws IOException {
        RowGroupFilterEvaluator.LeafDecisions leafDecisions = workItem.leafDecisions();
        if (!workItem.bloomFilterColumns().isEmpty()) {
            RowGroupFilterEvaluator.LeafDecisions refined = new RowGroupFilterEvaluator.LeafDecisions();
            if (refineWithBloomFilters(workItem, refined) == FilterDecision.CANNOT_MATCH) {
                emitBloomFilterDropEvent(workItem);
                return Arrival.DROPPED;
            }
            leafDecisions = refined;
        }
        RowGroupDictionaryFilterSource dictionaries = new RowGroupDictionaryFilterSource(workItem.inputFile(),
                workItem.rowGroup(), workItem.fileSchema(), context);
        FilterDecision decision;
        try (FetchReason.Scope pruning = FetchReason.set("rg=" + workItem.rowGroupIndex() + " pruning")) {
            decision = RowGroupFilterEvaluator.refineWithDictionaries(
                    workItem.columnOrdinals().filter(), workItem.rowGroup(), leafDecisions, dictionaries);
        }
        if (decision == FilterDecision.CANNOT_MATCH) {
            emitDictionaryDropEvent(workItem);
            return Arrival.DROPPED;
        }
        return new Arrival(false, dictionaries);
    }

    /// What a row group's bloom filters and dictionaries decided once the read reached it.
    ///
    /// @param dropped whether they proved the row group holds no match
    /// @param dictionaries the dictionaries the probe read, for the fetch plans to decode with, or
    ///        `null` when it read none
    record Arrival(boolean dropped, RowGroupDictionaryFilterSource dictionaries) {

        static final Arrival KEPT = new Arrival(false, null);
        static final Arrival DROPPED = new Arrival(true, null);
    }

    /// Whether the read narrows `workItem`'s rows by the ColumnIndex: under a filter with metadata
    /// filtering on, in a row group statistics left undecided. In one they proved to match in full,
    /// no page bound can rule a page out, since every value of it matches.
    boolean pageFiltering(WorkItem workItem) {
        return filterPredicate != null && metadataFilteringEnabled && !workItem.filterAlwaysMatches();
    }

    /// The file ordinals of the columns whose ColumnIndex the read of `workItem` needs: the filter
    /// columns under page filtering, none otherwise.
    static BitSet columnIndexColumns(WorkItem workItem, boolean pageFiltering) {
        BitSet columns = new BitSet();
        if (pageFiltering) {
            ResolvedPredicate.collectColumnIndices(workItem.columnOrdinals().filter(), columns);
        }
        return columns;
    }

    /// Sharpens the work item's planned decision with its bloom filters: those its window fetched,
    /// and any other a probe reaches, read from the file.
    ///
    /// @param refined receives the leaf decisions after the bloom filters
    private FilterDecision refineWithBloomFilters(WorkItem workItem,
            RowGroupFilterEvaluator.LeafDecisions refined) throws IOException {
        RowGroupBloomFilterSource bloomFilters = new RowGroupBloomFilterSource(workItem.inputFile(),
                workItem.rowGroup(), workItem.indexWindow().bloomFiltersFor(workItem.workItemIndex()));
        try (FetchReason.Scope pruning = FetchReason.set("rg=" + workItem.rowGroupIndex() + " pruning")) {
            return RowGroupFilterEvaluator.refineWithBloomFilters(workItem.columnOrdinals().filter(),
                    workItem.rowGroup(), workItem.leafDecisions(), bloomFilters, refined);
        }
    }

    /// Reports a row group its bloom filters dropped once the read reached it.
    private static void emitBloomFilterDropEvent(WorkItem workItem) {
        RowGroupBloomFilterEvent event = new RowGroupBloomFilterEvent();
        event.file = workItem.inputFile().name();
        event.rowGroupIndex = workItem.rowGroupIndex();
        event.commit();
    }

    /// Reports a row group its dictionaries dropped once the read reached it.
    private static void emitDictionaryDropEvent(WorkItem workItem) {
        RowGroupDictionaryFilterEvent event = new RowGroupDictionaryFilterEvent();
        event.file = workItem.inputFile().name();
        event.rowGroupIndex = workItem.rowGroupIndex();
        event.commit();
    }

    /// A row group surviving predicate push-down, and whether its statistics prove
    /// every row matches (so per-row filtering can be skipped for it).
    /// A row group that survived pruning, with the index it has **in the file** — not its
    /// position in this list, which pruning makes a different number. Every consumer means
    /// the file's: the fetch log, the JFR events, the row group a failure names.
    record FilteredRowGroup(RowGroup rowGroup, boolean alwaysMatches,
            int fileRowGroupIndex, RowGroupFilterEvaluator.LeafDecisions leafDecisions,
            BitSet bloomFilterColumns) {}

    List<FilteredRowGroup> filterRowGroups(List<RowGroup> rowGroups, InputFile inputFile,
                                                   FileColumnOrdinals columnOrdinals) throws IOException {
        // The metadata-filtering opt-out (#797) disables every metadata-driven prune,
        // dictionary membership included — with it off, no row group is dropped without
        // reading rows.
        if (!prunesByMetadata()) {
            List<FilteredRowGroup> unpruned = new ArrayList<>(rowGroups.size());
            for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
                unpruned.add(new FilteredRowGroup(rowGroups.get(rgIndex), false, rgIndex, null,
                        new BitSet()));
            }
            return unpruned;
        }
        List<FilteredRowGroup> filtered = new ArrayList<>(rowGroups.size());
        int fullyMatching = 0;
        for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
            RowGroup rg = rowGroups.get(rgIndex);
            FilterDecision decision;
            RowGroupFilterEvaluator.LeafDecisions leafDecisions = new RowGroupFilterEvaluator.LeafDecisions();
            // Planning reads nothing: it decides on statistics and records the columns whose
            // bloom filter a leaf would probe. Bloom filters and dictionaries are read once the
            // read reaches the row group, in `computeSharedMetadata`.
            BloomFilterColumnRecorder bloomFilterColumns = new BloomFilterColumnRecorder(rg);
            decision = RowGroupFilterEvaluator.planRowGroup(columnOrdinals.filter(), rg,
                    bloomFilterColumns, new LogContext(inputFile.name(), rgIndex),
                    columnOrdinals.boundsReadability(), leafDecisions);
            if (decision == FilterDecision.CANNOT_MATCH) {
                continue;
            }
            boolean alwaysMatches = decision == FilterDecision.ALWAYS_MATCHES;
            if (alwaysMatches) {
                fullyMatching++;
            }
            // A row group statistics proved to match in full is not probed on entry, so it
            // fetches no bloom filter, although an open leaf before the deciding one asked for one.
            filtered.add(new FilteredRowGroup(rg, alwaysMatches, rgIndex, leafDecisions,
                    alwaysMatches ? new BitSet() : bloomFilterColumns.columns()));
        }

        RowGroupFilterEvent event = new RowGroupFilterEvent();
        event.file = inputFile.name();
        event.totalRowGroups = rowGroups.size();
        event.rowGroupsKept = filtered.size();
        event.rowGroupsSkipped = rowGroups.size() - filtered.size();
        event.rowGroupsFullyMatching = fullyMatching;
        event.commit();

        return filtered;
    }
}
