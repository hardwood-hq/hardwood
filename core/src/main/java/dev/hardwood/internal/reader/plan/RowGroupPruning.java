/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BloomFilterColumnRecorder;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.jfr.RowGroupBloomFilterEvent;
import dev.hardwood.jfr.RowGroupDictionaryFilterEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import jdk.jfr.Event;

/// The pruning of a read's row groups by metadata: by statistics when a file is planned
/// ([#filterRowGroups]), by Bloom filters and dictionaries once the read reaches a row group
/// ([#probeOnArrival]), and whether the read narrows a row group's rows by the ColumnIndex
/// ([#pageFiltering]).
public final class RowGroupPruning {

    private final HardwoodContextImpl context;
    private final ResolvedPredicate filterPredicate;
    private final boolean metadataFilteringEnabled;

    public RowGroupPruning(HardwoodContextImpl context, ResolvedPredicate filterPredicate, boolean metadataFilteringEnabled) {
        this.context = context;
        this.filterPredicate = filterPredicate;
        this.metadataFilteringEnabled = metadataFilteringEnabled;
    }

    /// Whether the read prunes by metadata: under a filter, with metadata filtering on.
    public boolean prunesByMetadata() {
        return filterPredicate != null && metadataFilteringEnabled;
    }

    /// Whether `unit`'s bloom filters and dictionaries may still drop it once the read reaches
    /// it: under a filter, in a row group statistics left undecided. One they proved to match in
    /// full is not probed, since neither a bloom filter nor a dictionary can contradict them, and a
    /// filter-only column is not read there at all.
    public static boolean droppableOnArrival(UnitListing unit) {
        return unit.leafDecisions() != null && !unit.alwaysMatches();
    }

    /// Sharpens the decisions planning recorded with the row group's bloom filters, then with its
    /// dictionaries for every leaf they leave undecided, over the bytes the read holds: what
    /// `bloomFilters` holds of the filters, and what `bytes` holds of dictionary pages. A filter or
    /// dictionary not held counts as absent; the caller sees from `bytes` whether any was, and only
    /// a drop then stands. `probe` keeps the dictionaries read and the leaves decided across evaluations.
    public Arrival probeOnArrival(UnitListing unit, ByteSource bloomFilters, ByteSource bytes, Probe probe)
            throws IOException {
        RowGroupFilterEvaluator.LeafDecisions leafDecisions = unit.leafDecisions();
        if (!unit.bloomFilterColumns().isEmpty()) {
            RowGroupFilterEvaluator.LeafDecisions refined = new RowGroupFilterEvaluator.LeafDecisions();
            RowGroupBloomFilterSource filters = new RowGroupBloomFilterSource(unit.file(), bloomFilters,
                    unit.metaData());
            if (RowGroupFilterEvaluator.refineWithBloomFilters(unit.columnOrdinals().filter(),
                    unit.metaData(), unit.leafDecisions(), filters, refined) == FilterDecision.CANNOT_MATCH) {
                return new Arrival(true, bloomFilterDropEvent(unit));
            }
            leafDecisions = refined;
        }
        if (probe.dictionaries == null) {
            probe.dictionaries = new RowGroupDictionaryFilterSource(unit.file(), bytes,
                    unit.metaData(), unit.fileSchema(), context);
        }
        if (RowGroupFilterEvaluator.refineWithDictionaries(unit.columnOrdinals().filter(), unit.metaData(),
                leafDecisions, probe.dictionaries, probe.decided) == FilterDecision.CANNOT_MATCH) {
            return new Arrival(true, dictionaryDropEvent(unit));
        }
        return Arrival.KEPT;
    }

    /// The dictionary pages the next pass of `unit`'s probe may parse: those of the filter's
    /// columns whose header `held` holds and whose dictionary `probe` has not read. A header that
    /// does not parse is left out: the pass fails on it.
    public static List<HeldDictionary> dictionariesToParse(UnitListing unit, ByteSource held, Probe probe) {
        BitSet columns = new BitSet();
        ResolvedPredicate.collectColumnIndices(unit.columnOrdinals().filter(), columns);
        List<HeldDictionary> dictionaries = new ArrayList<>();
        for (int c = columns.nextSetBit(0); c >= 0; c = columns.nextSetBit(c + 1)) {
            if (probe.dictionaries == null || !probe.dictionaries.decided(c)) {
                PageHeader header = heldHeader(unit.metaData().columns().get(c), held);
                if (header != null) {
                    dictionaries.add(new HeldDictionary(unit.fileSchema().getColumn(c), header));
                }
            }
        }
        return dictionaries;
    }

    /// A dictionary page a probe holds: its column's schema and the page's header.
    public record HeldDictionary(ColumnSchema schema, PageHeader header) {
    }

    /// The header of `chunk`'s dictionary page where `held` holds it, else `null`.
    private static PageHeader heldHeader(ColumnChunk chunk, ByteSource held) {
        try {
            Parsed<PageHeader> header = DictionaryParser.readPageHeader(held, chunk, "");
            return header.needsBytes() ? null : header.value();
        }
        catch (IOException | RuntimeException e) {
            return null;
        }
    }

    /// What the passes of one row group's probe on arrival keep between them: the dictionaries
    /// read, and each leaf's decision once a dictionary has made it.
    public static final class Probe {

        private RowGroupDictionaryFilterSource dictionaries;
        private final RowGroupFilterEvaluator.LeafDecisions decided = new RowGroupFilterEvaluator.LeafDecisions();

        /// The dictionaries the probe read, for the row group's plan to decode with; `null` when it
        /// read none.
        public RowGroupDictionaryFilterSource dictionaries() {
            return dictionaries;
        }
    }

    /// What a row group's bloom filters and dictionaries decided once the read reached it.
    ///
    /// @param dropped whether they proved the row group holds no match
    /// @param dropEvent the event reporting the drop, for the read to commit once it reaches the
    ///        row group; `null` when they kept it
    public record Arrival(boolean dropped, Event dropEvent) {

        public static final Arrival KEPT = new Arrival(false, null);
    }

    /// Whether the read narrows `unit`'s rows by the ColumnIndex: under a filter with metadata
    /// filtering on, in a row group statistics left undecided. In one they proved to match in full,
    /// no page bound can rule a page out, since every value of it matches.
    public boolean pageFiltering(UnitListing unit) {
        return filterPredicate != null && metadataFilteringEnabled && !unit.alwaysMatches();
    }

    /// The file ordinals of the columns whose ColumnIndex the read of `unit` needs: the filter
    /// columns under page filtering, none otherwise.
    public static BitSet columnIndexColumns(UnitListing unit, boolean pageFiltering) {
        BitSet columns = new BitSet();
        if (pageFiltering) {
            ResolvedPredicate.collectColumnIndices(unit.columnOrdinals().filter(), columns);
        }
        return columns;
    }

    /// The report of a row group its bloom filters dropped.
    private static Event bloomFilterDropEvent(UnitListing unit) {
        RowGroupBloomFilterEvent event = new RowGroupBloomFilterEvent();
        event.file = unit.file().name();
        event.rowGroupIndex = unit.rowGroup();
        return event;
    }

    /// The report of a row group its dictionaries dropped.
    private static Event dictionaryDropEvent(UnitListing unit) {
        RowGroupDictionaryFilterEvent event = new RowGroupDictionaryFilterEvent();
        event.file = unit.file().name();
        event.rowGroupIndex = unit.rowGroup();
        return event;
    }

    /// A row group surviving predicate push-down, and whether its statistics prove
    /// every row matches (so per-row filtering can be skipped for it).
    /// A row group that survived pruning, with the index it has **in the file** — not its
    /// position in this list, which pruning makes a different number. Every consumer means
    /// the file's: the fetch log, the JFR events, the row group a failure names.
    public record FilteredRowGroup(RowGroup rowGroup, boolean alwaysMatches,
            int fileRowGroupIndex, RowGroupFilterEvaluator.LeafDecisions leafDecisions,
            BitSet bloomFilterColumns) {}

    /// The row groups of a file that pruning by statistics keeps, and the [RowGroupFilterEvent]
    /// reporting it, for the read to commit once it reaches the file; `null` when the read does not
    /// prune by metadata.
    public record FilteredRowGroups(List<FilteredRowGroup> rowGroups, Event event) {
    }

    public FilteredRowGroups filterRowGroups(List<RowGroup> rowGroups, InputFile inputFile,
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
            return new FilteredRowGroups(unpruned, null);
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
        return new FilteredRowGroups(filtered, event);
    }
}
