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
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.BloomFilterColumnRecorder;
import dev.hardwood.internal.predicate.DictionaryColumnRecorder;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.internal.predicate.dictionary.RowGroupDictionaryFilterSource;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.jfr.RowGroupBloomFilterEvent;
import dev.hardwood.jfr.RowGroupDictionaryFilterEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import jdk.jfr.Event;

/// The pruning of a read's row groups by metadata: by statistics when a file is planned
/// ([#filterRowGroups]), by Bloom filters and dictionaries once the read reaches a row group
/// ([#read], [#evaluate]), and whether the read narrows a row group's rows by the ColumnIndex
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

    /// The columns whose dictionary `unit`'s probe reads: those of fully dictionary-encoded chunks
    /// that evaluating its leaves left open by statistics may consult, as a dry run of the
    /// evaluation finds them ([DictionaryColumnRecorder]); none for a row group not probed.
    public static BitSet dictionaryColumns(UnitListing unit) throws IOException {
        BitSet columns = new BitSet();
        if (!droppableOnArrival(unit)) {
            return columns;
        }
        DictionaryColumnRecorder recorder = new DictionaryColumnRecorder();
        RowGroupFilterEvaluator.refineWithDictionaries(unit.columnOrdinals().filter(), unit.metaData(),
                unit.leafDecisions(), recorder);
        BitSet asked = recorder.columns();
        for (int c = asked.nextSetBit(0); c >= 0; c = asked.nextSetBit(c + 1)) {
            if (c < unit.metaData().columns().size() && RowGroupDictionaryFilterSource.isFullyDictionaryEncoded(
                    unit.metaData().columns().get(c).metaData())) {
                columns.set(c);
            }
        }
        return columns;
    }

    /// The dictionary pages of `unit`'s probe ([#dictionaryColumns]) whose extent its footer gives,
    /// from `dictionary_page_offset` to `data_page_offset` within the chunk and the file, which the
    /// read requests with the row group's Bloom filters; the probe reads any other header first,
    /// and a chunk whose extent does not check out fails its probe as it reaches it.
    public static List<FileRange> dictionaryRanges(UnitListing unit) throws IOException {
        List<FileRange> ranges = new ArrayList<>();
        BitSet columns = dictionaryColumns(unit);
        for (int c = columns.nextSetBit(0); c >= 0; c = columns.nextSetBit(c + 1)) {
            ColumnChunk chunk = unit.metaData().columns().get(c);
            ColumnMetaData metaData = chunk.metaData();
            Long start = metaData.dictionaryPageOffset();
            long chunkEnd = chunk.chunkStartOffset() + metaData.totalCompressedSize();
            if (start != null && start > 0 && start >= chunk.chunkStartOffset() && metaData.dataPageOffset() > start
                    && metaData.dataPageOffset() - start <= Integer.MAX_VALUE
                    && metaData.dataPageOffset() <= chunkEnd && chunkEnd <= unit.file().length()) {
                ranges.add(new FileRange(start, Math.toIntExact(metaData.dataPageOffset() - start),
                        "rg=" + unit.rowGroup() + " pruning"));
            }
        }
        return ranges;
    }

    /// The first step of `unit`'s probe, on a job: reads, from what `bytes` holds, the row group's
    /// Bloom filters and the dictionary pages its evaluation may consult ([#dictionaryColumns]),
    /// their headers parsed, without evaluating any. What `bytes` lacks, a filter or page read
    /// header first, it records missing, and the caller reads again once that has arrived; the
    /// probe is complete once a read records nothing.
    public void read(UnitListing unit, ByteSource bytes, Probe probe) throws IOException {
        if (probe.columns == null) {
            probe.columns = dictionaryColumns(unit);
            probe.bloomFilters = new RowGroupBloomFilterSource(unit.file(), bytes, unit.metaData());
            probe.dictionaries = new RowGroupDictionaryFilterSource(unit.file(), bytes, unit.metaData(),
                    unit.fileSchema(), context);
        }
        for (int c = unit.bloomFilterColumns().nextSetBit(0); c >= 0; c = unit.bloomFilterColumns().nextSetBit(c + 1)) {
            probe.bloomFilters.forColumn(c);
        }
        for (int c = probe.columns.nextSetBit(0); c >= 0; c = probe.columns.nextSetBit(c + 1)) {
            probe.dictionaries.page(c);
        }
    }

    /// The buffers [#evaluate] holds while it runs, which R is charged: those of each dictionary
    /// parse that exceeds `DecodeBuffers.RETAINED_BYTES`, from the headers [#read] parsed; under the
    /// read's lock.
    public static long parseBytes(UnitListing unit, Probe probe) {
        long bytes = 0;
        for (int c = probe.columns.nextSetBit(0); c >= 0; c = probe.columns.nextSetBit(c + 1)) {
            DataPage page = probe.dictionaries.readAhead(c);
            long task = page == null ? 0 : DataPage.taskBytes(page.header(), unit.fileSchema().getColumn(c));
            bytes += task > DecodeBuffers.RETAINED_BYTES ? task : 0;
        }
        return bytes;
    }

    /// The second and last step of `unit`'s probe, on a job charged [#parseBytes], once [#read]
    /// has read everything: evaluates the Bloom filters, then the dictionaries for the leaves they
    /// leave open, decoding each a leaf consults.
    public Arrival evaluate(UnitListing unit, Probe probe) throws IOException {
        RowGroupFilterEvaluator.LeafDecisions leafDecisions = unit.leafDecisions();
        if (!unit.bloomFilterColumns().isEmpty()) {
            RowGroupFilterEvaluator.LeafDecisions refined = new RowGroupFilterEvaluator.LeafDecisions();
            if (RowGroupFilterEvaluator.refineWithBloomFilters(unit.columnOrdinals().filter(), unit.metaData(),
                    unit.leafDecisions(), probe.bloomFilters, refined) == FilterDecision.CANNOT_MATCH) {
                return new Arrival(true, bloomFilterDropEvent(unit));
            }
            leafDecisions = refined;
        }
        probe.dictionaries.readsEnded();
        if (RowGroupFilterEvaluator.refineWithDictionaries(unit.columnOrdinals().filter(), unit.metaData(),
                leafDecisions, probe.dictionaries) == FilterDecision.CANNOT_MATCH) {
            return new Arrival(true, dictionaryDropEvent(unit));
        }
        return Arrival.KEPT;
    }

    /// What one row group's probe keeps between its steps: its Bloom filters, and the dictionaries
    /// its evaluation may consult, read and then decoded.
    public static final class Probe {

        private BitSet columns;
        private RowGroupBloomFilterSource bloomFilters;
        private RowGroupDictionaryFilterSource dictionaries;

        /// The dictionaries the probe decoded, without what it read to decode them, for the row
        /// group's plan to decode with; `null` when it read none.
        public RowGroupDictionaryFilterSource.DecodedDictionaries decoded() {
            return dictionaries == null ? null : dictionaries.decoded();
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
