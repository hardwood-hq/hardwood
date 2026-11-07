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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.Sections;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.FileMetadataCache.ClaimedLoad;
import dev.hardwood.internal.reader.plan.FileMetadataCache.PreparedFile;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.jfr.PageFilterEvent;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.Event;

/// The planning of a read for a driver outside this package: the footers of its files, which the
/// reader's [FileMetadataCache] holds for every read of the reader, the work list, which the
/// driver lists a file at a time, and each work item's stages, which [Planner] decides and the
/// driver runs one at a time, in work-list order. Every byte planning reads comes from the
/// driver's [Sections].
///
/// A footer the read needs is claimed from the cache ([#claimFooter]): a footer loaded, or whose
/// load failed, is taken as it stands, a load another read runs is waited for, and otherwise the
/// driver runs the load itself, its read ([#readFooter]) and then its parse ([#parseFooter]),
/// which completes the cache's entry. A footer's failure is kept and raised by the listing of its
/// file ([#listFile]).
public final class ReadPlanning {

    static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";

    /// The files after the one a read has reached, the next one included, whose footers it reads
    /// ahead without the override: a bound on the footer reads a read has in flight for files it
    /// has not reached. A read of many small files, whose data never fills F, is bound by round
    /// trips, and overlaps as many files as this lets it; a read of larger files fills F first,
    /// and its footers wait behind its data.
    static final int DEFAULT_MAX_FILES_AHEAD = 64;

    private final FileMetadataCache files;
    private final Planner planner;
    private final WorkingSet workingSet;
    /// The work items listed and not released, by index.
    private final Map<Integer, Planning> plannings = new ConcurrentHashMap<>();
    /// The loads the driver claimed and has not completed, by file.
    private final Map<Integer, ClaimedLoad> loads = new ConcurrentHashMap<>();
    /// The footers loaded and not yet listed, by file.
    private final Map<Integer, PreparedFile> prepared = new ConcurrentHashMap<>();
    /// The failures of the footers that could not be loaded, by file.
    private final Map<Integer, Throwable> failed = new ConcurrentHashMap<>();
    /// The [dev.hardwood.jfr.RowGroupFilterEvent] of each file listed, until the read reaches it.
    /// Guarded by the driver.
    private final TreeMap<Integer, Event> fileEvents = new TreeMap<>();
    /// The loads of other reads a claim found under way, by file, until the driver registers for
    /// their end ([#whenFooterDone]).
    private final Map<Integer, CompletableFuture<?>> pending = new ConcurrentHashMap<>();

    /// @param filter the read's resolved filter, `null` for none
    /// @param metadataFiltering whether the filter takes metadata-derived shortcuts: row-group
    ///        pruning by statistics, Bloom filters and dictionaries, page skipping and the always-match
    ///        decision
    /// @param maxRows the read's row budget, `0` for none: the listing stops once it is covered, and
    ///        the plans of an unfiltered read truncate the pages past it
    /// @param physicalSkip the leading rows of the read whose row groups the listing drops; the
    ///        residue within the first work item kept is [#firstRowGroupSkip]
    public ReadPlanning(FileMetadataCache files, Sections sections, HardwoodContextImpl context,
                          FileSchema referenceSchema, List<RowGroup> firstFileRowGroups, ReadProjection projection,
                          ResolvedPredicate filter, boolean metadataFiltering, long maxRows, long physicalSkip,
                          int maxSpan, long indexWindowBytes, WorkingSet workingSet) throws IOException {
        this.files = files;
        this.workingSet = workingSet;
        SchemaCompatibility compatibility = new SchemaCompatibility(referenceSchema, projection.decoded(), filter);
        compatibility.validateReferenceColumns(files.inputFiles().getFirst().name());
        this.planner = new Planner(sections, referenceSchema, firstFileRowGroups, projection, filter, compatibility,
                new RowGroupPruning(context, filter, metadataFiltering),
                new FetchPlanBuilder(projection, filter, metadataFiltering, context, workingSet, maxSpan),
                maxRows, physicalSkip, 0, indexWindowBytes);
        prepared.put(0, files.getFile(0));
    }

    /// The files after the one it has reached whose footers a read of `fileCount` files reads
    /// ahead: up to [#maxFilesAhead] for a multi-file read that plans deep (its files are not all
    /// in memory and it has no row limit), and otherwise only the next file: a read of files all in
    /// memory, whose reads wait for nothing to overlap, or with a row limit, which may need no
    /// further file and under a filter learns only downstream that it is satisfied.
    public static int filesAhead(boolean plansDeep, int fileCount) {
        return fileCount < 2 || !plansDeep ? 1 : maxFilesAhead();
    }

    /// The files after the one it has reached a read reads footers of ahead:
    /// [#DEFAULT_MAX_FILES_AHEAD] unless overridden through [#MAX_FILES_AHEAD_PROPERTY], read for
    /// each read. A value that is not a positive count is rejected rather than replaced by the
    /// default; `1` reads only the next file's footer ahead.
    private static int maxFilesAhead() {
        return InternalProperty.intValue(MAX_FILES_AHEAD_PROPERTY, DEFAULT_MAX_FILES_AHEAD, 1, "a positive count");
    }

    /// Whether a [dev.hardwood.MetadataSource] supplied the footers rather than each being read
    /// from its file.
    public boolean footersSupplied() {
        return files.footersSupplied();
    }

    // ==================== Footers ====================

    /// What the driver does about the footer of a file.
    public enum FooterState {
        /// The footer is loaded, or its load failed: the file can be listed.
        LOADED,
        /// Another read loads the footer; the driver asks again once it is done.
        PENDING,
        /// The driver loads the footer: [#readFooter] unless [#footerReadsInPlace], then
        /// [#parseFooter].
        CLAIMED
    }

    /// Claims the footer of `fileIndex` for the read ([FooterState]).
    public FooterState claimFooter(int fileIndex) {
        if (prepared.containsKey(fileIndex) || failed.containsKey(fileIndex)) {
            return FooterState.LOADED;
        }
        FileMetadataCache.Claim claim = files.claim(fileIndex);
        if (claim.file() != null) {
            prepared.put(fileIndex, claim.file());
            return FooterState.LOADED;
        }
        if (claim.failure() != null) {
            failed.put(fileIndex, claim.failure());
            return FooterState.LOADED;
        }
        if (claim.pending() != null) {
            pending.put(fileIndex, claim.pending());
            return FooterState.PENDING;
        }
        loads.put(fileIndex, claim.load());
        return FooterState.CLAIMED;
    }

    /// Runs `resume` once the load of another read that the claim of `fileIndex` found
    /// ([FooterState#PENDING]) is done: on the thread that completes it, or at once on the caller's.
    /// Called without the driver's lock, which `resume` may take.
    public void whenFooterDone(int fileIndex, Runnable resume) {
        pending.remove(fileIndex).whenComplete((ignored, failure) -> resume.run());
    }

    /// Whether the claimed footer of `fileIndex` waits for no I/O, so its parse reads it.
    public boolean footerReadsInPlace(int fileIndex) {
        return loads.get(fileIndex).readsInPlace();
    }

    /// Reads the claimed footer of `fileIndex` as far as I/O goes, on an I/O thread.
    ///
    /// @throws IOException the read's failure, naming the file
    public void readFooter(int fileIndex) throws IOException {
        loads.get(fileIndex).read();
    }

    /// Parses the claimed footer of `fileIndex`, on a CPU thread, completing its load. A failure is
    /// kept for the listing of the file.
    ///
    /// @return the `Error` the parse failed with, which the read raises at its close unless the
    ///         listing of the file raises it; `null` for none
    public Error parseFooter(int fileIndex) {
        ClaimedLoad load = loads.get(fileIndex);
        Error erred = null;
        try {
            prepared.put(fileIndex, load.parse());
        }
        catch (IOException | RuntimeException e) {
            failed.put(fileIndex, e);
        }
        catch (Error e) {
            failed.put(fileIndex, e);
            erred = e;
        }
        loads.remove(fileIndex);
        return erred;
    }

    /// Takes the claimed load of `fileIndex` as failed with `failure`, its read's last, which the
    /// listing of the file raises; returns the completion of the load, which the driver runs once
    /// it has let go of its lock, since it runs what other reads wait for.
    public Runnable failFooter(int fileIndex, Throwable failure) {
        ClaimedLoad load = loads.remove(fileIndex);
        failed.put(fileIndex, failure);
        return () -> load.fail(failure);
    }

    /// Gives up the claimed load of `fileIndex`, which failed ahead of demand, so that the reader's
    /// other reads load it afresh and the read claims it again once it needs the file; returns the
    /// giving up, which the driver runs once it has let go of its lock.
    public Runnable abandonFooter(int fileIndex) {
        ClaimedLoad load = loads.remove(fileIndex);
        return load::abandon;
    }

    /// Gives up the loads claimed and not completed, as the read closes; returns the giving up,
    /// which the driver runs once it has let go of its lock.
    public Runnable abandonFooters() {
        List<ClaimedLoad> abandoned = new ArrayList<>(loads.values());
        loads.clear();
        return () -> abandoned.forEach(ClaimedLoad::abandon);
    }

    /// Drops the tail of `fileIndex` and, but for the first file's, its footer from the reader's
    /// cache, once the read is done with the file or will not reach it.
    public void dropFile(int fileIndex) {
        prepared.remove(fileIndex);
        files.drop(fileIndex);
    }

    // ==================== The work list ====================

    /// The work items of one file, as [#listFile] planned them, for the driver to append.
    public static final class FileListing {

        private final Planner.Listed listed;
        private final int fileIndex;

        private FileListing(Planner.Listed listed, int fileIndex) {
            this.listed = listed;
            this.fileIndex = fileIndex;
        }
    }

    /// Plans the work items of `fileIndex`, whose footer is loaded or failed, after the `listedBefore`
    /// work items of the files before it. Never on two threads at once, nor with [#plan].
    ///
    /// @throws IOException the failure of the file's footer, or of its planning: a schema that does
    ///         not match the first file's, among others
    public FileListing listFile(int fileIndex, int listedBefore) throws IOException {
        Throwable failure = failed.get(fileIndex);
        if (failure != null) {
            throw rethrow(failure);
        }
        PreparedFile file = prepared.remove(fileIndex);
        return new FileListing(planner.planRowGroups(fileIndex, file, listedBefore), fileIndex);
    }

    /// Appends `listing` to the work list; returns how many work items it holds. Under the driver's
    /// lock.
    public int append(FileListing listing) {
        for (Planning planning : listing.listed.plannings()) {
            plannings.put(planning.workItem.workItemIndex(), planning);
        }
        if (listing.listed.event() != null) {
            fileEvents.put(listing.fileIndex, listing.listed.event());
        }
        return listing.listed.plannings().size();
    }

    /// Commits the [dev.hardwood.jfr.RowGroupFilterEvent]s of the files listed up to `fileIndex`,
    /// which the read has reached. Under the driver's lock.
    public void reportFiles(int fileIndex) {
        while (!fileEvents.isEmpty() && fileEvents.firstKey() <= fileIndex) {
            fileEvents.pollFirstEntry().getValue().commit();
        }
    }

    /// Whether the read lists no further file: the row budget is spent.
    public boolean budgetLeft() {
        return planner.budgetLeft();
    }

    public void setBatchPlan(BatchPlan batchPlan) {
        planner.setBatchPlan(batchPlan);
    }

    /// Sets the leading rows of the first work item its plans mask out, the tail read's skip.
    ///
    /// @throws IllegalStateException if a work item's batches are planned already
    public void setTailSkip(long tailSkip) {
        if (planner.batchesPlanned()) {
            throw new IllegalStateException("setTailSkip must be called before any column requests its fetch plan");
        }
        planner.setTailSkip(tailSkip);
    }

    /// The residue of the physical skip within the first work item, once it is listed.
    public long firstRowGroupSkip() {
        return planner.firstRowGroupSkip();
    }

    /// Takes work item `index` through its stages as far as the bytes the read holds allow:
    /// `null` once its plans are made, else what the next stage lacks. Run again once it has
    /// arrived; never on two threads at once, nor with [#listFile].
    public Need plan(int index) throws IOException {
        return planner.plans(plannings.get(index));
    }

    /// Takes work item `index` through its Bloom filters and dictionaries as far as the bytes the
    /// read holds allow: `null` once decided ([#dropped]), else what it lacks. Run again once it has
    /// arrived; never on two threads at once, nor with [#listFile] or [#plan].
    public Need arrival(int index) throws IOException {
        return planner.arrival(plannings.get(index));
    }

    /// Takes work item `index` through its page index and, unless its Bloom filters or dictionaries
    /// dropped it, its mask probe, as far as the bytes the read holds allow: `null` once decided
    /// ([#maskClosed]), else what it lacks. Run again once it has arrived; never on two threads at
    /// once, nor with [#listFile] or [#plan].
    public Need masked(int index) throws IOException {
        return planner.masked(plannings.get(index));
    }

    /// Whether work item `index` is kept and a decoded column of it cannot take a page mask, once
    /// [#masked] decided.
    public boolean maskClosed(int index) {
        Planning planning = plannings.get(index);
        return !planning.shared.dropped() && planning.mask == MaskCapability.NO;
    }

    /// Whether work item `index`'s Bloom filters or dictionaries dropped it, once [#arrival] decided.
    public boolean dropped(int index) {
        return plannings.get(index).arrival.dropped();
    }

    /// Commits the drop of work item `index` by its Bloom filters or dictionaries, once, when the
    /// read reaches it, however far ahead its planning ran. Under the driver's lock.
    public void report(int index) {
        Planning planning = plannings.get(index);
        RowGroupPruning.Arrival arrival = planning != null ? planning.arrival : null;
        if (arrival != null && arrival.dropEvent() != null && planning.reported.compareAndSet(false, true)) {
            arrival.dropEvent().commit();
        }
    }

    /// Records that the read enters work item `index`, once planned: the first time, commits its
    /// drop's event and the [PageFilterEvent]s its plans decided. Under the driver's lock.
    public void enterUnit(int index) {
        Planning planning = plannings.get(index);
        if (!planning.entered.compareAndSet(false, true)) {
            return;
        }
        report(index);
        for (PageFilterEvent event : planning.plans.pageFilterEvents()) {
            event.commit();
        }
    }

    public WorkItem workItem(int index) {
        return plannings.get(index).workItem;
    }

    /// The fetch plan of each decoded column of work item `index`, once planned.
    public FetchPlan[] plans(int index) {
        return plannings.get(index).plans.columns();
    }

    /// The batches of work item `index`, once planned.
    public BatchPlan.RowGroupBatches batches(int index) {
        return plannings.get(index).plans.batches();
    }

    /// Tells the working set that decoded column `column` enters work item `index`, once planned,
    /// so that the dictionaries its batches hold are counted from where the batch plan places the
    /// row group ([WorkingSet#enter]).
    public void enter(int column, int index) {
        BatchPlan.RowGroupBatches batches = batches(index);
        workingSet.enter(column, batches.batchStartRow(), batches.readRowsBefore() + batches.plannedRows());
    }

    /// Releases what work item `index`'s planning holds, once every column has passed it.
    public void release(int index) {
        Planning planning = plannings.remove(index);
        planning.reads.release();
        planning.workItem.indexWindow().release(index);
    }

    /// Releases what every work item not released holds, as the read closes.
    public void discard() {
        for (Iterator<Planning> it = plannings.values().iterator(); it.hasNext(); ) {
            Planning planning = it.next();
            it.remove();
            planning.workItem.indexWindow().releaseAll();
            planning.reads.discard();
        }
    }

    private static IOException rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException io) {
            throw io;
        }
        if (failure instanceof RuntimeException re) {
            throw re;
        }
        if (failure instanceof Error err) {
            throw err;
        }
        throw new IOException(failure);
    }
}
