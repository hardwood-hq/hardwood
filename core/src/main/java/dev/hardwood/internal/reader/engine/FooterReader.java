/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.List;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.RowGroupPruning;
import dev.hardwood.internal.reader.plan.SchemaCompatibility;
import dev.hardwood.schema.FileSchema;

/// Reads the footers of a read's files through the reader's [FileMetadataCache], which every
/// read of the reader shares: a footer is claimed, its load waited for where another read runs it,
/// or read on an I/O job and parsed on a CPU job. Files are listed in order, each against the first
/// file's schema, and their row groups handed to [RowGroupPlanner].
///
/// Footers start at most `filesAhead` files past the one the consumer's need reaches, and a file the consumer has
/// passed is dropped from the cache. A footer that cannot be loaded fails the read at its file's
/// first row, which the consumer meets only where it reaches it; a failed read is given up in the
/// cache, so that the next read of the file reads it again. Used under the read's lock, except the
/// jobs' work.
final class FooterReader {

    static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";

    /// The files after the one a read has reached, the next one included, whose footers it reads
    /// ahead without the override: a bound on the footer reads a read has in flight for files it
    /// has not reached. A read of many small files, whose data never fills F, is bound by round
    /// trips, and overlaps as many files as this lets it; a read of larger files fills F first,
    /// and its footers wait behind its data.
    static final int DEFAULT_MAX_FILES_AHEAD = 64;

    private final FileMetadataCache files;
    private final JobScheduler scheduler;
    private final RowGroupPlanner planner;
    private final BatchPublisher publisher;
    private final SchemaCompatibility compatibility;
    private final ResolvedPredicate filter;
    /// applies a result under the read's lock: the end of another read's load of a footer
    private final Consumer<Runnable> results;
    private final int count;
    private final int filesAhead;
    /// per file, its footer once started, until the consumer passes the file
    private final FooterLoad[] footers;
    private int nextList;
    private int dropped;
    /// once set, another read's load ending starts nothing: the read is done with its footers
    private boolean closed;

    FooterReader(FileMetadataCache files, JobScheduler scheduler, RowGroupPlanner planner, BatchPublisher publisher,
                 SchemaCompatibility compatibility, ResolvedPredicate filter, boolean limited,
                 Consumer<Runnable> results) {
        this.files = files;
        this.scheduler = scheduler;
        this.planner = planner;
        this.publisher = publisher;
        this.compatibility = compatibility;
        this.filter = filter;
        this.results = results;
        List<InputFile> inputs = files.inputFiles();
        this.count = inputs.size();
        this.filesAhead = filesAhead(limited, inputs.stream().allMatch(HeldBytes::inMemory), count);
        this.footers = new FooterLoad[count];
    }

    /// One file's footer as the read has it: its load while the read runs it, what that load gave,
    /// and what it holds in R, its tail, until the consumer passes the file.
    private static final class FooterLoad {
        private FileMetadataCache.ClaimedLoad claimed;
        private FileMetadataCache.PreparedFile file;
        private Throwable failure;
        private Charge charge;
        /// the job reading or parsing the claimed load, until it has run
        private JobScheduler.Job job;

        /// Whether its file or its failure is in.
        boolean done() {
            return file != null || failure != null;
        }

        /// The load this read claimed, until it completes or is given up; `null` otherwise.
        FileMetadataCache.ClaimedLoad claimed() {
            return claimed;
        }

        /// This read runs `load`.
        void claim(FileMetadataCache.ClaimedLoad load) {
            claimed = load;
        }

        FileMetadataCache.PreparedFile file() {
            return file;
        }

        Throwable failure() {
            return failure;
        }

        /// The load ended with `file`, or with `failure`; the claim and the job are done.
        void loaded(FileMetadataCache.PreparedFile file, Throwable failure) {
            claimed = null;
            job = null;
            this.file = file;
            this.failure = failure;
        }

        /// Listing the file failed with `failure`.
        void failed(Throwable failure) {
            this.failure = failure;
        }

        /// The job reading or parsing the load, until it has run.
        JobScheduler.Job job() {
            return job;
        }

        void job(JobScheduler.Job job) {
            this.job = job;
        }

        /// The read gave up the load before its job started.
        void abandoned() {
            claimed = null;
            job = null;
        }

        /// The charge its tail holds in R from its read, `null` where none.
        Charge charge() {
            return charge;
        }

        void charge(Charge charge) {
            this.charge = charge;
        }

        /// Hands back the charge its tail holds: returns it, `null` where none, and holds none from
        /// then on.
        Charge takeCharge() {
            Charge taken = charge;
            charge = null;
            return taken;
        }
    }

    /// The files past the one it has reached whose footers a read of `fileCount` files reads ahead:
    /// [#DEFAULT_MAX_FILES_AHEAD] unless overridden through [#MAX_FILES_AHEAD_PROPERTY], for a
    /// multi-file read of files not all in memory and without a row limit; otherwise only the next
    /// file: a read of files all in memory waits for nothing to overlap, and one with a row limit
    /// may need no further file. A property value that is not a positive count is rejected.
    private static int filesAhead(boolean limited, boolean allInMemory, int fileCount) {
        return fileCount < 2 || allInMemory || limited ? 1
                : InternalProperty.intValue(MAX_FILES_AHEAD_PROPERTY, DEFAULT_MAX_FILES_AHEAD, 1, "a positive count");
    }

    /// Lists the first file, `file` as its footer's load left it, with `schema`, the row groups its
    /// statistics keep, `kept`, and `ordinals`, and starts the footers ahead of it.
    void start(InputFile file, FileSchema schema, RowGroupPruning.FilteredRowGroups kept, FileColumnOrdinals ordinals) {
        planner.addFile(0, file, schema, kept, ordinals);
        footers[0] = new FooterLoad();
        nextList = 1;
        if (count == 1 || !planner.budgetLeft()) {
            nextList = count;
            planner.listed();
        }
        advance(0, 1);
    }

    /// Takes up the consumer's move to `key`, its first row not published, with the read reaching
    /// `reach`: the footers up to `filesAhead` files past the file that row lies in start, and the
    /// files before the consumer's are dropped.
    void advance(long key, long reach) {
        int at = planner.fileAt(key, true);
        int needed = planner.fileAt(reach - 1, false);
        int reached = needed < 0 ? nextList : needed;
        for (int i = nextList; i < count && i <= reached + filesAhead && planner.budgetLeft(); i++) {
            if (footers[i] == null) {
                footers[i] = new FooterLoad();
                startFooter(i);
            }
        }
        for (; dropped < (at < 0 ? nextList : at); dropped++) {
            files.drop(dropped);
            returnCharge(footers[dropped]);
            footers[dropped] = null;
        }
    }

    private void startFooter(int i) {
        if (closed) {
            return;
        }
        FooterLoad f = footers[i];
        FileMetadataCache.Claim claim = files.claim(i);
        if (claim.load() == null) {
            if (claim.pending() != null) {
                claim.pending().whenComplete((ignored, t) -> results.accept(() -> startFooter(i)));
                return;
            }
            f.loaded(claim.file(), claim.failure());
            list();
            return;
        }
        FileMetadataCache.ClaimedLoad load = claim.load();
        f.claim(load);
        if (load.readsInPlace()) {
            addParse(f, load);
            return;
        }
        f.charge(new Charge());
        f.job(new JobScheduler.Job(planner.endKey(), TailedInputFile.tailBytes(), JobScheduler.Kind.FOOTER,
                f.charge(), buffers -> {
            load.read();
            return () -> addParse(f, load);
        }, t -> () -> {
            // a failed read stays this read's; the next read of the file reads it again
            load.abandon();
            returnCharge(f);
            loaded(f, null, t);
        }));
        scheduler.add(f.job());
    }

    private void returnCharge(FooterLoad f) {
        Charge charge = f == null ? null : f.takeCharge();
        if (charge != null) {
            scheduler.release(charge);
        }
    }

    private void addParse(FooterLoad f, FileMetadataCache.ClaimedLoad load) {
        f.job(new JobScheduler.Job(planner.endKey(), 0, JobScheduler.Kind.PLAN, null, buffers -> {
            FileMetadataCache.PreparedFile file = load.parse();
            return () -> loaded(f, file, null);
        }, t -> () -> {
            // a parse that never ran, refused by a closed context, completes the load too
            load.fail(t);
            loaded(f, null, t);
        }));
        scheduler.add(f.job());
    }

    private void loaded(FooterLoad f, FileMetadataCache.PreparedFile file, Throwable t) {
        f.loaded(file, t);
        list();
    }

    /// Lists the files whose footers are in, in order, each checked against the first file's
    /// schema; a file that cannot be listed fails the read at its first row.
    private void list() {
        while (nextList < count && footers[nextList] != null && footers[nextList].done()) {
            FooterLoad f = footers[nextList];
            if (f.failure() == null) {
                try {
                    FileMetadataCache.PreparedFile file = f.file();
                    FileColumnOrdinals ordinals = ordinals(compatibility, filter, file);
                    SchemaCompatibility.ChunkPathCheck paths = compatibility.chunkPathCheck(file.schema(), ordinals);
                    for (int rg = 0; rg < file.rowGroups().size(); rg++) {
                        paths.verify(file.rowGroups().get(rg), rg, file.inputFile());
                    }
                    planner.addFile(nextList, file.inputFile(), file.schema(), file.rowGroups(), ordinals);
                }
                catch (RuntimeException e) {
                    f.failed(e);
                }
            }
            if (f.failure() != null) {
                publisher.failAt(planner.endKey(), f.failure());
                planner.listed();
                return;
            }
            nextList++;
            if (!planner.budgetLeft()) {
                // the row limit is covered: no further file is listed
                nextList = count;
            }
        }
        if (nextList == count) {
            planner.listed();
        }
    }

    /// The ordinals of a file after the first, checked against the first file's schema, with the
    /// filter in them.
    static FileColumnOrdinals ordinals(SchemaCompatibility compatibility, ResolvedPredicate filter,
                                       FileMetadataCache.PreparedFile file) {
        return FileColumnOrdinals.of(compatibility.validateSchemaCompatibility(file.inputFile(), file.schema()), filter,
                file.boundsReadability());
    }

    /// Gives up the loads whose next job is keyed past `end`, where the read ends at its failure or
    /// at its last page, and has not started: no job past it starts, and other reads of the file
    /// then load it themselves.
    void endAt(long end) {
        for (int i = dropped; i < count; i++) {
            FooterLoad f = footers[i];
            if (f != null && f.claimed() != null && f.job() != null && f.job().key() > end
                    && scheduler.remove(f.job())) {
                f.claimed().abandon();
                f.abandoned();
                returnCharge(f);
            }
        }
    }

    /// Gives up the loads claimed and not completed, as the read closes; once nothing is in flight.
    void close() {
        closed = true;
        for (FooterLoad f : footers) {
            if (f != null && f.claimed() != null) {
                f.claimed().abandon();
            }
        }
    }
}
