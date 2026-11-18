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

    /// One file's footer as the read has it, from its start until the consumer passes the file. A
    /// state's transitions are [FooterReader]'s: each replaces the file's state with the next.
    /// A state holding a charge holds what the footer's tail holds in R, returned once the
    /// consumer passes the file or the load is given up; `null` where it holds none.
    private sealed interface FooterLoad {

        Charge charge();
    }

    /// Started, and waiting for a load of the footer that the reader's [FileMetadataCache] runs
    /// for someone else: another read of the reader, or a caller outside a read.
    private record Waiting() implements FooterLoad {

        @Override
        public Charge charge() {
            return null;
        }
    }

    /// A load this read runs: `job` reads its tail or parses it. `charge` is `null` for a file held
    /// in memory, parsed in place.
    private record Loading(FileMetadataCache.ClaimedLoad load, JobScheduler.Job<?> job, Charge charge)
            implements FooterLoad {
    }

    /// Loaded, by this read's own load or found loaded in the reader's [FileMetadataCache], and not
    /// yet listed.
    private record Loaded(FileMetadataCache.PreparedFile file, Charge charge) implements FooterLoad {
    }

    /// Its load or its listing failed with `failure`, which fails the read at its file's first row.
    private record Failed(Throwable failure, Charge charge) implements FooterLoad {
    }

    /// Listed: its file is checked against the first file's schema and its row groups handed to
    /// [RowGroupPlanner#addFile], in file order. Nothing of the footer is needed any more; it holds
    /// its tail's charge until the consumer passes the file.
    private record Listed(Charge charge) implements FooterLoad {
    }

    /// Given up before its job started, where the read ends before the file: other reads of the
    /// file load it themselves.
    private record GivenUp() implements FooterLoad {

        @Override
        public Charge charge() {
            return null;
        }
    }

    static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";

    /// The files after the one a read has reached, the next one included, whose footers it reads
    /// ahead without the override: a bound on the footer reads a read has in flight for files it
    /// has not reached. A read of many small files, whose data never fills F, is bound by round
    /// trips, and overlaps as many files as this lets it; a read of larger files fills F first,
    /// and its footers wait behind its data.
    static final int DEFAULT_MAX_FILES_AHEAD = 64;

    private static final Waiting WAITING = new Waiting();
    private static final GivenUp GIVEN_UP = new GivenUp();

    private final FileMetadataCache files;
    private final JobScheduler scheduler;
    private final RowGroupPlanner planner;
    private final ReadFailure readFailure;
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

    FooterReader(FileMetadataCache files, JobScheduler scheduler, RowGroupPlanner planner, ReadFailure readFailure,
                 SchemaCompatibility compatibility, ResolvedPredicate filter, boolean limited,
                 Consumer<Runnable> results) {
        this.files = files;
        this.scheduler = scheduler;
        this.planner = planner;
        this.readFailure = readFailure;
        this.compatibility = compatibility;
        this.filter = filter;
        this.results = results;
        List<InputFile> inputs = files.inputFiles();
        this.count = inputs.size();
        this.filesAhead = filesAhead(limited, inputs.stream().allMatch(HeldBytes::inMemory), count);
        this.footers = new FooterLoad[count];
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
        footers[0] = new Listed(null);
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
                footers[i] = WAITING;
                startFooter(i);
            }
        }
        for (; dropped < (at < 0 ? nextList : at); dropped++) {
            files.drop(dropped);
            returnCharge(footers[dropped]);
            footers[dropped] = null;
        }
    }

    /// Claims file `i`'s footer, which waits: takes what another read loaded, waits for another
    /// read's load, or loads it on a job of this read.
    private void startFooter(int i) {
        // another read's load can end after the consumer has passed the file, and the read is done
        // with it
        if (closed || i < dropped) {
            return;
        }
        if (!(footers[i] instanceof Waiting)) {
            throw new IllegalStateException("The footer of file " + i + " starts while " + footers[i]);
        }
        FileMetadataCache.Claim claim = files.claim(i);
        FileMetadataCache.ClaimedLoad load = claim.load();
        if (load == null) {
            if (claim.pending() != null) {
                claim.pending().whenComplete((ignored, t) -> results.accept(() -> startFooter(i)));
                return;
            }
            loaded(i, claim.file(), claim.failure(), null);
            return;
        }
        if (load.readsInPlace()) {
            parse(i, load, null);
            return;
        }
        Charge charge = new Charge();
        JobScheduler.Job<Void> job = new JobScheduler.Job<>(planner.endKey(), TailedInputFile.tailBytes(),
                JobScheduler.Kind.FOOTER, charge, buffers -> {
                    load.read();
                    return null;
                }, read -> parse(i, load, charge), t -> {
                    // a failed read stays this read's; the next read of the file reads it again
                    load.abandon();
                    scheduler.release(charge);
                    loaded(i, null, t, null);
                });
        footers[i] = new Loading(load, job, charge);
        scheduler.add(job);
    }

    private void returnCharge(FooterLoad f) {
        Charge charge = f == null ? null : f.charge();
        if (charge != null) {
            scheduler.release(charge);
        }
    }

    /// Parses file `i`'s tail, read and holding `charge`, on a job of this read.
    private void parse(int i, FileMetadataCache.ClaimedLoad load, Charge charge) {
        JobScheduler.Job<FileMetadataCache.PreparedFile> job = new JobScheduler.Job<>(planner.endKey(), 0,
                JobScheduler.Kind.PLAN, null, buffers -> load.parse(), file -> loaded(i, file, null, charge), t -> {
                    // a parse that never ran, refused by a closed context, completes the load too
                    load.fail(t);
                    loaded(i, null, t, charge);
                });
        footers[i] = new Loading(load, job, charge);
        scheduler.add(job);
    }

    /// File `i`'s footer is loaded as `file`, or failed with `failure`, its tail holding `charge`.
    private void loaded(int i, FileMetadataCache.PreparedFile file, Throwable failure, Charge charge) {
        footers[i] = failure == null ? new Loaded(file, charge) : new Failed(failure, charge);
        list();
    }

    /// Lists the files whose footers are in, in order, each checked against the first file's
    /// schema; a file that cannot be listed fails the read at its first row.
    private void list() {
        while (nextList < count) {
            FooterLoad f = footers[nextList];
            if (f instanceof Loaded l) {
                f = listed(l);
                footers[nextList] = f;
            }
            if (f instanceof Failed failed) {
                readFailure.failAt(planner.endKey(), failed.failure());
                planner.listed();
                return;
            }
            if (!(f instanceof Listed)) {
                break;
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

    /// Lists file `nextList`, loaded as `l`: [Listed], or [Failed] where it does not match the
    /// first file's schema.
    private FooterLoad listed(Loaded l) {
        try {
            FileMetadataCache.PreparedFile file = l.file();
            FileColumnOrdinals ordinals = ordinals(compatibility, filter, file);
            SchemaCompatibility.ChunkPathCheck paths = compatibility.chunkPathCheck(file.schema(), ordinals);
            for (int rg = 0; rg < file.rowGroups().size(); rg++) {
                paths.verify(file.rowGroups().get(rg), rg, file.inputFile());
            }
            planner.addFile(nextList, file.inputFile(), file.schema(), file.rowGroups(), ordinals);
            return new Listed(l.charge());
        }
        catch (RuntimeException e) {
            return new Failed(e, l.charge());
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
            if (footers[i] instanceof Loading loading && loading.job().key() > end && scheduler.remove(loading.job())) {
                loading.load().abandon();
                returnCharge(loading);
                footers[i] = GIVEN_UP;
            }
        }
    }

    /// Gives up the loads claimed and not completed, as the read closes; once nothing is in flight.
    void close() {
        closed = true;
        for (FooterLoad f : footers) {
            if (f instanceof Loading loading) {
                loading.load().abandon();
            }
        }
    }
}
