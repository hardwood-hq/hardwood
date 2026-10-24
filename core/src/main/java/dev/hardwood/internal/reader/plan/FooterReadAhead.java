/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.memory.InternalProperty;

/// Reads the footers of a multi-file read's files ahead of it, as requests of its [FetchSequence]:
/// the footer of the file after the one the read has reached at once and uncharged, as that
/// file's next unit, and those of the files after it, up to the files ahead, in position order
/// after the read's entries of earlier files, each charged to R at the tail its read takes
/// ([TailedInputFile#tailBytes()]). A footer's charge is returned once the read is done with its
/// file, which drops the file's tail and footer, or as the read stops reading footers ahead or
/// closes. Its monitor guards its state and is held across no call into the sequence or the
/// footers.
final class FooterReadAhead {

    static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";

    /// The files after the one a read has reached, the next one included, whose footers it reads
    /// ahead without the override: a bound on the footer requests a read has in flight for files
    /// it has not reached. A read of many small files, whose data never fills F, is bound by round
    /// trips, and overlaps as many files as this lets it; a read of larger files fills F first,
    /// and its footers wait behind its data.
    static final int DEFAULT_MAX_FILES_AHEAD = 64;

    /// Where the footers are read and dropped: the read's [FileMetadataCache].
    interface Footers {

        /// Starts the footer read of `fileIndex` unless it has started or is read in place.
        void start(int fileIndex);

        /// Drops the tail and the footer of `fileIndex`, which the read is done with.
        void drop(int fileIndex);
    }

    private final FetchSequence sequence;
    private final Footers footers;
    /// The files after the one the read has reached whose footers start ahead of it.
    private final int filesAhead;
    private final int fileCount;
    private final int charge = TailedInputFile.tailBytes();

    /// The footer requests not discarded, by file.
    private final Map<Integer, Footer> requests = new HashMap<>();
    /// The last file the read has reached, `-1` before any.
    private int reachedFile = -1;
    /// The next file whose footer is to be requested.
    private int nextFooter = 1;
    /// The files before this one the read is done with.
    private int doneBefore;
    /// Whether footers are requested no more.
    private boolean stopped;

    /// @param filesAhead the files after the one the read has reached whose footers start ahead
    ///        of it, the next one included
    /// @param fileCount the read's files
    FooterReadAhead(FetchSequence sequence, Footers footers, int filesAhead, int fileCount) {
        this.sequence = sequence;
        this.footers = footers;
        this.filesAhead = filesAhead;
        this.fileCount = fileCount;
    }

    /// The files after the one it has reached whose footers a read reads ahead: up to
    /// [#maxFilesAhead] for a multi-file read that plans past its next unit (a read of files not
    /// all in memory without a row limit), and otherwise only the next file: a read of files all
    /// in memory, whose reads wait for nothing to overlap, or with a row limit, which may need no
    /// further file and under a filter learns only downstream that it is satisfied.
    static int filesAhead(boolean plansDeep, int fileCount) {
        return fileCount < 2 || !plansDeep ? 1 : maxFilesAhead();
    }

    /// The files after the one it has reached a read reads footers of ahead:
    /// [#DEFAULT_MAX_FILES_AHEAD] unless overridden through [#MAX_FILES_AHEAD_PROPERTY], read for
    /// each read. A value that is not a positive count is rejected rather than replaced by the
    /// default; `1` reads only the next file's footer ahead.
    private static int maxFilesAhead() {
        return InternalProperty.intValue(MAX_FILES_AHEAD_PROPERTY, DEFAULT_MAX_FILES_AHEAD, 1, "a positive count");
    }

    /// Records that the read has reached `fileIndex`: starts the footers requested up to the next
    /// file's, those of files it passed included, and queues those of the files ahead the move
    /// brings within reach.
    void reached(int fileIndex) {
        List<Footer> ahead = new ArrayList<>();
        List<Footer> now = new ArrayList<>();
        synchronized (this) {
            if (stopped || fileIndex <= reachedFile) {
                return;
            }
            for (int i = reachedFile + 1; i <= fileIndex + 1; i++) {
                Footer requested = requests.get(i);
                if (requested != null) {
                    now.add(requested);
                }
            }
            reachedFile = fileIndex;
            int last = Math.min(fileIndex + filesAhead, fileCount - 1);
            for (int i = Math.max(nextFooter, fileIndex + 1); i <= last; i++) {
                Footer footer = new Footer(i);
                requests.put(i, footer);
                if (i > fileIndex + 1) {
                    ahead.add(footer);
                }
            }
            nextFooter = Math.max(nextFooter, last + 1);
            Footer next = requests.get(fileIndex + 1);
            if (next != null && !now.contains(next)) {
                now.add(next);
            }
        }
        now.forEach(sequence::startNow);
        sequence.queue(ahead);
    }

    /// Whether the footer of `fileIndex` has started, or was not to be read ahead: the planning
    /// chain lists a file ahead of the read only then.
    synchronized boolean started(int fileIndex) {
        Footer footer = requests.get(fileIndex);
        return fileIndex < nextFooter && (footer == null || footer.started());
    }

    /// Records that the read is done with every file before `fileIndex`: their tails and footers
    /// are dropped and their footers' charges returned.
    void filesDoneBefore(int fileIndex) {
        List<Footer> done = new ArrayList<>();
        int from;
        synchronized (this) {
            from = doneBefore;
            doneBefore = Math.max(doneBefore, fileIndex);
            for (int i = from; i < fileIndex; i++) {
                Footer footer = requests.remove(i);
                if (footer != null) {
                    done.add(footer);
                }
            }
        }
        done.forEach(sequence::discard);
        for (int i = from; i < fileIndex; i++) {
            footers.drop(i);
        }
    }

    /// Requests no further footer, since the read plans no further file, and discards those
    /// requested, dropping their tails.
    void stop() {
        List<Footer> requested;
        synchronized (this) {
            stopped = true;
            requested = new ArrayList<>(requests.values());
            requests.clear();
        }
        for (Footer footer : requested) {
            sequence.discard(footer);
            footers.drop(footer.fileIndex);
        }
    }

    /// Requests no further footer, and drops the footers of every file the read has requested or
    /// passed, discarding their requests.
    void close() {
        int requestedUpTo;
        synchronized (this) {
            stopped = true;
            requestedUpTo = nextFooter;
        }
        filesDoneBefore(requestedUpTo);
    }

    /// Whether `footer` is still requested: not discarded as the read finished its file, stopped
    /// or closed.
    private synchronized boolean requested(Footer footer) {
        return requests.get(footer.fileIndex) == footer;
    }

    /// The read of one file's footer, as a request of the read's sequence.
    private final class Footer extends FetchSequence.Request {

        private final int fileIndex;

        Footer(int fileIndex) {
            super(fileIndex, charge);
            this.fileIndex = fileIndex;
        }

        @Override
        protected Runnable begin() {
            return () -> {
                footers.start(fileIndex);
                if (!requested(this)) {
                    // Discarded while it started, after its tail was dropped
                    footers.drop(fileIndex);
                }
            };
        }
    }
}
