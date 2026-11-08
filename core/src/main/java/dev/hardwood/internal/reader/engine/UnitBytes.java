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
import java.util.Arrays;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.ReadRanges;

/// The buffers that the data requests of one unit of a read fetched from their files. The read
/// gives each buffer back to its file ([InputFile#release]) when nothing reads it: every column
/// that holds its request has passed the request, no walk advance that can slice it runs, and
/// every page sliced from it is done.
///
/// A page sliced from a request is read after the request is released: its decode runs later, and
/// a column decoded through levels fills its cells from it later still. Thus each buffer counts
/// the pages placed from it that are not done. A walk advance can slice a request and then pass
/// it before the read places the pages it took. Thus a buffer whose request is released while
/// advances of the unit run waits until each advance that started before the release has ended.
///
/// Thread-safe: a page can end on the job that fills its last cell.
final class UnitBytes {

    /// The buffer of one data request.
    static final class Fetched {

        private final InputFile inputFile;
        /// The buffer, `null` once it is given back, so that the request does not keep it reachable.
        private ByteBuffer bytes;
        /// The pages placed from the buffer that are not done.
        private int pages;
        /// The number of the last advance started when the request was released, `-1` while the
        /// request is not released.
        private long releasedAfter = -1;
        private boolean givenBack;

        private Fetched(InputFile inputFile, ByteBuffer bytes) {
            this.inputFile = inputFile;
            this.bytes = bytes;
        }
    }

    /// The buffers not given back, `null` once the read closes.
    private List<Fetched> held = new ArrayList<>();
    /// The number of advances of the unit started.
    private long advances;
    /// The numbers of the advances in flight, `[0, inFlightCount)` in use: one at most for each
    /// column.
    private long[] inFlight = new long[4];
    private int inFlightCount;

    /// Takes in `bytes`, which `inputFile` returned for one of the unit's requests.
    ///
    /// @throws IllegalStateException if the read is closed
    synchronized Fetched add(InputFile inputFile, ByteBuffer bytes) {
        if (held == null) {
            throw new IllegalStateException("The read of the unit is closed");
        }
        Fetched fetched = new Fetched(inputFile, bytes);
        held.add(fetched);
        return fetched;
    }

    /// Records that the request of `fetched` is released; returns the pending release of its buffer
    /// if nothing reads it, `null` otherwise. Only the first call has an effect.
    synchronized Runnable released(Fetched fetched) {
        if (fetched.releasedAfter >= 0) {
            return null;
        }
        fetched.releasedAfter = advances;
        return pendingReleaseIfDone(fetched);
    }

    /// Records a walk advance of the unit that starts; returns its number for [#advanceEnded].
    synchronized long advanceStarted() {
        if (inFlightCount == inFlight.length) {
            inFlight = Arrays.copyOf(inFlight, inFlightCount * 2);
        }
        inFlight[inFlightCount++] = ++advances;
        return advances;
    }

    /// Records that advance `advance` has ended, and that the read has placed the pages it took;
    /// returns the pending release of the buffers that nothing reads now, `null` for none.
    ///
    /// @throws IllegalStateException if no advance of that number is in flight
    synchronized Runnable advanceEnded(long advance) {
        int i = 0;
        while (i < inFlightCount && inFlight[i] != advance) {
            i++;
        }
        if (i == inFlightCount) {
            throw new IllegalStateException("No advance " + advance + " of the unit is in flight");
        }
        inFlight[i] = inFlight[--inFlightCount];
        if (held == null) {
            return null;
        }
        List<Fetched> done = null;
        for (int f = held.size() - 1; f >= 0; f--) {
            Fetched fetched = held.get(f);
            if (isDone(fetched)) {
                held.remove(f);
                if (done == null) {
                    done = new ArrayList<>();
                }
                done.add(fetched);
            }
        }
        return done == null ? null : pendingRelease(done);
    }

    /// Pins the buffer of `fetched` for a page sliced from it.
    ///
    /// @throws IllegalStateException if the buffer is given back already
    synchronized void pin(Fetched fetched) {
        if (fetched.givenBack) {
            throw new IllegalStateException("The buffer of the request is given back already");
        }
        fetched.pages++;
    }

    /// Drops the pin of a page on the buffer of `fetched`; returns the pending release of the buffer
    /// if nothing reads it now, `null` otherwise.
    ///
    /// @throws IllegalStateException if no page holds a pin on the buffer
    synchronized Runnable unpin(Fetched fetched) {
        if (fetched.pages <= 0) {
            throw new IllegalStateException("No page holds a pin on the buffer of the request");
        }
        fetched.pages--;
        return pendingReleaseIfDone(fetched);
    }

    /// Gives back every buffer not given back, whatever reads it, as the read closes when no job
    /// of it runs; returns the pending release, `null` for none.
    synchronized Runnable releaseAll() {
        List<Fetched> remaining = held;
        held = null;
        if (remaining == null || remaining.isEmpty()) {
            return null;
        }
        return pendingRelease(remaining);
    }

    /// The pending release of the buffer of `fetched` if nothing reads it, `null` otherwise.
    private Runnable pendingReleaseIfDone(Fetched fetched) {
        if (held == null || !isDone(fetched)) {
            return null;
        }
        held.remove(fetched);
        return pendingRelease(List.of(fetched));
    }

    /// Whether nothing reads the buffer of `fetched`: its request is released, no page holds a
    /// pin on it, and each advance in flight started after the release.
    private boolean isDone(Fetched fetched) {
        if (fetched.givenBack || fetched.releasedAfter < 0 || fetched.pages > 0) {
            return false;
        }
        for (int i = 0; i < inFlightCount; i++) {
            if (inFlight[i] <= fetched.releasedAfter) {
                return false;
            }
        }
        return true;
    }

    /// Marks `fetched` given back and drops their buffers; returns the pending release that gives
    /// the buffers to their files.
    private static Runnable pendingRelease(List<Fetched> fetched) {
        InputFile[] files = new InputFile[fetched.size()];
        ByteBuffer[] buffers = new ByteBuffer[fetched.size()];
        for (int i = 0; i < files.length; i++) {
            Fetched f = fetched.get(i);
            f.givenBack = true;
            files[i] = f.inputFile;
            buffers[i] = f.bytes;
            f.bytes = null;
        }
        return () -> {
            for (int i = 0; i < files.length; i++) {
                ReadRanges.release(files[i], buffers[i]);
            }
        };
    }
}
