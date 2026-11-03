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
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.util.Lazy;

/// The buffers the data requests of one unit of a read fetched from their files, given back to
/// them ([InputFile#release]) once nothing reads them: every column has passed the unit, no walk
/// advance of it runs, and every page taken from it reads its bytes no more.
///
/// A page sliced from a request outlives the request's release by the walks: its decode runs
/// later, and a column decoded through levels fills its cells from it later still. So the unit
/// counts pins: its own until every column has passed it, one per walk advance in flight, and one
/// per page placed and not done. Bytes that arrive once the count is back at zero are given back at
/// once.
///
/// Thread-safe: a page ends on the job that fills its last cell.
final class UnitBytes {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(UnitBytes.class.getName()));

    private record Fetched(InputFile inputFile, ByteBuffer bytes) {
    }

    /// The buffers arrived and not given back, `null` once given back.
    private List<Fetched> fetched = new ArrayList<>();
    private int pins = 1;
    /// Whether the unit's own pin is dropped.
    private boolean passed;

    /// Pins the bytes for a walk advance or a page.
    ///
    /// @throws IllegalStateException if the bytes are given back already
    synchronized void pin() {
        if (fetched == null) {
            throw new IllegalStateException("The bytes of the unit are given back already");
        }
        pins++;
    }

    /// Drops a pin; returns what gives the bytes back where it was the last, `null` otherwise.
    ///
    /// @throws IllegalStateException if no pin is held
    synchronized Runnable unpin() {
        if (pins <= 0) {
            throw new IllegalStateException("No pin is held on the bytes of the unit");
        }
        return --pins == 0 ? takeAll() : null;
    }

    /// Drops the unit's own pin as every column has passed it, once; returns what gives the bytes
    /// back where it was the last, `null` otherwise.
    synchronized Runnable passed() {
        if (passed) {
            return null;
        }
        passed = true;
        return unpin();
    }

    /// Takes in `bytes`, which `inputFile` returned for one of the unit's requests; returns what
    /// gives them back at once where nothing reads the unit's bytes any more, `null` otherwise.
    Runnable add(InputFile inputFile, ByteBuffer bytes) {
        Fetched arrived = new Fetched(inputFile, bytes);
        synchronized (this) {
            if (fetched != null) {
                fetched.add(arrived);
                return null;
            }
        }
        return () -> giveBack(arrived);
    }

    /// Takes every buffer whatever the pins, as the read closes once no job of it runs; returns what
    /// gives them back, `null` for none.
    synchronized Runnable releaseAll() {
        return takeAll();
    }

    private Runnable takeAll() {
        List<Fetched> taken = fetched;
        fetched = null;
        if (taken == null || taken.isEmpty()) {
            return null;
        }
        return () -> {
            for (Fetched f : taken) {
                giveBack(f);
            }
        };
    }

    /// Gives `f` back to its file; a failure of the file's release is logged, since the read's bytes
    /// are its own no longer either way.
    private static void giveBack(Fetched f) {
        try {
            f.inputFile().release(f.bytes());
        }
        catch (RuntimeException e) {
            LOG.get().log(System.Logger.Level.WARNING, () -> "Release of a buffer failed for " + f.inputFile().name(), e);
        }
    }
}
