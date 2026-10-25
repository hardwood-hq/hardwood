/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/// The fetching stage: the next row group's index, the planned requests not started, and the bytes
/// in flight for wants. Its share of the engine's poll, [#poll], decides what to fetch from the
/// demand, the requests planned and the requests a stopped walk waits for, which the poll passes
/// in; it reads no other stage.
final class Fetching {

    /// the bytes a row group's index read takes from the budget
    static final long INDEX_BYTES = 512;

    private final long rowGroupRows;
    private final int rowGroupCount;
    /// the bytes in flight for wants (F); a need's or a demand's fetch starts outside it
    private final long maxBytes;
    private long bytesInFlight;
    /// the row group whose index is fetched next
    private int nextIndex;
    /// the planned requests taken in so far ([#poll])
    private int seen;
    /// the planned requests not started, lowest row first
    private final TreeSet<Request> pending = new TreeSet<>(Comparator.comparingLong(Request::firstRow)
            .thenComparingInt(Request::rowGroup).thenComparingInt(Request::index));
    /// the fetches started for a want, and the bytes each holds of the budget
    private final Map<Command, Long> charged = new HashMap<>();

    Fetching(long totalRows, long rowGroupRows, long maxBytes) {
        this.rowGroupRows = rowGroupRows;
        this.rowGroupCount = Math.toIntExact((totalRows + rowGroupRows - 1) / rowGroupRows);
        this.maxBytes = maxBytes;
    }

    // --- its share of the poll ---

    /// Starts fetches: the requests a stopped walk waits for (`stoppedOn`) at once; then, lowest row
    /// first, index or request, what `demand` needs at once, and what it wants while the bytes in
    /// flight leave room. Only the requests of row groups planned are fetched (`planned`).
    void poll(Demand demand, List<Request> planned, Set<Request> stoppedOn, List<Command> out) {
        for (; seen < planned.size(); seen++) {
            pending.add(planned.get(seen));
        }
        for (Request request : stoppedOn) {
            if (pending.remove(request)) {
                start(new Command.Fetch(request), false, out);
            }
        }
        Command next;
        while ((next = next(demand.need())) != null) {
            start(next, false, out);
        }
        while ((next = next(demand.want())) != null && fits(next)) {
            start(next, true, out);
        }
    }

    /// The fetch of the lowest row below `limit`, `null` if none.
    private Command next(long limit) {
        long indexRow = nextIndex < rowGroupCount ? nextIndex * rowGroupRows : Long.MAX_VALUE;
        long requestRow = pending.isEmpty() ? Long.MAX_VALUE : pending.first().firstRow();
        if (indexRow <= requestRow) {
            return indexRow < limit ? new Command.FetchIndex(nextIndex) : null;
        }
        return requestRow < limit ? new Command.Fetch(pending.first()) : null;
    }

    /// Whether `fetch` fits the budget; with nothing in flight, any fetch does.
    private boolean fits(Command fetch) {
        return bytesInFlight == 0 || bytesInFlight + bytes(fetch) <= maxBytes;
    }

    private void start(Command fetch, boolean charge, List<Command> out) {
        switch (fetch) {
            case Command.FetchIndex ignored -> nextIndex++;
            case Command.Fetch f -> pending.remove(f.request());
            default -> throw new IllegalStateException("not a fetch: " + fetch);
        }
        if (charge) {
            charged.put(fetch, bytes(fetch));
            bytesInFlight += bytes(fetch);
        }
        out.add(fetch);
    }

    private static long bytes(Command fetch) {
        return switch (fetch) {
            case Command.FetchIndex ignored -> INDEX_BYTES;
            case Command.Fetch f -> f.request().length();
            default -> throw new IllegalStateException("not a fetch: " + fetch);
        };
    }

    // --- facts ---

    /// `fetch` ended: it arrived or failed.
    void ended(Command fetch) {
        Long bytes = charged.remove(fetch);
        if (bytes != null) {
            bytesInFlight -= bytes;
        }
    }

    @Override
    public String toString() {
        return "fetching[bytes=" + bytesInFlight + "/" + maxBytes + " pending=" + pending.size() + "]";
    }
}
