/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.engine.micro.runtime.Machine;

/// The read as a pure state machine that decides in one poll: [Fact]s in, [Command]s out. It runs
/// nothing, and knows nothing of threads, locks, time, files or decoders: a generic
/// [dev.hardwood.internal.reader.engine.micro.runtime.Driver] runs it, under one lock, with
/// [ReadInterpreter] running its commands.
///
/// Its state is grouped by the stage of the read it belongs to, one object each: [Planning],
/// [Walking], [Fetching], [Decoding], and [Output], the consumer's side. A stage's object holds
/// that stage's state and its share of the poll, and reads no other stage. [#poll] is the only
/// caller of the stages' polls, in one pass and a fixed order, and it passes each what it needs
/// from the other stages as values: the consumer's [Demand], the rows and requests planned, the
/// requests the walks stopped on, and the pages walked. What one stage takes from another is
/// therefore the argument list of a call in [#poll], and a fact goes to the stages it concerns
/// through [#post].
final class Engine implements Machine<Fact, Command> {

    static final int BATCH_ROWS = 500;
    /// the bytes in flight for wants (F); a need's or a stopped walk's request starts at once,
    /// outside it
    static final long MAX_FETCH_BYTES = 128 * 1024;
    /// the most bytes one request reads
    static final long MAX_REQUEST_BYTES = 8 * 1024;
    /// the read-ahead cap, in rows past the first row the consumer has not taken
    static final long AHEAD_ROWS = 4 * BATCH_ROWS;

    private final Planning planning;
    private final Walking walking;
    private final Fetching fetching;
    private final Decoding decoding;
    private final Output output;
    private Throwable failure;

    Engine(List<List<Page>> layout, long totalRows, long rowGroupRows, long maxFetchBytes, long maxRequestBytes,
           int maxDecodes) {
        this.planning = new Planning(layout, rowGroupRows, maxRequestBytes);
        this.walking = new Walking(layout);
        this.fetching = new Fetching(totalRows, rowGroupRows, maxFetchBytes);
        this.decoding = new Decoding(layout.size(), maxDecodes);
        this.output = new Output(BATCH_ROWS, totalRows);
    }

    // ==================== Facts in ====================

    /// Records `fact` with the stages it concerns. Records only: what follows from it is decided by
    /// the next poll.
    @Override
    public void post(Fact fact) {
        switch (fact) {
            case Fact.Fetched f -> {
                fetching.ended(new Command.Fetch(f.request()));
                walking.arrived(f.request());
            }
            case Fact.IndexFetched f -> {
                fetching.ended(new Command.FetchIndex(f.rowGroup()));
                planning.indexFetched(f.rowGroup());
            }
            case Fact.IndexParsed p -> planning.parsed(p.rowGroup());
            case Fact.Decoded d -> decoding.decoded(d.page(), d.values());
            case Fact.Failed f -> {
                switch (f.command()) {
                    case Command.Fetch c -> fetching.ended(c);
                    case Command.FetchIndex c -> fetching.ended(c);
                    case Command.ParseIndex ignored -> planning.parseFailed();
                    case Command.Decode ignored -> decoding.decodeFailed();
                }
                if (failure == null) {
                    failure = f.cause();
                }
            }
        }
    }

    // ==================== Commands out ====================

    /// The one place that decides what happens next: each stage's poll in turn, with what it takes
    /// from the others.
    @Override
    public List<Command> poll() {
        List<Command> out = new ArrayList<>();
        if (failure == null) {
            Demand demand = output.demand();
            planning.poll(out);
            walking.poll(demand, planning.plannedRows(), planning.plannedRequests());
            fetching.poll(demand, planning.plannedRequests(), walking.stoppedOn(), out);
            decoding.poll(walking.takeWalked(), out);
            buildBatches();
        }
        return out;
    }

    /// Builds every batch, in order, whose rows every column has decoded: the hand-off from the
    /// decoding stage to the consumer's side.
    private void buildBatches() {
        while (!output.allBuilt() && decoding.decodedBefore(output.nextEnd())) {
            long first = output.nextFirst();
            output.built(new Batch(first, decoding.takeRows(first, output.nextEnd())));
        }
    }

    // ==================== The consumer's side ====================

    /// Whether a batch is built and not taken.
    boolean hasBatch() {
        return output.hasBuilt();
    }

    /// The consumer takes the next batch, `null` if none is built.
    Batch take() {
        return output.take();
    }

    /// Whether every batch is built and taken.
    boolean done() {
        return output.done();
    }

    /// The failure that ended the read, `null` while it goes on.
    Throwable failure() {
        return failure;
    }

    @Override
    public String toString() {
        return planning + " " + walking + " " + fetching + " " + decoding + " " + output;
    }
}
