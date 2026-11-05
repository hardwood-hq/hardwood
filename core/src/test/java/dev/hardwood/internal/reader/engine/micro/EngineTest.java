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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The engine driven by hand, without a thread: each test posts the facts of one interleaving, in
/// the order it chooses, and checks what the polls decide.
///
/// Two row groups of 500 rows; column 0 in pages of 250 rows, column 1 in pages of 500, 8 bytes a
/// row. Row group 0 holds c0p0 `[0, 2000)`, c0p1 `[2000, 4000)` and c1p0 `[4000, 8000)`; cut into
/// requests of 3,000 bytes, r0.0 `[0, 3000)`, r0.1 `[3000, 6000)` and r0.2 `[6000, 8000)`, so c0p1
/// straddles r0.0 and r0.1, and c1p0 straddles r0.1 and r0.2. Row group 1 likewise, from row 500.
class EngineTest {

    private static final List<List<Page>> LAYOUT = SimulatedFile.layout(1_000, 500, 250, 500);
    private static final Page C0P0 = page(0, 0);
    private static final Page C0P1 = page(0, 1);
    private static final Page C0P2 = page(0, 2);
    private static final Page C0P3 = page(0, 3);
    private static final Page C1P0 = page(1, 0);
    private static final Page C1P1 = page(1, 1);
    private static final Request R00 = new Request(0, 0, 0, 3_000, 0);
    private static final Request R01 = new Request(0, 1, 3_000, 3_000, 0);
    private static final Request R02 = new Request(0, 2, 6_000, 2_000, 0);
    private static final Request R10 = new Request(1, 0, 8_000, 3_000, 500);
    private static final Request R11 = new Request(1, 1, 11_000, 3_000, 500);
    private static final Request R12 = new Request(1, 2, 14_000, 2_000, 500);

    private static Engine engine(long maxFetchBytes) {
        return new Engine(LAYOUT, 1_000, 500, maxFetchBytes, 3_000, 4);
    }

    private static Page page(int column, int index) {
        return LAYOUT.get(column).get(index);
    }

    @Test
    void indexesAreFetchedBeforeAnyRequest() {
        assertThat(engine(64_000).poll()).containsExactly(new Command.FetchIndex(0), new Command.FetchIndex(1));
    }

    /// Once row group 0 is planned, the walks of both columns stop on the requests holding their
    /// first pages, which start at once, and the one left, which the next batch needs, after them.
    @Test
    void aRowGroupsRequestsAreFetchedOnceItIsPlanned() {
        Engine engine = engine(64_000);
        engine.poll();
        engine.post(new Fact.IndexFetched(1));
        engine.post(new Fact.IndexFetched(0));
        assertThat(engine.poll()).containsExactly(new Command.ParseIndex(0));

        engine.post(new Fact.IndexParsed(0));
        List<Command> commands = engine.poll();
        assertThat(fetches(commands)).containsExactly(new Command.Fetch(R00), new Command.Fetch(R01),
                new Command.Fetch(R02));
        assertThat(commands).contains(new Command.ParseIndex(1));
    }

    /// With the budget taken by the second row group's index, read ahead, the requests the walks
    /// stop on and the next batch needs still start: both start at once, outside the budget.
    @Test
    void whatTheWalksAndTheNextBatchNeedStartsWithTheBudgetTaken() {
        Engine engine = engine(Fetching.INDEX_BYTES);
        assertThat(engine.poll()).containsExactly(new Command.FetchIndex(0), new Command.FetchIndex(1));
        engine.post(new Fact.IndexFetched(0));
        engine.poll();
        engine.post(new Fact.IndexParsed(0));

        assertThat(fetches(engine.poll())).containsExactly(new Command.Fetch(R00), new Command.Fetch(R01),
                new Command.Fetch(R02));
    }

    /// A page is walked once every request holding its bytes has arrived: c0p1 needs r0.0 and r0.1,
    /// c1p0 needs r0.1 and r0.2.
    @Test
    void aPageIsWalkedOnceEveryRequestHoldingItHasArrived() {
        Engine engine = engine(64_000);
        assertThat(fetches(planBoth(engine))).containsExactly(new Command.Fetch(R00), new Command.Fetch(R01),
                new Command.Fetch(R02), new Command.Fetch(R10), new Command.Fetch(R11), new Command.Fetch(R12));

        engine.post(new Fact.Fetched(R01));
        assertThat(engine.poll()).isEmpty();

        engine.post(new Fact.Fetched(R00));
        assertThat(engine.poll()).containsExactly(new Command.Decode(C0P0), new Command.Decode(C0P1));

        engine.post(new Fact.Fetched(R02));
        assertThat(engine.poll()).containsExactly(new Command.Decode(C1P0));
    }

    @Test
    void decodesStartLowestRowFirstAmongTheWalkedPages() {
        Engine engine = engine(64_000);
        planBoth(engine);
        // the requests arrive in reverse
        for (Request request : List.of(R12, R11, R10, R02, R01, R00)) {
            engine.post(new Fact.Fetched(request));
        }

        assertThat(engine.poll()).containsExactly(
                new Command.Decode(C0P0), new Command.Decode(C1P0), new Command.Decode(C0P1),
                new Command.Decode(C0P2));
    }

    @Test
    void aBatchWaitsForTheContiguousRowsOfEveryColumn() {
        Engine engine = engine(64_000);
        planBoth(engine);
        for (Request request : List.of(R00, R01, R02, R10, R11, R12)) {
            engine.post(new Fact.Fetched(request));
        }
        engine.poll();

        // rows 250 to 499 of column 0, and all of column 1's first page, but not rows 0 to 249
        engine.post(decoded(C0P1));
        engine.post(decoded(C1P0));
        assertThat(engine.poll()).containsExactly(new Command.Decode(C1P1), new Command.Decode(C0P3));
        assertThat(engine.take()).isNull();

        engine.post(decoded(C0P0));
        engine.poll();
        Batch batch = engine.take();
        assertThat(batch.firstRow()).isZero();
        assertThat(batch.columns()[0]).hasSize(500).startsWith(0, 1, 2).endsWith(499);
        assertThat(batch.columns()[1]).hasSize(500).startsWith(0, 2, 4).endsWith(998);
        assertThat(engine.take()).isNull();
    }

    @Test
    void aFailurePostedEndsTheDecisions() {
        Engine engine = engine(64_000);
        engine.poll();
        engine.post(new Fact.Failed(new Command.FetchIndex(0), new IllegalStateException("connection reset")));

        assertThat(engine.poll()).isEmpty();
        assertThat(engine.failure()).hasMessage("connection reset");
    }

    /// Fetches and parses both row groups' indexes; returns every command the polls returned.
    private static List<Command> planBoth(Engine engine) {
        List<Command> commands = new ArrayList<>(engine.poll());
        engine.post(new Fact.IndexFetched(0));
        engine.post(new Fact.IndexFetched(1));
        commands.addAll(engine.poll());
        engine.post(new Fact.IndexParsed(0));
        commands.addAll(engine.poll());
        engine.post(new Fact.IndexParsed(1));
        commands.addAll(engine.poll());
        return commands;
    }

    private static List<Command> fetches(List<Command> commands) {
        return commands.stream().filter(c -> c instanceof Command.Fetch).toList();
    }

    /// `page` decoded to the values [SimulatedFile#decode] gives: row `r` of column `c` holds
    /// `r * (c + 1)`.
    private static Fact.Decoded decoded(Page page) {
        long[] values = new long[page.rows()];
        for (int i = 0; i < values.length; i++) {
            values[i] = (page.firstRow() + i) * (page.column() + 1);
        }
        return new Fact.Decoded(page, values);
    }
}
