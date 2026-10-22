/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// The CPU work a read does on what it fetched as it goes, parsing the footer of every file after
/// the first, the column indexes a filter evaluates and the offset indexes each row group's fetch
/// plans are built from, and scanning a dictionary for a filter's literal, runs on the context's pool
/// ([HardwoodContextImpl#compute]), so that the pool's size bounds it, rather than on the read's
/// virtual threads.
@Tag("pool")
class MetadataParseThreadTest {

    /// Four row groups of 50 rows, with a page index.
    private static final Path FILE = Path.of("src/test/resources/differential/diff_order_multi.parquet");
    /// Four row groups of 100 rows with disjoint `i64` ranges, every column dictionary-encoded.
    private static final Path DICTIONARY_FILE = Path.of("src/test/resources/predicate/predicate_dict.parquet");

    /// One computation the pool ran: what it was, the thread waiting for it and the one running it.
    private record Computed(String what, Thread caller, Thread runner) {
    }

    private static final String POOL_THREAD = "hardwood-\\d+";

    private final List<Computed> computed = new CopyOnWriteArrayList<>();

    @AfterEach
    void stopObserving() {
        HardwoodContextImpl.computeObserver = null;
    }

    @Test
    void footersPageIndexesAndFetchPlansAreParsedOnThePool() throws Exception {
        HardwoodContextImpl.computeObserver = (what, caller) -> computed.add(
                new Computed(what, caller, Thread.currentThread()));
        int rows = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.openAll(
                     List.of(InputFile.of(FILE), InputFile.of(FILE)), context);
             RowReader rowReader = reader.buildRowReader().filter(FilterPredicate.lt("__row__", 10L)).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(20);
        assertThat(computed).extracting(Computed::what).contains("footer", "page filter", "fetch plans");
        assertRanOnThePool();
        assertThat(computed).as("computations a fetch step ran in place").anySatisfy(
                c -> assertThat(c.caller()).isSameAs(c.runner()));
    }

    /// A literal inside every row group's `i64` range and absent from all of them leaves the
    /// statistics undecided, so each row group's dictionary is scanned for it.
    @Test
    void aDictionaryIsScannedForAFilterLiteralOnThePool() throws Exception {
        HardwoodContextImpl.computeObserver = (what, caller) -> computed.add(
                new Computed(what, caller, Thread.currentThread()));
        int rows = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(DICTIONARY_FILE), context);
             RowReader rowReader = reader.buildRowReader()
                     .filter(FilterPredicate.in("i64", -3_990_000_000_000L, 10_000_000_000L)).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        assertThat(rows).isZero();
        assertThat(computed).extracting(Computed::what).contains("dictionary check");
        assertRanOnThePool();
    }

    /// Every computation ran on a thread of the pool: handed over by a thread off the pool, or
    /// in place on the thread of the pool that needed it, such as a column's fetch step, which
    /// never waits for another thread of the pool.
    private void assertRanOnThePool() {
        assertThat(computed).allSatisfy(c -> {
            assertThat(c.runner().getName()).as("thread running %s", c.what()).matches(POOL_THREAD);
            if (c.caller().getName().matches(POOL_THREAD)) {
                assertThat(c.caller()).as("thread of the pool needing %s", c.what()).isSameAs(c.runner());
            }
        });
    }
}
