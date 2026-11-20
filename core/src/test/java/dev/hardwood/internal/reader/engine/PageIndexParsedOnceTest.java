/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// A read parses each structure of a column chunk's page index at most once: page filtering asks
/// for a column once per leaf testing it, and page planning for every decoded column's
/// OffsetIndex, and they share one parse. Counted through [PageFilterEvaluator#parseObserver], on
/// a context of zero threads, whose read parses on the thread driving it.
class PageIndexParsedOnceTest {

    /// three row groups of 1000 rows, `id` 0 to 2999 and `value` 1000 to 3999, a page index per chunk
    private static final Path FILE = Path.of("src/test/resources/multi_row_group_page_index.parquet");

    @Test
    void aRangeOnAProjectedColumnParsesEachStructureOnce() throws Exception {
        // two leaves on id, which is also decoded; statistics leave row group 1 only
        List<Class<?>> parses = traceParses(FilterPredicate.and(FilterPredicate.gtEq("id", 1500L),
                FilterPredicate.lt("id", 1600L)), "id", "value");

        // the OffsetIndex of id and value, the ColumnIndex of id
        assertThat(parses).filteredOn(OffsetIndex.class::equals).hasSize(2);
        assertThat(parses).filteredOn(ColumnIndex.class::equals).hasSize(1);
    }

    @Test
    void aFilterOnlyColumnParsesItsPairOnce() throws Exception {
        List<Class<?>> parses = traceParses(FilterPredicate.or(FilterPredicate.eq("id", 1500L),
                FilterPredicate.eq("id", 1700L)), "value");

        assertThat(parses).filteredOn(OffsetIndex.class::equals).hasSize(2);
        assertThat(parses).filteredOn(ColumnIndex.class::equals).hasSize(1);
    }

    private static List<Class<?>> traceParses(FilterPredicate filter, String... columns) throws Exception {
        Thread reading = Thread.currentThread();
        List<Class<?>> parses = new CopyOnWriteArrayList<>();
        PageFilterEvaluator.parseObserver = structure -> {
            if (Thread.currentThread() == reading) {
                parses.add(structure);
            }
        };
        try (HardwoodContext context = HardwoodContext.create(0);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE), context);
                ColumnReaders readers = reader.buildColumnReaders(ColumnProjection.columns(columns))
                        .filter(filter).build()) {
            while (readers.nextBatch()) {
                assertThat(readers.getRecordCount()).isPositive();
            }
        }
        finally {
            PageFilterEvaluator.parseObserver = null;
        }
        return parses;
    }
}
