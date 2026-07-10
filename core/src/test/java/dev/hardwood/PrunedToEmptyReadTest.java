/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Exercises the empty-projection short-circuit that
/// [ParquetFileReader#buildColumnReaders] takes when row-group pruning drops
/// every row group: it exposes the projected columns as immediately-exhausted
/// no-op readers ([ColumnReader#exhausted]) instead of building per-column
/// worker threads, batch buffers, and (on the filtered path) a selection
/// engine. Both the plain-projection path (dropped by a [RowGroupPredicate])
/// and the exact-filter path (dropped by statistics) are covered, for flat and
/// nested columns.
class PrunedToEmptyReadTest {

    private static final Path INT_FIXTURE = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final Path LIST_FIXTURE = Paths.get("src/test/resources/filter_pushdown_list.parquet");

    /// A byte range that lies entirely past the end of the file keeps no row
    /// group under the midpoint rule, so the work list is empty.
    private static RowGroupPredicate dropAll(Path fixture) {
        long len = fixture.toFile().length();
        return RowGroupPredicate.byteRange(len + 1, len + 100);
    }

    @Test
    void plainProjectionPrunedToEmptyExposesExhaustedReaders() throws Exception {
        ColumnProjection projection = ColumnProjection.columns("id", "value", "label");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FIXTURE));
             ColumnReaders cols = reader.buildColumnReaders(projection)
                     .filter(dropAll(INT_FIXTURE))
                     .build()) {

            assertThat(cols.getColumnCount()).isEqualTo(3);
            assertThat(cols.nextBatch()).isFalse();

            for (String name : new String[] {"id", "value", "label"}) {
                assertThat(cols.getColumnReader(name).nextBatch()).isFalse();
            }
            assertThat(cols.getColumnReader(0).nextBatch()).isFalse();
        }
    }

    @Test
    void filterPredicatePrunedToEmptyExposesExhaustedReaders() throws Exception {
        // Every `value` is in 1..300, so gt(value, 1000) drops all three row groups
        // by statistics — the exact-filter path builds no selection engine.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FIXTURE));
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns("id", "value"))
                     .filter(FilterPredicate.gt("value", 1000L))
                     .build()) {

            assertThat(cols.getColumnCount()).isEqualTo(2);
            assertThat(cols.nextBatch()).isFalse();
            assertThat(cols.getColumnReader("id").nextBatch()).isFalse();
            assertThat(cols.getColumnReader("value").nextBatch()).isFalse();
        }
    }

    @Test
    void prunedNestedReaderPreservesLayerModel() throws Exception {
        // `scores` is list<int32>: one REPEATED layer. The exhausted reader must
        // report the same structural metadata a live reader would, since
        // getLayerCount()/getLayerKind() are documented stable and safe to call
        // before the first batch.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LIST_FIXTURE));
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns("id", "scores.list.element"))
                     .filter(dropAll(LIST_FIXTURE))
                     .build()) {

            ColumnReader id = cols.getColumnReader("id");
            ColumnReader scores = cols.getColumnReader("scores.list.element");

            assertThat(id.getLayerCount()).isEqualTo(0);
            assertThat(scores.getLayerCount()).isEqualTo(1);
            assertThat(scores.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);

            assertThat(cols.nextBatch()).isFalse();
            assertThat(scores.nextBatch()).isFalse();
        }
    }
}
