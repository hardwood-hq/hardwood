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
import java.time.Instant;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end bloom-filter pruning through the public reader APIs against
/// `bloom_filter_test.parquet` (one row group, 64 rows). The file's only row group is dropped
/// entirely when the predicate value falls inside the column's statistics range but is proven
/// absent by the bloom filter, so no rows — and on remote backends, no data pages — are read.
///
/// `code` holds multiples of 3: `eq("code", 1)` is in `[0, 189]` (statistics keep) but never
/// written (bloom drops); `eq("code", 3)` is present. Complements
/// [dev.hardwood.internal.predicate.BloomFilterPushDownTest], which asserts the evaluator decision
/// directly; here the assertions run through `ColumnReader`, `ColumnReaders`, and `RowReader`.
class BloomFilterEndToEndTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/bloom_filter_test.parquet");

    private static final FilterPredicate ABSENT = FilterPredicate.eq("code", 1);
    private static final FilterPredicate PRESENT = FilterPredicate.eq("code", 3);

    @Test
    void fixtureCarriesBloomFiltersOnTheFilteredColumns() throws Exception {
        // Precondition for every prune assertion below: `code` and `ts` must actually carry a bloom
        // filter. Without one, an absent in-range value would still yield zero rows via record-level
        // filtering, so the prune tests could pass even if row-group bloom pushdown were broken.
        // BloomFilterPushDownTest separately proves statistics do not drop these values, so a filter
        // present here is what makes the drop attributable to the bloom path.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            assertHasBloomFilter(reader, "code");
            assertHasBloomFilter(reader, "ts");
        }
    }

    @Test
    void columnReaderPrunesRowGroupForAbsentValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (ColumnReader col = reader.buildColumnReader("code").filter(ABSENT).build()) {
                assertThat(countRows(col)).isZero();
            }
            try (ColumnReader col = reader.buildColumnReader("code").filter(PRESENT).build()) {
                // Exactly one row matches; one batch of one record, then exhausted.
                assertThat(col.nextBatch()).isTrue();
                assertThat(col.getRecordCount()).isEqualTo(1);
                assertThat(col.getInts()[0]).isEqualTo(3);
                assertThat(col.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void columnReadersProjectionPrunesRowGroupForAbsentValue() throws Exception {
        ColumnProjection projection = ColumnProjection.columns("code", "id");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (ColumnReaders cols = reader.buildColumnReaders(projection).filter(ABSENT).build()) {
                assertThat(countRows(cols.getColumnReader("code"))).isZero();
            }
            try (ColumnReaders cols = reader.buildColumnReaders(projection).filter(PRESENT).build()) {
                ColumnReader code = cols.getColumnReader("code");
                ColumnReader id = cols.getColumnReader("id");
                assertThat(code.nextBatch() & id.nextBatch()).isTrue();
                assertThat(code.getRecordCount()).isEqualTo(1);
                assertThat(code.getInts()[0]).isEqualTo(3);
                assertThat(id.getLongs()[0]).isEqualTo(1L); // code = id*3, so code==3 is id==1
                assertThat(code.nextBatch() & id.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void rowReaderPrunesRowGroupForAbsentValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (RowReader rows = reader.buildRowReader().filter(ABSENT).build()) {
                assertThat(countRows(rows)).isZero();
            }
            try (RowReader rows = reader.buildRowReader().filter(PRESENT).build()) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getInt("code")).isEqualTo(3);
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void rowReaderPrunesRowGroupForAbsentLogicalTypeValue() throws Exception {
        // `ts` is TIMESTAMP(us, UTC); an Instant predicate resolves to the physical INT64 and is
        // pruned by the bloom filter end-to-end. An odd-second instant is in range but never written.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            FilterPredicate absentTs = FilterPredicate.eq("ts", Instant.parse("2024-01-01T00:00:01Z"));
            try (RowReader rows = reader.buildRowReader().filter(absentTs).build()) {
                assertThat(countRows(rows)).isZero();
            }
            FilterPredicate presentTs = FilterPredicate.eq("ts", Instant.parse("2024-01-01T00:00:02Z"));
            try (RowReader rows = reader.buildRowReader().filter(presentTs).build()) {
                assertThat(countRows(rows)).isEqualTo(1);
            }
        }
    }

    /// Asserts the fixture's single row group carries a bloom filter on `column`, looked up by name
    /// so it does not depend on column ordering.
    private static void assertHasBloomFilter(ParquetFileReader reader, String column) {
        RowGroup rowGroup = reader.getFileMetaData().rowGroups().getFirst();
        ColumnChunk chunk = rowGroup.columns().stream()
                .filter(c -> c.metaData().pathInSchema().toString().equals(column))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No column named '" + column + "' in the fixture"));
        assertThat(chunk.metaData().bloomFilterOffset())
                .as("bloom_filter_offset for column '%s'", column)
                .isNotNull().isPositive();
    }

    private static int countRows(ColumnReader reader) {
        int total = 0;
        while (reader.nextBatch()) {
            total += reader.getRecordCount();
        }
        return total;
    }

    private static int countRows(RowReader reader) {
        int total = 0;
        while (reader.hasNext()) {
            reader.next();
            total++;
        }
        return total;
    }
}
