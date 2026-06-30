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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

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
    void columnReaderPrunesRowGroupForAbsentValue() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            try (ColumnReader col = reader.buildColumnReader("code").filter(ABSENT).build()) {
                assertThat(countRows(col)).isZero();
            }
            try (ColumnReader col = reader.buildColumnReader("code").filter(PRESENT).build()) {
                assertThat(countRows(col)).isEqualTo(1);
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
                List<Long> matchedIds = new ArrayList<>();
                while (code.nextBatch() & id.nextBatch()) {
                    int count = code.getRecordCount();
                    int[] codes = code.getInts();
                    long[] ids = id.getLongs();
                    for (int i = 0; i < count; i++) {
                        // Exact filtering: every returned row satisfies the predicate, batch-independent.
                        assertThat(codes[i]).as("exact filter yields only code==3").isEqualTo(3);
                        matchedIds.add(ids[i]);
                    }
                }
                // code = id*3, so code==3 matches exactly one row, id==1 — asserted once over the
                // full result rather than per row, so it holds regardless of how batches split.
                assertThat(matchedIds).containsExactly(1L);
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
                int total = 0;
                while (rows.hasNext()) {
                    rows.next();
                    assertThat(rows.getInt("code")).isEqualTo(3);
                    total++;
                }
                assertThat(total).isEqualTo(1);
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
