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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.Validity;

import static org.assertj.core.api.Assertions.assertThat;

/// The regular nested assembly path (`NestedColumnWorker.assembleRegularPage`)
/// fills each batch by bulk-copying runs of consecutive kept positions. A run is
/// cut wherever a batch boundary falls mid-page, so the reconstructed records must
/// not depend on the batch size. Reading a nullable list column with a tiny batch
/// (splitting almost every page) and with a batch large enough to hold the whole
/// column must yield identical records — the direct regression guard that
/// splitting a run across a publish preserves values, list boundaries, and nulls.
class NestedRegularBatchInvarianceTest {

    private static final Path FILE =
            Paths.get("src/test/resources/batch_array_identity_test.parquet");
    private static final String COLUMN = "nums.list.element";

    @Test
    void regularNestedReadIsBatchSizeInvariant() throws Exception {
        List<int[]> small = readIntLists(2);
        List<int[]> large = readIntLists(4096);

        assertThat(small).as("record count (small vs large batch)").hasSameSizeAs(large);
        for (int r = 0; r < small.size(); r++) {
            assertThat(small.get(r)).as("record %d (batchSize 2 vs 4096)", r).isEqualTo(large.get(r));
        }
    }

    /// Reads every list record as an `int[]` (or `null` for a null list),
    /// flattened across batches, at the given batch size.
    private static List<int[]> readIntLists(int batchSize) throws Exception {
        List<int[]> records = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader(COLUMN).batchSize(batchSize).build()) {
            while (col.nextBatch()) {
                int recordCount = col.getRecordCount();
                int[] offsets = col.getLayerOffsets(0);
                Validity validity = col.getLayerValidity(0);
                int[] values = col.getInts();
                for (int r = 0; r < recordCount; r++) {
                    if (validity.hasNulls() && validity.isNull(r)) {
                        records.add(null);
                        continue;
                    }
                    int[] list = new int[offsets[r + 1] - offsets[r]];
                    System.arraycopy(values, offsets[r], list, 0, list.length);
                    records.add(list);
                }
            }
        }
        return records;
    }
}
