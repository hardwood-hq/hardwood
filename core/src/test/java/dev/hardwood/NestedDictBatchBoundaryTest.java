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
import java.util.Objects;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.Validity;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression test for value corruption when a variable-length leaf nested in a
/// struct is decoded across a batch boundary.
///
/// The drain (`NestedColumnWorker`) reused one bytes buffer across batches and
/// published each batch by aliasing it, so assembling the next batch overwrote
/// the previous batch's still-unread values. It triggers whenever a single
/// nested variable-length leaf spans more than one batch — no sibling column is
/// involved.
///
/// The row reader auto-sizes its batch to `6 MB / row width`, so a lone string
/// column would need > ~393k rows to cross a boundary. The [ColumnReader] path
/// shares the same worker but takes an explicit batch size, so the bug
/// reproduces on a tiny fixture: `nested_dict_batch_boundary.parquet` (from
/// `tools/simple-datagen.py`) holds 300 rows of a dictionary-encoded
/// `nested.name` string with distinct values, and a batch size of 64 spans five
/// batches. Every value must match what was written.
class NestedDictBatchBoundaryTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/nested_dict_batch_boundary.parquet");
    private static final int TOTAL_ROWS = 300;
    private static final int BATCH_SIZE = 64;

    /// `nested` is null on every 7th row; within a present struct `name` is null
    /// on every 3rd row; otherwise the name is the distinct value `value_NNNN`.
    private static String expectedName(int row) {
        if (row % 7 == 0 || row % 3 == 0) {
            return null;
        }
        return String.format("value_%04d", row);
    }

    @Test
    void columnReaderAcrossBatchBoundariesReturnsTheWrittenValues() throws Exception {
        List<String> examples = new ArrayList<>();
        int mismatches = 0;
        int row = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             // The sole leaf column is `nested.name`; a small batch forces the
             // multi-batch path the auto-sized row reader only reaches at scale.
             ColumnReader name = reader.buildColumnReader(0).batchSize(BATCH_SIZE).build()) {
            while (name.nextBatch()) {
                int count = name.getRecordCount();
                String[] values = name.getStrings();
                Validity validity = name.getLeafValidity();
                boolean hasNulls = validity.hasNulls();
                for (int i = 0; i < count; i++) {
                    String got = hasNulls && validity.isNull(i) ? null : values[i];
                    if (!Objects.equals(got, expectedName(row))) {
                        mismatches++;
                        if (examples.size() < 10) {
                            examples.add(String.format(
                                    "row %d: got=%s (expected %s)", row, got, expectedName(row)));
                        }
                    }
                    row++;
                }
            }
        }

        assertThat(row).as("total rows").isEqualTo(TOTAL_ROWS);
        assertThat(mismatches)
                .as("values decoded to the wrong content (first examples: %s)", examples)
                .isZero();
    }
}
