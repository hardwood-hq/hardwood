/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies the byte-budgeted batch sizing the column- and row-reader paths
/// share: the size depends on the projected columns' physical widths scaled by
/// their list fan-out, clamped to at most `MAX_BATCH`, rather than being a fixed
/// record count.
class BatchSizingTest {

    /// `int_col` (INT32), `long_col` (INT64), `float_col` (FLOAT),
    /// `double_col` (DOUBLE), `bool_col` (BOOLEAN), `string_col` (BYTE_ARRAY).
    private static final Path PRIMITIVES = Path.of("src/test/resources/primitive_types_test.parquet");

    private static final long TARGET_BYTES = 6L * 1024 * 1024;

    @Test
    void narrowProjectionClampsToMaxBatch() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRIMITIVES))) {
            // A single 1-byte column would compute 6 Mi rows; the clamp caps it.
            assertThat(sizeFor(reader, "bool_col")).isEqualTo(BatchSizing.MAX_BATCH);
            // A single 8-byte column computes 786 432 rows, still above the clamp.
            assertThat(sizeFor(reader, "long_col")).isEqualTo(BatchSizing.MAX_BATCH);
        }
    }

    @Test
    void batchSizeShrinksAsProjectionWidens() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRIMITIVES))) {
            int single = sizeFor(reader, "long_col");
            int wide = sizeFor(reader, "int_col", "long_col", "float_col",
                    "double_col", "bool_col", "string_col");

            // The wider projection must size smaller — the whole point of byte
            // budgeting over a fixed record count.
            assertThat(wide).isLessThan(single);

            // Width sum: 4 + 8 + 4 + 8 + 1 + 16 = 41 bytes/row.
            assertThat(wide).isEqualTo((int) (TARGET_BYTES / 41));
        }
    }

    @Test
    void sizeMatchesByteBudgetForAModeratelyWideProjection() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRIMITIVES))) {
            // long_col (8) + double_col (8) + string_col (16) = 32 bytes/row.
            int expected = (int) (TARGET_BYTES / 32);
            assertThat(sizeFor(reader, "long_col", "double_col", "string_col")).isEqualTo(expected);
            assertThat(expected).isBetween(16_384, BatchSizing.MAX_BATCH);
        }
    }

    @Test
    void fanoutScalesTheByteBudgetByValuesPerRow() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRIMITIVES))) {
            FileSchema schema = reader.getFileSchema();
            ProjectedSchema projected =
                    ProjectedSchema.create(schema, ColumnProjection.columns("float_col"));

            // Fan-out 1 (a flat float column): 4 bytes/row budgets far above the clamp.
            assertThat(BatchSizing.computeOptimalBatchSize(projected, new double[] { 1.0 }))
                    .isEqualTo(BatchSizing.MAX_BATCH);

            // Fan-out 768 (a 768-wide LIST<float32>): each row is 768 * 4 = 3072 bytes,
            // so the batch follows the byte budget down to 2 048 rows — far below the
            // 16 384 rows a fan-out-blind sizing would have forced, which would have made
            // the value array ~768x the L2 target.
            assertThat(BatchSizing.computeOptimalBatchSize(projected, new double[] { 768.0 }))
                    .isEqualTo((int) (TARGET_BYTES / (768 * 4)))
                    .isEqualTo(2048)
                    .isLessThan(16_384);
        }
    }

    private static int sizeFor(ParquetFileReader reader, String... columns) {
        FileSchema schema = reader.getFileSchema();
        ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.columns(columns));
        return BatchSizing.computeOptimalBatchSize(projected);
    }
}
