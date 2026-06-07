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
/// share: the size depends on the projected columns' physical widths and is
/// clamped to `[16 384, MAX_BATCH]`, rather than being a fixed record count.
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

    private static int sizeFor(ParquetFileReader reader, String... columns) {
        FileSchema schema = reader.getFileSchema();
        ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.columns(columns));
        return BatchSizing.computeOptimalBatchSize(projected);
    }
}
