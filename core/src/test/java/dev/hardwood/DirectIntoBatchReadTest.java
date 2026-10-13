/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The cursor path writes a flat numeric page into the batch array, including a
/// page that straddles a batch and a page that contains a null, for both V1 and
/// V2 data pages.
class DirectIntoBatchReadTest {

    private static final int ROWS = 200;

    private static final int BATCH = 3;

    @Test
    void plainDataPageV2NumericPagesStraddleABatch() throws Exception {
        Path path = Paths.get("src/test/resources/differential/diff_types.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            assertV2Doubles(reader, "f64");
            assertV2Longs(reader, "i64");
            assertV2Ints(reader, "i32");
            assertV2Floats(reader, "f32");
        }
    }

    @Test
    void plainDataPageV2NullableNumericPagesStraddleABatch() throws Exception {
        Path path = Paths.get("src/test/resources/differential/diff_nulls.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            assertV2NullableLongs(reader, "val");
        }
    }

    @Test
    void plainSnappyNumericPagesStraddleABatch() throws Exception {
        byte[] file = writePlain();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            assertDoubles(reader, "d", expectedDoubles(), false);
            assertLongs(reader, "l");
            assertInts(reader, "i");
            assertFloats(reader, "f");
            assertDoubles(reader, "dn", expectedDoubles(), true);
        }
    }

    @Test
    void byteStreamSplitDoublesStraddleABatch() throws Exception {
        byte[] file = write(schema("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED),
                WriterConfig.builder()
                        .encoding(ColumnEncoding.BYTE_STREAM_SPLIT)
                        .codec(CompressionCodec.SNAPPY)
                        .pageTargetBytes(64)
                        .build(),
                false);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            assertDoubles(reader, "d", expectedDoubles(), false);
        }
    }

    @Test
    void headCutsACursorPage() throws Exception {
        byte[] file = writePlain();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                RowReader rows = reader.buildRowReader().head(5).build()) {
            List<Double> actual = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                actual.add(rows.getDouble("d"));
            }
            assertThat(actual).containsExactly(0.5, 1.5, 2.5, 3.5, 4.5);
        }
    }

    private static byte[] writePlain() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("l", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("i", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED)
                .addColumn("dn", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .build();
        return write(schema, WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .codec(CompressionCodec.SNAPPY)
                .pageTargetBytes(64)
                .build(), true);
    }

    private static byte[] write(FileSchema schema, WriterConfig config, boolean wide) throws Exception {
        double[] doubles = expectedDoubles();
        long[] longs = new long[ROWS];
        int[] ints = new int[ROWS];
        float[] floats = new float[ROWS];
        boolean[] nulls = new boolean[ROWS];
        for (int r = 0; r < ROWS; r++) {
            longs[r] = r;
            ints[r] = r;
            floats[r] = r + 0.25f;
            nulls[r] = r % 5 == 0;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> {
                batch.doubles(0, doubles);
                if (wide) {
                    batch.longs(1, longs).ints(2, ints).floats(3, floats).doubles(4, doubles, nulls);
                }
            });
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static double[] expectedDoubles() {
        double[] values = new double[ROWS];
        for (int r = 0; r < ROWS; r++) {
            values[r] = r + 0.5;
        }
        return values;
    }

    private static void assertDoubles(ParquetFileReader reader, String column, double[] expected, boolean nullable)
            throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                assertThat(col.getRecordCount()).isLessThanOrEqualTo(BATCH);
                double[] values = col.getDoubles();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    boolean absent = nullable && row % 5 == 0;
                    assertThat(col.getLeafValidity().isNull(i)).isEqualTo(absent);
                    if (!absent) {
                        assertThat(values[i]).isEqualTo(expected[row]);
                    }
                    row++;
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    private static void assertLongs(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                long[] values = col.getLongs();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row);
                    row++;
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    private static void assertInts(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                int[] values = col.getInts();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row);
                    row++;
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    private static void assertFloats(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                float[] values = col.getFloats();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row + 0.25f);
                    row++;
                }
            }
            assertThat(row).isEqualTo(ROWS);
        }
    }

    private static void assertV2Doubles(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                assertThat(col.getRecordCount()).isLessThanOrEqualTo(BATCH);
                double[] values = col.getDoubles();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row / 8.0);
                    row++;
                }
            }
            assertThat(row).isEqualTo(200);
        }
    }

    private static void assertV2Longs(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                assertThat(col.getRecordCount()).isLessThanOrEqualTo(BATCH);
                long[] values = col.getLongs();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row * 1000L - 50);
                    row++;
                }
            }
            assertThat(row).isEqualTo(200);
        }
    }

    private static void assertV2Ints(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                assertThat(col.getRecordCount()).isLessThanOrEqualTo(BATCH);
                int[] values = col.getInts();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row - 100);
                    row++;
                }
            }
            assertThat(row).isEqualTo(200);
        }
    }

    private static void assertV2Floats(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                assertThat(col.getRecordCount()).isLessThanOrEqualTo(BATCH);
                float[] values = col.getFloats();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    assertThat(values[i]).isEqualTo(row * 0.5f);
                    row++;
                }
            }
            assertThat(row).isEqualTo(200);
        }
    }

    private static void assertV2NullableLongs(ParquetFileReader reader, String column) throws Exception {
        try (ColumnReader col = reader.buildColumnReader(column).batchSize(BATCH).build()) {
            int row = 0;
            while (col.nextBatch()) {
                assertThat(col.getRecordCount()).isLessThanOrEqualTo(BATCH);
                long[] values = col.getLongs();
                for (int i = 0; i < col.getRecordCount(); i++) {
                    boolean absent = row % 3 == 0;
                    assertThat(col.getLeafValidity().isNull(i)).isEqualTo(absent);
                    if (!absent) {
                        assertThat(values[i]).isEqualTo(row * 2L);
                    }
                    row++;
                }
            }
            assertThat(row).isEqualTo(150);
        }
    }

    private static FileSchema schema(String name, PhysicalType type, RepetitionType repetition) {
        return FileSchema.builder("schema").addColumn(name, type, repetition).build();
    }
}
