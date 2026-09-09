/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.Validity;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The row-oriented layer is an adapter over the columnar core, not a second write path: the
/// same logical data written through [ColumnWriter#writeBatch] and through [RowWriter]
/// must produce byte-identical files.
///
/// This is the strongest available assertion that the layer makes no paging, dictionary,
/// statistics or row-group decision of its own, and it fails loudly if it ever starts to.
/// The columnar side deliberately supplies junk in the value slots the levels mark absent —
/// under a null struct, at a null entry — which the row layer fills with its own placeholder;
/// identical files prove neither leaks into the encoded bytes.
class RowWriterEquivalenceTest {

    @Test
    void flatColumnsWithNullsMatchTheColumnarPath() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .addColumn("score", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .build();

        int records = 500;
        int[] ids = new int[records];
        byte[][] names = new byte[records][];
        boolean[] nameNulls = new boolean[records];
        double[] scores = new double[records];
        boolean[] scoreNulls = new boolean[records];
        for (int i = 0; i < records; i++) {
            ids[i] = i;
            nameNulls[i] = i % 3 == 0;
            names[i] = nameNulls[i] ? bytes("junk") : bytes("n" + i);
            scoreNulls[i] = i % 5 == 0;
            scores[i] = scoreNulls[i] ? Double.NaN : i * 1.5;
        }

        byte[] columnar = writeColumnar(schema, batch -> batch
                .ints(0, ids)
                .bytes(1, names, nameNulls)
                .doubles(2, scores, scoreNulls));

        byte[] rowOriented = writeRows(schema, rows -> {
            for (int i = 0; i < records; i++) {
                int index = i;
                rows.writeRow(row -> {
                    // A null name is spelled as a null value, a null score by leaving the
                    // field unset — the columnar path fills both slots with junk instead.
                    row.setInt("id", index).setString("name", index % 3 == 0 ? null : "n" + index);
                    if (index % 5 != 0) {
                        row.setDouble("score", index * 1.5);
                    }
                });
            }
        });

        assertThat(rowOriented).isEqualTo(columnar);
    }

    @Test
    void nestedShapesMatchTheColumnarPath() throws Exception {
        FileSchema schema = nestedSchema();

        Validity addressNulls = Validity.ofNulls(new boolean[] { false, true, false });
        Validity tagNulls = Validity.ofNulls(new boolean[] { false, true, false });
        Validity propNulls = Validity.ofNulls(new boolean[] { false, false, true });

        byte[] columnar = writeColumnar(schema, batch -> batch
                .ints(0, new int[] { 1, 2, 3 })
                .struct("address", addressNulls)
                .bytes("address.city", new byte[][] { bytes("Berlin"), bytes("junk"), bytes("Aarhus") })
                .list("tags", new int[] { 0, 2, 2, 2 }, tagNulls)
                .bytes("tags.list.element", new byte[][] { bytes("a"), bytes("junk") },
                        new boolean[] { false, true })
                .map("props", new int[] { 0, 1, 1, 1 }, propNulls)
                .bytes("props.key_value.key", new byte[][] { bytes("x") })
                .longs("props.key_value.value", new long[] { 1 }));

        byte[] rowOriented = writeRows(schema, rows -> {
            rows.writeRow(row -> row
                    .setInt("id", 1)
                    .setStruct("address", address -> address.setString("city", "Berlin"))
                    .setList("tags", tags -> tags.addString("a").addNull())
                    .setMap("props", props -> props.addEntry(
                            entry -> entry.setString("key", "x").setLong("value", 1))));
            rows.writeRow(row -> row
                    .setInt("id", 2)
                    .setMap("props", props -> { }));
            rows.writeRow(row -> row
                    .setInt("id", 3)
                    .setStruct("address", address -> address.setString("city", "Aarhus"))
                    .setList("tags", tags -> { }));
        });

        assertThat(rowOriented).isEqualTo(columnar);
    }

    /// The row layer stages records into batches of its own size. Submitting the same records
    /// as batches of that size through the columnar API must land on the same bytes, so the
    /// batch boundary is an arrival detail and not a property of the file.
    @Test
    void manyRecordsAcrossSeveralStagedBatchesMatchTheColumnarPath() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string())
                .build();

        int records = 2_500;
        int batchSize = 1_024;

        ByteBufferOutputFile columnarOut = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(columnarOut, schema)) {
            for (int from = 0; from < records; from += batchSize) {
                int size = Math.min(batchSize, records - from);
                int[] ids = new int[size];
                byte[][] names = new byte[size][];
                boolean[] nulls = new boolean[size];
                for (int i = 0; i < size; i++) {
                    ids[i] = from + i;
                    nulls[i] = ids[i] % 7 == 0;
                    names[i] = nulls[i] ? bytes("junk") : bytes("n" + ids[i]);
                }
                writer.columnWriter().writeBatch(batch -> batch.ints(0, ids).bytes(1, names, nulls));
            }
        }

        byte[] rowOriented = writeRows(schema, rows -> {
            for (int i = 0; i < records; i++) {
                int index = i;
                rows.writeRow(row -> row
                        .setInt("id", index)
                        .setString("name", index % 7 == 0 ? null : "n" + index));
            }
        });

        assertThat(rowOriented).isEqualTo(columnarOut.toByteArray());
    }

    private static FileSchema nestedSchema() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                LogicalType.string()))
                .list("tags", RepetitionType.OPTIONAL, element -> element.primitive(
                        PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, LogicalType.string()))
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();
    }

    /// Populates a batch through the columnar API.
    private interface ColumnarWrite {
        void accept(ColumnBatch batch);
    }

    /// Writes records through the row-oriented API.
    private interface RowWrite {
        void accept(RowWriter rows) throws Exception;
    }

    private static byte[] writeColumnar(FileSchema schema, ColumnarWrite filler) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(filler::accept);
        }
        return out.toByteArray();
    }

    private static byte[] writeRows(FileSchema schema, RowWrite filler) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            filler.accept(writer.rowWriter());
        }
        return out.toByteArray();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
