/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Copies a nested file through [NestedColumnCopier] and asserts the copy is byte-identical to
/// its source. Nesting is the shape where the two columnar APIs describe the same file most
/// differently, so this pins that a caller can in fact bridge them without going through the
/// row layer, and that the bridge is exact rather than merely equivalent.
///
/// The fixture is sized so the reader hands the copy more than one batch, on boundaries that
/// have nothing to do with the ones the source was written on. Byte-identity across that
/// mismatch is the assertion worth making, because it is what makes a batch an arrival unit
/// rather than a property of the file — and it is what lets a copier submit whatever the
/// reader gives it. It holds while the row-group target is not crossed inside an arriving
/// batch; a target small enough to be crossed mid-batch moves the cut with the arrival
/// granularity, and the copy then reproduces the data but not the layout.
class ColumnarNestedCopyTest {

    @Test
    void aNestedFileCopiedThroughTheColumnarApisIsByteIdentical() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .struct("address", RepetitionType.OPTIONAL, address -> address
                        .addColumn("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                                LogicalType.string())
                        .addColumn("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .list("tags", RepetitionType.OPTIONAL, element -> element.primitive(
                        PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.string()))
                .map("props", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY,
                        LogicalType.string(),
                        value -> value.primitive(PhysicalType.INT64, RepetitionType.OPTIONAL))
                .build();

        byte[] source = writeSource(schema);

        int batches;
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileReader reader = open(source);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            batches = NestedColumnCopier.copy(reader, schema, writer);
        }

        // The reader's batches do not coincide with the ones the source was written in, so
        // the copy is only a pass-through if they need not: a batch is an arrival unit, not a
        // property of the file. Without this the copier's per-batch reset of already-set
        // groups and offsets restarting at a batch would never be exercised.
        assertThat(batches).isGreaterThan(1);
        assertThat(out.toByteArray()).isEqualTo(source);
    }

    /// Enough records that the reader hands the copy more than one batch, cycling the four
    /// container states every nested column has — present with entries, present and empty,
    /// absent, and (for the struct) present with its own `OPTIONAL` leaf unset — so a batch
    /// boundary falls somewhere inside each of them rather than only between whole shapes.
    private static final int RECORDS = 4_000;

    /// Entries on the wide records' list. The reader sizes a batch to a 6 MB value budget
    /// divided by the bytes one row occupies, and a list's fan-out multiplies that width, so
    /// this is the cheapest lever that makes the file arrive in more than one batch.
    private static final int WIDE_TAGS = 250;

    private static byte[] writeSource(FileSchema schema) throws Exception {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            RowWriter rows = writer.rowWriter();
            for (int i = 0; i < RECORDS; i++) {
                int id = i;
                rows.writeRow(row -> {
                    row.setInt("id", id);
                    switch (id % 4) {
                        case 0 -> row
                                .setStruct("address", a -> a.setString("city", "Berlin" + id)
                                        .setInt("zip", 10115 + id))
                                .setList("tags", t -> {
                                    for (int e = 0; e < WIDE_TAGS; e++) {
                                        t.addString("a" + id + "-" + e);
                                    }
                                })
                                .setMap("props", p -> p.addEntry(
                                        e -> e.setString("key", "x" + id).setLong("value", id)));
                        // An absent struct, an empty list and an absent map.
                        case 1 -> row.setList("tags", t -> { });
                        // A present struct whose OPTIONAL leaf is unset, and an empty map.
                        case 2 -> row
                                .setStruct("address", a -> a.setString("city", "Aarhus" + id))
                                .setMap("props", p -> { });
                        // A null list distinct from case 1's empty one, and a null map value.
                        default -> row
                                .setNull("tags")
                                .setMap("props", p -> p.addEntry(
                                        e -> e.setString("key", "y" + id).setNull("value")));
                    }
                });
            }
        }
        return out.toByteArray();
    }

    private static ParquetFileReader open(byte[] file) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
    }

}
