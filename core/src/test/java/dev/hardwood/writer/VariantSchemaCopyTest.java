/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.VariantType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// A `VARIANT` group is written only under a schema read from an existing file, its `metadata`
/// and `value` children as the binaries they are, through either API. Each test writes under the
/// schema of `variant_test.parquet`, including a row whose variant is null, and reads the
/// variants back.
class VariantSchemaCopyTest {

    private static final Path FILE = Paths.get("src/test/resources/variant_test.parquet");

    @Test
    void rowsCopiedUnderTheFileSchemaReadBack() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileReader source = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = source.getFileSchema();
            try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema);
                    RowReader rows = source.rowReader()) {
                RowWriter rowWriter = writer.rowWriter();
                while (rows.hasNext()) {
                    rows.next();
                    PqVariant variant = rows.getVariant("var");
                    rowWriter.writeRow(row -> row
                            .setInt("id", rows.getInt("id"))
                            .setVariant("var", variant));
                }
                rowWriter.writeRow(row -> row.setInt("id", 5).setNull("var"));
            }
        }

        try (ParquetFileReader copy = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            SchemaNode.GroupNode var = (SchemaNode.GroupNode) copy.getFileSchema().getField("var");
            assertThat(var.logicalType()).isEqualTo(new LogicalType.VariantType(1));

            try (RowReader rows = copy.rowReader()) {
                rows.next();
                assertThat(rows.getVariant("var").type()).isEqualTo(VariantType.BOOLEAN_TRUE);
                rows.next();
                assertThat(rows.getVariant("var").type()).isEqualTo(VariantType.BOOLEAN_FALSE);
                rows.next();
                PqVariant int32 = rows.getVariant("var");
                assertThat(int32.type()).isEqualTo(VariantType.INT32);
                assertThat(int32.asInt()).isEqualTo(42);
                rows.next();
                assertThat(rows.getVariant("var").asString()).isEqualTo("hi");
                rows.next();
                assertThat(rows.isNull("var")).isTrue();
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void columnBatchesUnderTheFileSchemaReadBack() throws Exception {
        byte[] emptyMetadata = { 0x01, 0x00, 0x00 };
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileReader source = ParquetFileReader.open(InputFile.of(FILE));
                ParquetFileWriter writer = ParquetFileWriter.create(out, source.getFileSchema())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .ints("id", new int[] { 1, 2 })
                    .struct("var", Validity.ofNulls(new boolean[] { false, true }))
                    .bytes("var.metadata", new byte[][] { emptyMetadata, new byte[0] })
                    .bytes("var.value", new byte[][] { { 0x14, 42, 0, 0, 0 }, new byte[0] }));
        }

        try (ParquetFileReader copy = ParquetFileReader.open(InputFile.of(out.buffer()));
                RowReader rows = copy.rowReader()) {
            rows.next();
            assertThat(rows.getVariant("var").asInt()).isEqualTo(42);
            rows.next();
            assertThat(rows.isNull("var")).isTrue();
            assertThat(rows.hasNext()).isFalse();
        }
    }
}
