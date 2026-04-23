/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.VariantType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end recognition of the Parquet `VARIANT` logical-type annotation.
/// Reads the generated `variant_test.parquet` fixture and asserts that:
///
/// - the schema reports the `var` group as a Variant (not a plain struct);
/// - [PqVariant] surfaces the canonical `metadata` / `value` bytes for each
///   row and decodes them to the expected payload.
///
/// The fixture is produced by `simple-datagen.py` + `parquet_variant_annotation.py`
/// (PyArrow writes the `{metadata, value}` struct; the post-processor stamps
/// the `VARIANT(1)` annotation on the footer).
class VariantLogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/variant_test.parquet");

    @Test
    void schemaReportsVariantLogicalType() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            SchemaNode varNode = schema.getField("var");
            assertThat(varNode).isInstanceOf(SchemaNode.GroupNode.class);
            SchemaNode.GroupNode group = (SchemaNode.GroupNode) varNode;
            assertThat(group.isVariant()).isTrue();
            assertThat(group.logicalType()).isInstanceOf(LogicalType.VariantType.class);
            assertThat(((LogicalType.VariantType) group.logicalType()).specVersion()).isEqualTo(1);
        }
    }

    @Test
    void rowReaderSurfacesVariantBytes() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
                RowReader rowReader = fileReader.createRowReader()) {
            // Row 0: BOOLEAN_TRUE
            rowReader.next();
            PqVariant v0 = rowReader.getVariant("var");
            assertThat(v0).isNotNull();
            assertThat(v0.type()).isEqualTo(VariantType.BOOLEAN_TRUE);
            assertThat(v0.asBoolean()).isTrue();

            // Row 1: BOOLEAN_FALSE
            rowReader.next();
            PqVariant v1 = rowReader.getVariant("var");
            assertThat(v1.type()).isEqualTo(VariantType.BOOLEAN_FALSE);
            assertThat(v1.asBoolean()).isFalse();

            // Row 2: INT32(42)
            rowReader.next();
            PqVariant v2 = rowReader.getVariant("var");
            assertThat(v2.type()).isEqualTo(VariantType.INT32);
            assertThat(v2.asInt()).isEqualTo(42);

            // Row 3: short string "hi"
            rowReader.next();
            PqVariant v3 = rowReader.getVariant("var");
            assertThat(v3.type()).isEqualTo(VariantType.STRING);
            assertThat(v3.asString()).isEqualTo("hi");

            assertThat(rowReader.hasNext()).isFalse();
        }
    }
}
