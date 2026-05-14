/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.List;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit coverage for [AvroSchemaConverter] pieces that are awkward to exercise
/// through `AvroRowReaderTest` alone. Notably the VARIANT group conversion,
/// which emits a two-field `RECORD{metadata: bytes, value: bytes}` to match
/// parquet-java's AvroParquetReader output and hide the physical
/// `typed_value` shredding from consumers.
class AvroSchemaConverterTest {

    @Test
    void variantGroupBecomesCanonicalMetadataValueRecord() {
        FileSchema schema = buildVariantSchema(/* includeTypedValue= */ false);
        Schema avroSchema = AvroSchemaConverter.convert(schema);
        Schema.Field varField = avroSchema.getField("var");
        assertThat(varField).isNotNull();

        // Variant column is OPTIONAL → UNION[null, record]; pick the record branch.
        Schema varRecord = pickRecordBranch(varField.schema());
        assertThat(varRecord.getFields()).hasSize(2);
        assertThat(varRecord.getField("metadata")).isNotNull();
        assertThat(varRecord.getField("metadata").schema().getType()).isEqualTo(Schema.Type.BYTES);
        assertThat(varRecord.getField("value")).isNotNull();
        assertThat(varRecord.getField("value").schema().getType()).isEqualTo(Schema.Type.BYTES);
    }

    @Test
    void shreddedVariantAlsoHidesTypedValueFromAvroOutput() {
        FileSchema schema = buildVariantSchema(/* includeTypedValue= */ true);
        Schema avroSchema = AvroSchemaConverter.convert(schema);
        Schema varRecord = pickRecordBranch(avroSchema.getField("var").schema());

        // The physical column carries a typed_value sibling, but the Avro view is
        // always the canonical {metadata, value} pair.
        assertThat(varRecord.getFields()).hasSize(2);
        assertThat(varRecord.getField("typed_value")).isNull();
    }

    /// `pa.null()` columns carry the NULL logical type on an OPTIONAL primitive.
    /// The Avro schema must be a bare `NULL` — not `union [null, null]`, which
    /// the Avro spec forbids.
    @Test
    void nullLogicalTypeBecomesBareAvroNull() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement nothing = new SchemaElement("nothing", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, new LogicalType.NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, nothing));

        Schema avroSchema = AvroSchemaConverter.convert(schema);
        Schema.Field field = avroSchema.getField("nothing");
        assertThat(field).isNotNull();
        assertThat(field.schema().getType()).isEqualTo(Schema.Type.NULL);
        // Guard against regressing to `union [null, null]` — still NULL on the
        // top branch but illegal Avro that would throw at schema construction.
        assertThat(field.schema().isUnion()).isFalse();
    }

    /// `list<null>` with an OPTIONAL element must produce `array<null>`, not
    /// `array<union [null, null]>`.
    @Test
    void listOfNullElementsBecomesArrayOfBareNull() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement listGroup = new SchemaElement("nulls", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, new LogicalType.ListType());
        SchemaElement listInner = new SchemaElement("list", null, null, RepetitionType.REPEATED,
                1, null, null, null, null, null);
        SchemaElement element = new SchemaElement("element", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, new LogicalType.NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, listGroup, listInner, element));

        Schema avroSchema = AvroSchemaConverter.convert(schema);
        Schema listField = pickArrayBranch(avroSchema.getField("nulls").schema());
        Schema elementSchema = listField.getElementType();
        assertThat(elementSchema.getType()).isEqualTo(Schema.Type.NULL);
        assertThat(elementSchema.isUnion()).isFalse();
    }

    /// `map<string, null>` with OPTIONAL values must produce `map<null>`, not
    /// `map<union [null, null]>`.
    @Test
    void mapWithNullValuesBecomesMapOfBareNull() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement mapGroup = new SchemaElement("m", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, new LogicalType.MapType());
        SchemaElement kv = new SchemaElement("key_value", null, null, RepetitionType.REPEATED,
                2, null, null, null, null, null);
        SchemaElement key = new SchemaElement("key", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, new LogicalType.StringType());
        SchemaElement value = new SchemaElement("value", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, new LogicalType.NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, mapGroup, kv, key, value));

        Schema avroSchema = AvroSchemaConverter.convert(schema);
        Schema mapField = pickMapBranch(avroSchema.getField("m").schema());
        Schema valueSchema = mapField.getValueType();
        assertThat(valueSchema.getType()).isEqualTo(Schema.Type.NULL);
        assertThat(valueSchema.isUnion()).isFalse();
    }

    private static Schema pickArrayBranch(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.ARRAY) {
            return fieldSchema;
        }
        for (Schema sub : fieldSchema.getTypes()) {
            if (sub.getType() == Schema.Type.ARRAY) {
                return sub;
            }
        }
        throw new AssertionError("No array branch in union: " + fieldSchema);
    }

    private static Schema pickMapBranch(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.MAP) {
            return fieldSchema;
        }
        for (Schema sub : fieldSchema.getTypes()) {
            if (sub.getType() == Schema.Type.MAP) {
                return sub;
            }
        }
        throw new AssertionError("No map branch in union: " + fieldSchema);
    }

    private static Schema pickRecordBranch(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.RECORD) {
            return fieldSchema;
        }
        for (Schema sub : fieldSchema.getTypes()) {
            if (sub.getType() == Schema.Type.RECORD) {
                return sub;
            }
        }
        throw new AssertionError("No record branch in union: " + fieldSchema);
    }

    private static FileSchema buildVariantSchema(boolean includeTypedValue) {
        int varChildren = includeTypedValue ? 3 : 2;
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement var = new SchemaElement("var", null, null, RepetitionType.OPTIONAL,
                varChildren, null, null, null, null, new LogicalType.VariantType(1));
        SchemaElement metadata = new SchemaElement("metadata", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, null);
        if (!includeTypedValue) {
            return FileSchema.fromSchemaElements(List.of(root, var, metadata, value));
        }
        SchemaElement typedValue = new SchemaElement("typed_value", PhysicalType.INT64, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, var, metadata, value, typedValue));
    }
}
