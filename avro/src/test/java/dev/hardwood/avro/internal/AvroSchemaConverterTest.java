/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static dev.hardwood.metadata.SchemaElement.group;
import static dev.hardwood.metadata.SchemaElement.primitive;
import static dev.hardwood.metadata.SchemaElement.root;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Unit coverage for [AvroSchemaConverter] pieces that are awkward to exercise
/// through `AvroRowReaderTest` alone. Notably the VARIANT group conversion,
/// which emits a two-field `RECORD{metadata: bytes, value: bytes}` to match
/// parquet-java's AvroParquetReader output and hide the physical
/// `typed_value` shredding from consumers.
class AvroSchemaConverterTest {

    @Test
    @Disabled("#895 — enabled in Stage 3")
    void duplicateNestedNamesReproduceSchemaParseFailure() {
        Schema converted = convert(duplicateNestedAddressSchema());

        assertThatThrownBy(() -> new Schema.Parser().parse(converted.toString()))
                .isInstanceOf(SchemaParseException.class)
                .hasMessageContaining("Can't redefine: address");
    }

    @Test
    @Disabled("#895 — enabled in Stage 3")
    void illegalNestedNameReproducesSchemaParseFailure() {
        Schema converted = convert(illegalNestedGroupSchema());

        assertThatThrownBy(converted::toString)
                .isInstanceOf(SchemaParseException.class)
                .hasMessageContaining("Illegal character in: acme.address");
    }

    @Test
    void variantGroupBecomesCanonicalMetadataValueRecord() {
        FileSchema schema = buildVariantSchema(/* includeTypedValue= */ false);
        Schema avroSchema = convert(schema);
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
        Schema avroSchema = convert(schema);
        Schema varRecord = pickRecordBranch(avroSchema.getField("var").schema());

        // The physical column carries a typed_value sibling, but the Avro view is
        // always the canonical {metadata, value} pair.
        assertThat(varRecord.getFields()).hasSize(2);
        assertThat(varRecord.getField("typed_value")).isNull();
    }

    /// The two records convert to the same Avro shape — `{metadata: bytes, value: bytes}` —
    /// so only the plan distinguishes the annotated group from the ordinary one.
    @Test
    void planMarksOnlyTheAnnotatedGroupAsVariant() {
        AvroPlanNode plan = AvroSchemaConverter.plan(variantAndOrdinarySchema(), ColumnProjection.all());

        assertThat(plan.child(0).kind()).isEqualTo(AvroPlanNode.Kind.VARIANT);
        assertThat(plan.child(1).kind()).isEqualTo(AvroPlanNode.Kind.STRUCT);
        assertThat(plan.avro().getFields().get(0).name()).isEqualTo("variant_record");
        assertThat(plan.avro().getFields().get(1).name()).isEqualTo("ordinary_record");
    }

    /// A projection prunes plan children alongside record fields, so a plan child
    /// must still describe the field at its own position once fields are dropped.
    @Test
    void planChildrenStayAlignedUnderProjection() {
        AvroPlanNode plan = AvroSchemaConverter.plan(
                variantAndOrdinarySchema(), ColumnProjection.columns("variant_record"));

        assertThat(plan.avro().getFields()).hasSize(1);
        assertThat(plan.avro().getFields().getFirst().name()).isEqualTo("variant_record");
        assertThat(plan.child(0).kind()).isEqualTo(AvroPlanNode.Kind.VARIANT);
    }

    /// A group is a struct to the converter only when the row reader agrees it is one.
    /// An annotation neither side recognises would otherwise convert to an Avro RECORD
    /// that the reader cannot fill, since the list accessors serve the group's leaf.
    @Test
    void rejectsGroupCarryingAnUnrecognisedAnnotation() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement legacy = new SchemaElement("legacy", null, null, RepetitionType.OPTIONAL,
                1, ConvertedType.MAP_KEY_VALUE, null, null, null, null);
        SchemaElement leaf = primitive("v", PhysicalType.INT32, RepetitionType.REQUIRED);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, legacy, leaf));

        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("legacy")
                .hasMessageContaining("MAP_KEY_VALUE");
    }

    @Test
    void rejectsNonStringKeyedMapNestedInStruct() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement holder = group("holder", RepetitionType.OPTIONAL, 1);
        List<SchemaElement> elements = new ArrayList<>(List.of(rootElement, holder));
        elements.addAll(intKeyedMap("nested"));

        assertRejectsMap(FileSchema.fromSchemaElements(elements), "holder.nested", "INT32");
    }

    @Test
    void rejectsNonStringKeyedMapUsedAsListElement() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement items = group("items", RepetitionType.OPTIONAL, 1, new LogicalType.ListType());
        SchemaElement list = group("list", RepetitionType.REPEATED, 1);
        List<SchemaElement> elements = new ArrayList<>(List.of(rootElement, items, list));
        elements.addAll(intKeyedMap("element"));

        assertRejectsMap(FileSchema.fromSchemaElements(elements), "items.element", "INT32");
    }

    @Test
    void rejectsNonStringKeyedMapUsedAsMapValue() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement outer = group("outer", RepetitionType.OPTIONAL, 1, new LogicalType.MapType());
        SchemaElement outerKv = group("key_value", RepetitionType.REPEATED, 2);
        SchemaElement outerKey = primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                new LogicalType.StringType());
        List<SchemaElement> elements = new ArrayList<>(List.of(rootElement, outer, outerKv, outerKey));
        elements.addAll(intKeyedMap("value"));

        assertRejectsMap(FileSchema.fromSchemaElements(elements), "outer.value", "INT32");
    }

    /// Avro has no key schema, so every key it can represent arrives as a string.
    /// That is exactly the set [AvroSchemaConverter] renders as an Avro `STRING`:
    /// a `BYTE_ARRAY` annotated `STRING`, `ENUM` or `JSON`.
    @Test
    void acceptsEveryMapKeyAvroRendersAsString() {
        assertMapConverts(mapKeyedBy(
                convertedPrimitive("key", PhysicalType.BYTE_ARRAY, null, new LogicalType.StringType())));
        assertMapConverts(mapKeyedBy(
                convertedPrimitive("key", PhysicalType.BYTE_ARRAY, null, new LogicalType.EnumType())));
        assertMapConverts(mapKeyedBy(
                convertedPrimitive("key", PhysicalType.BYTE_ARRAY, null, new LogicalType.JsonType())));
    }

    /// Writers predating the logical-type union annotate string keys with the legacy
    /// `UTF8` converted type alone. Those maps are ordinary string-keyed maps and
    /// must not be rejected.
    @Test
    void acceptsMapKeyCarryingOnlyTheLegacyUtf8ConvertedType() {
        assertMapConverts(mapKeyedBy(
                convertedPrimitive("key", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, null)));
    }

    /// An unannotated `BYTE_ARRAY` key holds arbitrary bytes. Decoding those as UTF-8
    /// would substitute replacement characters, and two distinct keys that both decode
    /// to the same replacement sequence would collide into one Avro map entry —
    /// dropping the other. Reject instead of mangling.
    @Test
    void rejectsUnannotatedBinaryMapKey() {
        assertRejectsMap(mapKeyedBy(convertedPrimitive("key", PhysicalType.BYTE_ARRAY, null, null)),
                "m", "BYTE_ARRAY with no logical annotation");
    }

    /// A `UUID` key converts to an Avro `STRING`, but it is 16 raw bytes rather than
    /// text — the string accessor would hand back mojibake.
    @Test
    void rejectsUuidMapKey() {
        assertRejectsMap(mapKeyedBy(convertedPrimitive("key", PhysicalType.FIXED_LEN_BYTE_ARRAY,
                null, new LogicalType.UuidType())), "m", "UUID");
    }

    /// The Parquet spec requires a primitive map key; a group in the key position has
    /// no value for the string accessor to read.
    @Test
    void rejectsMapWithGroupKey() {
        List<SchemaElement> elements = List.of(
                root("root", 1),
                group("m", RepetitionType.OPTIONAL, 1, new LogicalType.MapType()),
                group("key_value", RepetitionType.REPEATED, 2),
                group("key", RepetitionType.REQUIRED, 1),
                convertedPrimitive("part", PhysicalType.INT32, null, null),
                primitive("value", PhysicalType.INT64, RepetitionType.OPTIONAL));

        assertRejectsMap(FileSchema.fromSchemaElements(elements), "m", "group 'key'");
    }

    private static void assertMapConverts(FileSchema schema) {
        Schema map = pickMapBranch(convert(schema).getField("m").schema());
        assertThat(map.getType()).isEqualTo(Schema.Type.MAP);
    }

    /// A map key Avro cannot represent is reported under the map's full path, so a
    /// map nested below the root is diagnosable without hunting for which `element`
    /// or `value` position the message means.
    private static void assertRejectsMap(FileSchema schema, String mapPath, String keyType) {
        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Map '" + mapPath + "'")
                .hasMessageContaining(keyType);
    }

    private static List<SchemaElement> intKeyedMap(String name) {
        return keyedMap(name, convertedPrimitive("key", PhysicalType.INT32, null, null));
    }

    /// A `map<?, int64>` whose key element is given, so key handling can be varied
    /// without restating the surrounding MAP / `key_value` shape.
    private static List<SchemaElement> keyedMap(String name, SchemaElement key) {
        return List.of(
                group(name, RepetitionType.OPTIONAL, 1, new LogicalType.MapType()),
                group("key_value", RepetitionType.REPEATED, 2),
                key,
                primitive("value", PhysicalType.INT64, RepetitionType.OPTIONAL));
    }

    /// A single-field schema whose field `m` is a `map<?, int64>` with the given key.
    private static FileSchema mapKeyedBy(SchemaElement key) {
        List<SchemaElement> elements = new ArrayList<>(List.of(root("root", 1)));
        elements.addAll(keyedMap("m", key));
        return FileSchema.fromSchemaElements(elements);
    }

    private static SchemaElement convertedPrimitive(String name, PhysicalType type,
            ConvertedType convertedType, LogicalType logicalType) {
        return new SchemaElement(name, type, null, RepetitionType.REQUIRED, null,
                convertedType, null, null, null, logicalType);
    }

    /// A Variant group and an ordinary group of the identical two-byte-field shape,
    /// side by side.
    private static FileSchema variantAndOrdinarySchema() {
        SchemaElement rootElement = root("root", 2);
        SchemaElement variant = group("variant_record", RepetitionType.OPTIONAL, 2, new LogicalType.VariantType(1));
        SchemaElement variantMetadata = primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        SchemaElement variantValue = primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        SchemaElement ordinary = group("ordinary_record", RepetitionType.OPTIONAL, 2);
        SchemaElement ordinaryMetadata = primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        SchemaElement ordinaryValue = primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);

        return FileSchema.fromSchemaElements(List.of(
                rootElement, variant, variantMetadata, variantValue,
                ordinary, ordinaryMetadata, ordinaryValue));
    }

    /// `pa.null()` columns carry the NULL logical type on an OPTIONAL primitive.
    /// The Avro schema must be a bare `NULL` — not `union [null, null]`, which
    /// the Avro spec forbids.
    @Test
    void nullLogicalTypeBecomesBareAvroNull() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement nothing = primitive("nothing", PhysicalType.INT32, RepetitionType.OPTIONAL,
                new LogicalType.NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, nothing));

        Schema avroSchema = convert(schema);
        Schema.Field field = avroSchema.getField("nothing");
        assertThat(field).isNotNull();
        assertThat(field.schema().getType()).isEqualTo(Schema.Type.NULL);
        // Guard against regressing to `union [null, null]` — still NULL on the
        // top branch but illegal Avro that would throw at schema construction.
        assertThat(field.schema().isUnion()).isFalse();
        assertThat(AvroSchemaConverter.plan(schema, ColumnProjection.all()).child(0).kind())
                .isEqualTo(AvroPlanNode.Kind.NULL);
    }

    @Test
    void rejectsListWithoutElementDuringPlanning() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement list = group("items", RepetitionType.OPTIONAL, 0, new LogicalType.ListType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, list));

        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("items")
                .hasMessageContaining("element");
    }

    @Test
    void keyOnlyMapBecomesMapOfBareNull() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement map = group("attributes", RepetitionType.OPTIONAL, 1, new LogicalType.MapType());
        SchemaElement keyValue = group("key_value", RepetitionType.REPEATED, 1);
        SchemaElement key = primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                new LogicalType.StringType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, map, keyValue, key));

        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        Schema mapSchema = pickMapBranch(plan.avro().getField("attributes").schema());
        assertThat(mapSchema.getValueType().getType()).isEqualTo(Schema.Type.NULL);
        assertThat(plan.child(0).mapValue().kind()).isEqualTo(AvroPlanNode.Kind.NULL);
    }

    /// A key-only map is still read key-first through `PqMap.Entry#getStringKey`, so the
    /// key check must run before the missing value short-circuits conversion. With the two
    /// in the other order this map converts to `map<null>` and only fails per value.
    @Test
    void rejectsKeyOnlyMapWhoseKeyIsNotAString() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement map = group("attributes", RepetitionType.OPTIONAL, 1, new LogicalType.MapType());
        SchemaElement keyValue = group("key_value", RepetitionType.REPEATED, 1);
        SchemaElement key = primitive("key", PhysicalType.INT32, RepetitionType.REQUIRED);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, map, keyValue, key));

        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attributes")
                .hasMessageContaining("INT32");
    }

    /// Container rejections name the dotted path, so two same-named lists under different
    /// structs stay distinguishable.
    @Test
    void namesTheDottedPathWhenANestedListHasNoElement() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement holder = group("holder", RepetitionType.OPTIONAL, 1);
        SchemaElement list = group("items", RepetitionType.OPTIONAL, 0, new LogicalType.ListType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, holder, list));

        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holder.items");
    }

    /// `list<null>` with an OPTIONAL element must produce `array<null>`, not
    /// `array<union [null, null]>`.
    @Test
    void listOfNullElementsBecomesArrayOfBareNull() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement listGroup = group("nulls", RepetitionType.OPTIONAL, 1, new LogicalType.ListType());
        SchemaElement listInner = group("list", RepetitionType.REPEATED, 1);
        SchemaElement element = primitive("element", PhysicalType.INT32, RepetitionType.OPTIONAL,
                new LogicalType.NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, listGroup, listInner, element));

        Schema avroSchema = convert(schema);
        Schema listField = pickArrayBranch(avroSchema.getField("nulls").schema());
        Schema elementSchema = listField.getElementType();
        assertThat(elementSchema.getType()).isEqualTo(Schema.Type.NULL);
        assertThat(elementSchema.isUnion()).isFalse();
    }

    /// `map<string, null>` with OPTIONAL values must produce `map<null>`, not
    /// `map<union [null, null]>`.
    @Test
    void mapWithNullValuesBecomesMapOfBareNull() {
        SchemaElement rootElement = root("root", 1);
        SchemaElement mapGroup = group("m", RepetitionType.OPTIONAL, 1, new LogicalType.MapType());
        SchemaElement kv = group("key_value", RepetitionType.REPEATED, 2);
        SchemaElement key = primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED,
                new LogicalType.StringType());
        SchemaElement value = primitive("value", PhysicalType.INT32, RepetitionType.OPTIONAL,
                new LogicalType.NullType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(rootElement, mapGroup, kv, key, value));

        Schema avroSchema = convert(schema);
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

    /// The converted schema alone, for assertions that do not care about the plan.
    private static Schema convert(FileSchema fileSchema) {
        return AvroSchemaConverter.plan(fileSchema, ColumnProjection.all()).avro();
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
    private static FileSchema duplicateNestedAddressSchema() {
        SchemaElement root = group("schema", null, 2);
        SchemaElement home = group("home", RepetitionType.OPTIONAL, 1);
        SchemaElement homeAddress = group("address", RepetitionType.OPTIONAL, 1);
        SchemaElement city = primitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType());
        SchemaElement work = group("work", RepetitionType.OPTIONAL, 1);
        SchemaElement workAddress = group("address", RepetitionType.OPTIONAL, 1);
        SchemaElement zip = primitive("zip", PhysicalType.INT32, null, null);
        return FileSchema.fromSchemaElements(List.of(
                root, home, homeAddress, city, work, workAddress, zip));
    }

    private static FileSchema illegalNestedGroupSchema() {
        SchemaElement root = group("schema", null, 1);
        SchemaElement address = group("acme.address", RepetitionType.OPTIONAL, 1);
        SchemaElement city = primitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType());
        return FileSchema.fromSchemaElements(List.of(root, address, city));
    }

    private static FileSchema buildVariantSchema(boolean includeTypedValue) {
        int varChildren = includeTypedValue ? 3 : 2;
        SchemaElement rootElement = root("root", 1);
        SchemaElement var = group("var", RepetitionType.OPTIONAL, varChildren, new LogicalType.VariantType(1));
        SchemaElement metadata = primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        SchemaElement value = primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        if (!includeTypedValue) {
            return FileSchema.fromSchemaElements(List.of(rootElement, var, metadata, value));
        }
        SchemaElement typedValue = primitive("typed_value", PhysicalType.INT64, RepetitionType.OPTIONAL);
        return FileSchema.fromSchemaElements(List.of(rootElement, var, metadata, value, typedValue));
    }
}
