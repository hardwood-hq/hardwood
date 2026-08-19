/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
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
    void duplicateNestedNamesSerializeWithPathNames() throws Exception {
        Schema converted = convert(duplicateNestedAddressSchema());
        Schema parsed = new Schema.Parser().parse(converted.toString());

        assertThat(parsed.toString()).contains("\"name\":\"address\",\"namespace\":\"schema.home\"",
                "\"name\":\"address\",\"namespace\":\"schema.work\"");
        new DataFileWriter<>(new GenericDatumWriter<>())
                .create(parsed, new ByteArrayOutputStream())
                .close();
    }

    @Test
    void illegalNestedNameIsSanitized() {
        Schema converted = convert(illegalNestedGroupSchema());
        Schema parsed = new Schema.Parser().parse(converted.toString());
        Schema record = pickRecordBranch(parsed.getField("acme_address").schema());

        assertThat(record.getFullName()).isEqualTo("schema.acme_address");
        assertThat(record.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("acme.address");
    }

    @Test
    void qualifiedRootDescendantsUseRootFullName() {
        Schema schema = convert(qualifiedRootNestedSchema());
        Schema parsed = new Schema.Parser().parse(schema.toString());

        assertThat(parsed.getFullName()).isEqualTo("acme.row");
        assertThat(parsed.toString()).contains("\"namespace\":\"acme.row\"",
                "\"namespace\":\"acme.row.acme\"");
    }

    @Test
    void invalidQualifiedRootIsRecoverable() {
        Schema schema = convert(schemaWithRootName("1acme.row"));
        Schema parsed = new Schema.Parser().parse(schema.toString());

        assertThat(parsed.getFullName()).isEqualTo("_1acme.row");
        assertThat(parsed.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("1acme.row");
    }

    @Test
    void canonicalFixedTypesRemainStable() {
        Schema parsed = new Schema.Parser().parse(convert(canonicalFixedSchema()).toString());

        assertThat(parsed.toString()).contains("\"name\":\"interval\"", "\"name\":\"float16\"");
        assertThat(parsed.getField("interval_one").schema().toString()).contains("\"size\":12");
        assertThat(parsed.getField("float16_one").schema().toString()).contains("\"size\":2");
    }

    @Test
    void canonicalRootConflictsAreRejected() {
        assertThatThrownBy(() -> convert(canonicalRootSchema("interval", new LogicalType.IntervalType(),
                PhysicalType.FIXED_LEN_BYTE_ARRAY, 12)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("interval");
        assertThatThrownBy(() -> convert(canonicalRootSchema("float16", new LogicalType.Float16Type(),
                PhysicalType.FIXED_LEN_BYTE_ARRAY, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("float16");
    }

    /// The conflict is decided from the complete unprojected schema, so a file either
    /// converts or does not whatever projection is applied. Projecting the interval
    /// column away must not turn the rejection into a success.
    @Test
    void canonicalRootConflictSurvivesProjectingTheColumnAway() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                root("interval", 2),
                fixed("span", 12, new LogicalType.IntervalType()),
                primitive("other", PhysicalType.INT32, RepetitionType.REQUIRED)));

        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.columns("other")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("interval");
    }

    /// The converter emits five kinds of named type: ordinary records, Variant records,
    /// fixed-backed decimals, plain `FIXED_LEN_BYTE_ARRAY` and `INT96`. Records and
    /// plain fixed are covered by `duplicateNestedNamesSerializeWithPathNames` and
    /// `fixedListElementReceivesAResolvedName`; these three cover the rest. Each puts a
    /// same-named instance in two branches, so only the resolved namespace keeps Avro
    /// from rejecting the second as a redefinition of the first.
    @Test
    void twinVariantRecordsAreDistinguishedByNamespace() throws Exception {
        assertTwinsAreDistinct(twinBranches(variantBranch("v")), "v");
    }

    @Test
    void twinInt96FixedTypesAreDistinguishedByNamespace() throws Exception {
        assertTwinsAreDistinct(
                twinBranches(List.of(convertedPrimitive("ts", PhysicalType.INT96, null, null))), "ts");
    }

    @Test
    void twinFixedBackedDecimalsAreDistinguishedByNamespace() throws Exception {
        assertTwinsAreDistinct(
                twinBranches(List.of(fixed("amount", 8, new LogicalType.DecimalType(2, 4)))), "amount");
    }

    /// A rewrite leaves a field with two names, so which one a projection path takes has
    /// to be pinned down: projection is resolved against the Parquet schema, and the
    /// rewritten Avro name is not a path a projection accepts.
    @Test
    void projectionPathsUseParquetNamesNotRewrittenAvroNames() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                root("schema", 2),
                group("acme-address", RepetitionType.OPTIONAL, 1),
                convertedPrimitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType()),
                primitive("other", PhysicalType.INT32, RepetitionType.REQUIRED)));

        Schema projected = AvroSchemaConverter.plan(schema,
                ColumnProjection.columns("acme-address.city")).avro();

        assertThat(projected.getFields()).extracting(Schema.Field::name).containsExactly("acme_address");
        assertThatThrownBy(() -> AvroSchemaConverter.plan(schema, ColumnProjection.columns("acme_address.city")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column not found");
    }

    @Test
    void projectionDoesNotRenameRetainedNamedTypes() {
        FileSchema schema = duplicateNestedAddressSchema();
        Schema all = convert(schema);
        Schema projected = AvroSchemaConverter.plan(schema,
                ColumnProjection.columns("home.address.city")).avro();

        Schema allHome = pickRecordBranch(all.getField("home").schema());
        Schema projectedHome = pickRecordBranch(projected.getField("home").schema());
        assertThat(pickRecordBranch(allHome.getField("address").schema()).getFullName())
                .isEqualTo(pickRecordBranch(projectedHome.getField("address").schema()).getFullName());
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

    /// A key in a fixed-backed position is classified by converting it, and converting a
    /// fixed needs the resolved name of a node the resolver has to have visited. The map
    /// key is the one value position that carries a name without being emitted, so it is
    /// the position a resolver walking only emitted types would skip — leaving the key
    /// rejected by an internal "unknown schema node" failure instead of the map
    /// diagnostic these assert.
    @Test
    void rejectsFixedLengthByteArrayMapKey() {
        assertRejectsMap(mapKeyedBy(fixed("key", 8, null)), "m", "FIXED_LEN_BYTE_ARRAY");
    }

    @Test
    void rejectsInt96MapKey() {
        assertRejectsMap(mapKeyedBy(convertedPrimitive("key", PhysicalType.INT96, null, null)), "m", "INT96");
    }

    @Test
    void rejectsFixedBackedDecimalMapKey() {
        assertRejectsMap(mapKeyedBy(fixed("key", 8, new LogicalType.DecimalType(2, 4))), "m", "DECIMAL");
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
    /// Contract row C5b: sibling fields whose raw names collide only after sanitizing
    /// stay legal and unique inside the record, and the rewritten one keeps its Parquet
    /// name across a parser round-trip.
    @Test
    void siblingFieldsCollidingAfterSanitizingStayUnique() {
        Schema parsed = new Schema.Parser().parse(convert(FileSchema.fromSchemaElements(List.of(
                root("schema", 2),
                primitive("a.b", PhysicalType.INT32, RepetitionType.REQUIRED),
                primitive("a_b", PhysicalType.INT32, RepetitionType.REQUIRED)))).toString());

        assertThat(parsed.getFields()).extracting(Schema.Field::name).containsExactly("a_b_2", "a_b");
        assertThat(parsed.getField("a_b_2").getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("a.b");
        assertThat(parsed.getField("a_b").getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isNull();
    }

    /// Contract row C3: an unqualified root outside the Avro grammar is sanitized and
    /// stays recoverable; a legal field below it carries no marker property.
    @Test
    void sanitizedUnqualifiedRootIsRecoverable() {
        Schema parsed = new Schema.Parser().parse(convert(FileSchema.fromSchemaElements(List.of(
                root("1schema", 1),
                primitive("v", PhysicalType.INT32, RepetitionType.REQUIRED)))).toString());

        assertThat(parsed.getFullName()).isEqualTo("_1schema");
        assertThat(parsed.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("1schema");
        assertThat(parsed.getField("v").getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isNull();
    }

    /// Contract row C4b: a LIST container field contributes its own local name to the
    /// namespace of the element below it, so an `element` record inside `items` and an
    /// `element` record directly under the root stay distinct.
    @Test
    void listContainerContributesItsNameToTheElementNamespace() throws Exception {
        Schema parsed = new Schema.Parser().parse(convert(listAndRootElementSchema()).toString());

        assertThat(parsed.toString()).contains("\"name\":\"element\",\"namespace\":\"schema.items\"",
                "\"name\":\"element\",\"namespace\":\"schema\"");
        new DataFileWriter<>(new GenericDatumWriter<>())
                .create(parsed, new ByteArrayOutputStream())
                .close();
    }

    /// Contract row C10: a fixed used directly as a list element is named by the
    /// resolver too, so it cannot redefine a same-named fixed elsewhere in the schema.
    @Test
    void fixedListElementReceivesAResolvedName() throws Exception {
        Schema parsed = new Schema.Parser().parse(convert(fixedListAndRootFixedSchema()).toString());

        assertThat(parsed.toString()).contains("\"name\":\"element\",\"namespace\":\"schema.items\"",
                "\"name\":\"element\",\"namespace\":\"schema\"");
        new DataFileWriter<>(new GenericDatumWriter<>())
                .create(parsed, new ByteArrayOutputStream())
                .close();
    }

    /// Contract row C9: adding, removing and reordering unrelated fields renames no
    /// retained named type. Projection is covered by
    /// `projectionDoesNotRenameRetainedNamedTypes`.
    @Test
    void unrelatedFieldChangesRenameNoRetainedNamedType() {
        assertThat(convert(duplicateNestedAddressSchema()).toString())
                .contains("\"namespace\":\"schema.home\"", "\"namespace\":\"schema.work\"");
        assertThat(convert(duplicateNestedAddressSchemaWithExtraLeaf()).toString())
                .contains("\"namespace\":\"schema.home\"", "\"namespace\":\"schema.work\"");
        assertThat(convert(duplicateNestedAddressSchemaReordered()).toString())
                .contains("\"namespace\":\"schema.home\"", "\"namespace\":\"schema.work\"");
    }

    /// Contract row C6: a name the resolver did not rewrite carries no marker property.
    @Test
    void unchangedNamesCarryNoParquetNameProperty() {
        Schema parsed = new Schema.Parser().parse(convert(duplicateNestedAddressSchema()).toString());

        assertThat(parsed.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isNull();
        assertThat(parsed.getField("home").getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isNull();
        assertThat(pickRecordBranch(parsed.getField("home").schema())
                .getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isNull();
    }

    private static FileSchema listAndRootElementSchema() {
        return FileSchema.fromSchemaElements(List.of(
                root("schema", 2),
                group("items", RepetitionType.OPTIONAL, 1, new LogicalType.ListType()),
                group("list", RepetitionType.REPEATED, 1),
                group("element", RepetitionType.OPTIONAL, 1),
                primitive("v", PhysicalType.INT32, RepetitionType.REQUIRED),
                group("element", RepetitionType.OPTIONAL, 1),
                primitive("w", PhysicalType.INT32, RepetitionType.REQUIRED)));
    }

    private static FileSchema fixedListAndRootFixedSchema() {
        return FileSchema.fromSchemaElements(List.of(
                root("schema", 2),
                group("items", RepetitionType.OPTIONAL, 1, new LogicalType.ListType()),
                group("list", RepetitionType.REPEATED, 1),
                fixed("element", 4, null),
                fixed("element", 4, null)));
    }

    private static FileSchema duplicateNestedAddressSchemaWithExtraLeaf() {
        return FileSchema.fromSchemaElements(List.of(
                root("schema", 3),
                group("home", RepetitionType.OPTIONAL, 1),
                group("address", RepetitionType.OPTIONAL, 1),
                convertedPrimitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType()),
                group("work", RepetitionType.OPTIONAL, 1),
                group("address", RepetitionType.OPTIONAL, 1),
                primitive("zip", PhysicalType.INT32, RepetitionType.REQUIRED),
                primitive("unrelated", PhysicalType.INT32, RepetitionType.REQUIRED)));
    }

    private static FileSchema duplicateNestedAddressSchemaReordered() {
        return FileSchema.fromSchemaElements(List.of(
                root("schema", 2),
                group("work", RepetitionType.OPTIONAL, 1),
                group("address", RepetitionType.OPTIONAL, 1),
                primitive("zip", PhysicalType.INT32, RepetitionType.REQUIRED),
                group("home", RepetitionType.OPTIONAL, 1),
                group("address", RepetitionType.OPTIONAL, 1),
                convertedPrimitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8,
                        new LogicalType.StringType())));
    }

    private static FileSchema qualifiedRootNestedSchema() {
        return FileSchema.fromSchemaElements(List.of(
                root("acme.row", 1),
                group("acme", RepetitionType.OPTIONAL, 1),
                group("row", RepetitionType.OPTIONAL, 1),
                primitive("value", PhysicalType.INT32, RepetitionType.REQUIRED)));
    }

    private static FileSchema schemaWithRootName(String rootName) {
        return FileSchema.fromSchemaElements(List.of(
                root(rootName, 1),
                primitive("value", PhysicalType.INT32, RepetitionType.REQUIRED)));
    }

    private static FileSchema canonicalFixedSchema() {
        SchemaElement rootElement = root("root", 4);
        SchemaElement intervalOne = fixed("interval_one", 12, new LogicalType.IntervalType());
        SchemaElement intervalTwo = fixed("interval_two", 12, new LogicalType.IntervalType());
        SchemaElement floatOne = fixed("float16_one", 2, new LogicalType.Float16Type());
        SchemaElement floatTwo = fixed("float16_two", 2, new LogicalType.Float16Type());
        return FileSchema.fromSchemaElements(List.of(rootElement, intervalOne, intervalTwo, floatOne, floatTwo));
    }

    private static FileSchema canonicalRootSchema(String rootName, LogicalType logicalType,
            PhysicalType physicalType, int typeLength) {
        SchemaElement rootElement = root(rootName, 1);
        SchemaElement value = new SchemaElement("value", physicalType, typeLength,
                RepetitionType.REQUIRED, null, null, null, null, null, logicalType);
        return FileSchema.fromSchemaElements(List.of(rootElement, value));
    }

    /// Asserts that the twin named types `home` and `work` hold are told apart by their
    /// namespace, and that the result is a schema Avro will write a file header for —
    /// the operation that rejects a redefinition, and the one a user hits first.
    private static void assertTwinsAreDistinct(FileSchema schema, String localName) throws Exception {
        Schema parsed = new Schema.Parser().parse(convert(schema).toString());

        assertThat(parsed.toString()).contains(
                "\"name\":\"" + localName + "\",\"namespace\":\"schema.home\"",
                "\"name\":\"" + localName + "\",\"namespace\":\"schema.work\"");
        new DataFileWriter<>(new GenericDatumWriter<>())
                .create(parsed, new ByteArrayOutputStream())
                .close();
    }

    /// A schema carrying `branch` verbatim under both `home` and `work`. `branch` is one
    /// subtree in pre-order, so its first element is the single child of each holder.
    private static FileSchema twinBranches(List<SchemaElement> branch) {
        List<SchemaElement> elements = new ArrayList<>();
        elements.add(root("schema", 2));
        elements.add(group("home", RepetitionType.OPTIONAL, 1));
        elements.addAll(branch);
        elements.add(group("work", RepetitionType.OPTIONAL, 1));
        elements.addAll(branch);
        return FileSchema.fromSchemaElements(elements);
    }

    private static List<SchemaElement> variantBranch(String name) {
        return List.of(
                group(name, RepetitionType.OPTIONAL, 2, new LogicalType.VariantType(1)),
                primitive("metadata", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED),
                primitive("value", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED));
    }

    private static SchemaElement fixed(String name, int typeLength, LogicalType logicalType) {
        return new SchemaElement(name, PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength,
                RepetitionType.REQUIRED, null, null, null, null, null, logicalType);
    }

    private static FileSchema duplicateNestedAddressSchema() {
        SchemaElement rootElement = root("schema", 2);
        SchemaElement home = group("home", RepetitionType.OPTIONAL, 1);
        SchemaElement homeAddress = group("address", RepetitionType.OPTIONAL, 1);
        SchemaElement city = convertedPrimitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType());
        SchemaElement work = group("work", RepetitionType.OPTIONAL, 1);
        SchemaElement workAddress = group("address", RepetitionType.OPTIONAL, 1);
        SchemaElement zip = primitive("zip", PhysicalType.INT32, RepetitionType.REQUIRED);
        return FileSchema.fromSchemaElements(List.of(
                rootElement, home, homeAddress, city, work, workAddress, zip));
    }

    private static FileSchema illegalNestedGroupSchema() {
        SchemaElement rootElement = root("schema", 1);
        SchemaElement address = group("acme.address", RepetitionType.OPTIONAL, 1);
        SchemaElement city = convertedPrimitive("city", PhysicalType.BYTE_ARRAY, ConvertedType.UTF8, new LogicalType.StringType());
        return FileSchema.fromSchemaElements(List.of(rootElement, address, city));
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
