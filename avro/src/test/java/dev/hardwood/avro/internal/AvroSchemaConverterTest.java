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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import static dev.hardwood.metadata.SchemaElement.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
    void rootAndTopLevelChildWithSameNameRemainDistinct() throws Exception {
        Schema parsed = new Schema.Parser().parse(convert(FileSchema.fromSchemaElements(List.of(
                root("root", 1),
                group("root", RepetitionType.OPTIONAL, 1),
                primitive("value", PhysicalType.INT32, RepetitionType.REQUIRED)))).toString());

        assertThat(parsed.getFullName()).isEqualTo("root");
        assertThat(pickRecordBranch(parsed.getField("root").schema()).getFullName()).isEqualTo("root.root");
        new DataFileWriter<>(new GenericDatumWriter<>())
                .create(parsed, new ByteArrayOutputStream())
                .close();
    }

    @Test
    void rewrittenFixedTypeRetainsParquetNameProperty() {
        Schema parsed = new Schema.Parser().parse(convert(FileSchema.fromSchemaElements(List.of(
                root("schema", 1),
                fixedLengthPrimitive("fixed.value", 4, RepetitionType.REQUIRED)))).toString());

        Schema fixedSchema = parsed.getField("fixed_value").schema();
        assertThat(fixedSchema.getFullName()).isEqualTo("schema.fixed_value");
        assertThat(fixedSchema.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("fixed.value");
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
        assertThat(parsed.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("acme.row");
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

    @Test
    void rewrittenVariantAndFixedTypesRetainParquetNameProperty() {
        Schema variant = new Schema.Parser().parse(convert(twinBranches(variantBranch("v.x"))).toString());
        Schema homeVariant = pickRecordBranch(
                pickRecordBranch(variant.getField("home").schema()).getField("v_x").schema());
        assertThat(homeVariant.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("v.x");

        Schema int96 = new Schema.Parser().parse(convert(
                twinBranches(List.of(convertedPrimitive("ts.x", PhysicalType.INT96, null, null)))).toString());
        Schema homeInt96 = pickRecordBranch(int96.getField("home").schema()).getField("ts_x").schema();
        assertThat(homeInt96.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("ts.x");

        Schema decimal = new Schema.Parser().parse(convert(
                twinBranches(List.of(fixed("amount.x", 8, new LogicalType.DecimalType(2, 4))))).toString());
        Schema homeDecimal = pickRecordBranch(decimal.getField("home").schema()).getField("amount_x").schema();
        assertThat(homeDecimal.getProp(AvroSchemaConverter.PARQUET_NAME_PROP)).isEqualTo("amount.x");
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
        assertRejectsMap(mapKeyedBy(fixedLengthPrimitive("key", 8, RepetitionType.REQUIRED)), "m", "FIXED_LEN_BYTE_ARRAY");
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

    @Test
    void parquetAvroCompatibilityCountsRecordsBeforeTheirChildren() {
        AvroPlanNode plan = AvroSchemaConverter.planForParquetAvroCompatibility(
                repeatedAddressSchema(), ColumnProjection.all());

        Schema home = pickRecordBranch(plan.avro().getField("home").schema());
        Schema work = pickRecordBranch(plan.avro().getField("work").schema());
        assertThat(pickRecordBranch(home.getField("address").schema()).getFullName()).isEqualTo("address");
        assertThat(pickRecordBranch(work.getField("address").schema()).getFullName())
                .isEqualTo("address2.address");
        assertThat(AvroSchemaConverter.plan(repeatedAddressSchema(), ColumnProjection.all()).avro()
                .getField("work").schema().getTypes().get(1).getField("address").schema().getTypes().get(1)
                .getFullName()).isEqualTo("root.work.address");
    }

    @Test
    void parquetAvroCompatibilityUsesSourceNameForLogicalFixedTypes() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement interval = new SchemaElement("duration", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                RepetitionType.OPTIONAL, null, null, null, null, null, new LogicalType.IntervalType());
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, interval));

        Schema fixed = pickRecordBranch(AvroSchemaConverter.planForParquetAvroCompatibility(
                schema, ColumnProjection.all()).avro()).getField("duration").schema().getTypes().get(1);
        assertThat(fixed.getName()).isEqualTo("duration");
        assertThat(fixed.getNamespace()).isNull();
        assertThat(fixed.getFixedSize()).isEqualTo(12);
    }

    @Test
    void parquetAvroCompatibilityRejectsBareInt96() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement instant = new SchemaElement("instant", PhysicalType.INT96, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, instant));

        // parquet-avro rejects INT96 unless READ_INT96_AS_FIXED is set; the compatibility
        // default reproduces its message verbatim.
        assertThatThrownBy(() -> AvroSchemaConverter.planForParquetAvroCompatibility(schema, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.");

        Schema fixed = pickRecordBranch(AvroSchemaConverter.planForParquetAvroCompatibility(
                schema, ColumnProjection.all(), true).avro()).getField("instant").schema().getTypes().get(1);
        assertThat(fixed.getType()).isEqualTo(Schema.Type.FIXED);
        assertThat(fixed.getFixedSize()).isEqualTo(12);
    }

    @Test
    void parquetAvroCompatibilityDoesNotShareCounterState() {
        FileSchema schema = repeatedAddressSchema();

        Schema first = AvroSchemaConverter.planForParquetAvroCompatibility(schema, ColumnProjection.all()).avro();
        Schema second = AvroSchemaConverter.planForParquetAvroCompatibility(schema, ColumnProjection.all()).avro();

        assertThat(first.toString()).isEqualTo(second.toString());
        assertThat(pickRecordBranch(second.getField("work").schema()).getField("address").schema()
                .getTypes().get(1).getFullName()).isEqualTo("address2.address");
    }

    @Test
    void parquetAvroCompatibilityRenumbersAfterPrecedingOccurrence() {
        Schema plan = AvroSchemaConverter.planForParquetAvroCompatibility(
                repeatedAddressSchemaWithPrecedingAddress(), ColumnProjection.all()).avro();

        Schema home = pickRecordBranch(plan.getField("home").schema());
        Schema work = pickRecordBranch(plan.getField("work").schema());
        assertThat(pickRecordBranch(home.getField("address").schema()).getFullName()).isEqualTo("address2.address");
        assertThat(pickRecordBranch(work.getField("address").schema()).getFullName()).isEqualTo("address3.address");
    }

    @Test
    void parquetAvroCompatibilitySuppressesShreddedVariantTypedValue() {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement variant = new SchemaElement("payload", null, null, RepetitionType.OPTIONAL,
                3, null, null, null, null, new LogicalType.VariantType(1));
        SchemaElement metadata = new SchemaElement("metadata", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, null);
        SchemaElement typedValue = new SchemaElement("typed_value", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement suppressed = new SchemaElement("address", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement external = new SchemaElement("address", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                root, variant, metadata, value, typedValue, suppressed, external));

        Schema avro = AvroSchemaConverter.planForParquetAvroCompatibility(schema, ColumnProjection.all()).avro();
        Schema payload = pickRecordBranch(avro.getField("payload").schema());
        Schema address = avro.getField("address").schema().getTypes().get(1);
        assertThat(payload.getFields()).extracting(Schema.Field::name).containsExactly("metadata", "value");
        assertThat(address.getFullName()).isEqualTo("address");
    }

    @Test
    void parquetAvroCompatibilitySerializesDistinctSameNamedDefinitions() throws Exception {
        Schema schema = AvroSchemaConverter.planForParquetAvroCompatibility(
                repeatedAddressSchema(), ColumnProjection.all()).avro();

        assertThatCode(() -> {
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                writer.create(schema, new ByteArrayOutputStream());
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void parquetAvroCompatibilitySerializesDistinctSameNamedFixedDefinitions() throws Exception {
        Schema schema = AvroSchemaConverter.planForParquetAvroCompatibility(
                repeatedAddressAndFixedSchema(), ColumnProjection.all()).avro();
        Schema first = pickFixedBranch(schema.getField("token").schema());
        Schema second = pickFixedBranch(pickRecordBranch(schema.getField("nested").schema())
                .getField("token").schema());

        assertThat(first.getType()).isEqualTo(Schema.Type.FIXED);
        assertThat(second.getType()).isEqualTo(Schema.Type.FIXED);
        assertThat(first.getFullName()).isEqualTo("token");
        assertThat(second.getFullName()).isEqualTo("token2.token");
        assertThat(first.getFixedSize()).isEqualTo(4);
        assertThat(second.getFixedSize()).isEqualTo(8);
        assertThatCode(schema::toString).doesNotThrowAnyException();
        assertThatCode(() -> {
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                writer.create(schema, new ByteArrayOutputStream());
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void parquetAvroCompatibilityStateIsolatedAcrossConcurrentConversions() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<String>> results = List.of(
                    executor.submit(task(start)), executor.submit(task(start)),
                    executor.submit(task(start)), executor.submit(task(start)));
            start.countDown();
            for (Future<String> result : results) {
                assertThat(result.get()).isEqualTo("address2.address");
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void parquetAvroCompatibilityCountsOnlyProjectedOrdinaryRecords() {
        AvroPlanNode plan = AvroSchemaConverter.planForParquetAvroCompatibility(
                repeatedAddressSchema(), ColumnProjection.columns("home.address.city"));

        Schema root = plan.avro();
        Schema home = pickRecordBranch(root.getField("home").schema());
        Schema address = pickRecordBranch(home.getField("address").schema());
        assertThat(root.getField("work")).isNull();
        assertThat(address.getFullName()).isEqualTo("address");
        assertThat(address.getField("city")).isNotNull();
        assertThat(plan.child(0).source().name()).isEqualTo("home");
        assertThat(plan.child(0).child(0).source().name()).isEqualTo("address");
    }

    @Test
    void keyOnlyMapProjectionRetainsNamedValuePlan() {
        FileSchema schema = namedMapValueSchema();
        ColumnProjection projection = ColumnProjection.columns("people.key_value.key");
        AvroPlanNode nativePlan = AvroSchemaConverter.plan(schema, projection);
        AvroPlanNode compatibilityPlan = AvroSchemaConverter.planForParquetAvroCompatibility(
                schema, projection);
        SchemaNode map = schema.getRootNode().children().getFirst();
        if (!(map instanceof SchemaNode.GroupNode mapGroup)) {
            throw new AssertionError("Expected MAP group");
        }
        SchemaNode value = mapGroup.getMapValue();

        Schema nativeValue = pickMapBranch(nativePlan.avro().getField("people").schema()).getValueType();
        Schema compatibilityValue = pickMapBranch(compatibilityPlan.avro().getField("people").schema())
                .getValueType();
        assertThat(nativeValue.getType()).isEqualTo(Schema.Type.UNION);
        assertThat(pickRecordBranch(nativeValue).getName()).isEqualTo("value");
        assertThat(pickRecordBranch(compatibilityValue).getFullName()).isEqualTo("value");
        assertThat(ProjectedSchema.create(schema, projection, true).getProjectedColumns())
                .extracting(column -> column.fieldPath().toString())
                .containsExactly("people.key_value.key");
        assertThat(nativePlan.child(0).mapValue().source()).isSameAs(value);
        assertThat(compatibilityPlan.child(0).mapValue().source()).isSameAs(value);
        assertThat(compatibilityPlan.child(0).mapValue().kind()).isEqualTo(AvroPlanNode.Kind.STRUCT);
        assertPlansEquivalent(nativePlan.child(0).mapValue(), compatibilityPlan.child(0).mapValue());
    }

    @Test
    void parquetAvroCompatibilityPreservesProjectedPlanAcrossNestedContainers() {
        FileSchema schema = projectedNestedSchema();
        ColumnProjection projection = ColumnProjection.columns(
                "retained.address.city", "items.list.address.city", "people.key_value.address.city");
        AvroPlanNode nativePlan = AvroSchemaConverter.plan(schema, projection);
        AvroPlanNode compatibilityPlan = AvroSchemaConverter.planForParquetAvroCompatibility(schema, projection);

        assertPlansEquivalent(nativePlan, compatibilityPlan);
        assertThat(nativePlan.avro().getFields()).extracting(Schema.Field::name)
                .containsExactly("retained", "items", "people");
        assertThat(pickRecordBranch(pickArrayBranch(compatibilityPlan.avro().getField("items").schema())
                .getElementType()).getFields()).extracting(Schema.Field::name).containsExactly("city");
        assertThat(pickRecordBranch(pickMapBranch(compatibilityPlan.avro().getField("people").schema())
                .getValueType()).getFields()).extracting(Schema.Field::name).containsExactly("city");

        Schema retainedAddress = pickRecordBranch(pickRecordBranch(
                compatibilityPlan.avro().getField("retained").schema()).getField("address").schema());
        Schema listAddress = pickRecordBranch(pickArrayBranch(
                compatibilityPlan.avro().getField("items").schema()).getElementType());
        Schema mapAddress = pickRecordBranch(pickMapBranch(
                compatibilityPlan.avro().getField("people").schema()).getValueType());
        assertThat(retainedAddress.getFullName()).isEqualTo("address");
        assertThat(listAddress.getFullName()).isEqualTo("address2.address");
        assertThat(mapAddress.getFullName()).isEqualTo("address3.address");
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

    private static Schema pickFixedBranch(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.FIXED) {
            return fieldSchema;
        }
        for (Schema sub : fieldSchema.getTypes()) {
            if (sub.getType() == Schema.Type.FIXED) {
                return sub;
            }
        }
        throw new AssertionError("No fixed branch in union: " + fieldSchema);
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

    /// Sibling fields whose raw names collide only after sanitizing
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

    /// An unqualified root outside the Avro grammar is sanitized and
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

    /// A LIST container field contributes its own local name to the
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

    /// A fixed used directly as a list element is named by the
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

    /// Adding, removing and reordering unrelated fields renames no
    /// retained named type. Projection is covered by
    /// `projectionDoesNotRenameRetainedNamedTypes`, and the one sibling change that does
    /// rename — a sibling occupying the suffix space of a sanitization collision — by
    /// `AvroNamesTest#aSiblingOccupyingTheSuffixSpaceRenamesTheCollidingMembers`.
    @Test
    void unrelatedFieldChangesRenameNoRetainedNamedType() {
        assertThat(convert(duplicateNestedAddressSchema()).toString())
                .contains("\"namespace\":\"schema.home\"", "\"namespace\":\"schema.work\"");
        assertThat(convert(duplicateNestedAddressSchemaWithExtraLeaf()).toString())
                .contains("\"namespace\":\"schema.home\"", "\"namespace\":\"schema.work\"");
        assertThat(convert(duplicateNestedAddressSchemaReordered()).toString())
                .contains("\"namespace\":\"schema.home\"", "\"namespace\":\"schema.work\"");
    }

    /// A name the resolver did not rewrite carries no marker property.
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
                fixedLengthPrimitive("element", 4, RepetitionType.REQUIRED),
                fixedLengthPrimitive("element", 4, RepetitionType.REQUIRED)));
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

    private static FileSchema canonicalRootSchema(String rootName, LogicalType logicalType, PhysicalType physicalType,
                                                  int typeLength) {
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
        //noinspection resource
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
        return fixedLengthPrimitive(name, typeLength, RepetitionType.REQUIRED, logicalType);
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

    private static FileSchema repeatedAddressSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 2, null, null, null, null, null);
        SchemaElement home = new SchemaElement("home", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement homeAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement city = new SchemaElement("city", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, new LogicalType.StringType());
        SchemaElement work = new SchemaElement("work", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement workAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement zip = new SchemaElement("zip", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, home, homeAddress, city, work, workAddress, zip));
    }

    private static FileSchema repeatedAddressSchemaWithPrecedingAddress() {
        SchemaElement root = new SchemaElement("root", null, null, null, 3, null, null, null, null, null);
        SchemaElement before = new SchemaElement("before", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement beforeAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement flag = new SchemaElement("flag", PhysicalType.BOOLEAN, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        List<SchemaElement> baseElements = List.of(
                new SchemaElement("home", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null, null),
                new SchemaElement("address", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null, null),
                new SchemaElement("city", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL, null,
                        null, null, null, null, new LogicalType.StringType()),
                new SchemaElement("work", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null, null),
                new SchemaElement("address", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null, null),
                new SchemaElement("zip", PhysicalType.INT32, null, RepetitionType.OPTIONAL, null,
                        null, null, null, null, null));
        return FileSchema.fromSchemaElements(List.of(root, before, beforeAddress, flag,
                baseElements.get(0), baseElements.get(1), baseElements.get(2),
                baseElements.get(3), baseElements.get(4), baseElements.get(5)));
    }

    private static FileSchema namedMapValueSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement map = new SchemaElement("people", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, new LogicalType.MapType());
        SchemaElement keyValue = new SchemaElement("key_value", null, null, RepetitionType.REPEATED,
                2, null, null, null, null, null);
        SchemaElement key = new SchemaElement("key", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, new LogicalType.StringType());
        SchemaElement value = new SchemaElement("value", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement address = new SchemaElement("address", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, map, keyValue, key, value, address));
    }

    private static FileSchema repeatedAddressAndFixedSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 3, null, null, null, null, null);
        SchemaElement token = new SchemaElement("token", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement nested = new SchemaElement("nested", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement nestedToken = new SchemaElement("token", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement value = new SchemaElement("value", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, token, nested, nestedToken, value));
    }

    private static FileSchema projectedNestedSchema() {
        SchemaElement root = new SchemaElement("root", null, null, null, 4, null, null, null, null, null);
        SchemaElement before = new SchemaElement("before", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement beforeAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement beforeCity = new SchemaElement("city", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement retained = new SchemaElement("retained", null, null, RepetitionType.OPTIONAL,
                1, null, null, null, null, null);
        SchemaElement retainedAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                2, null, null, null, null, null);
        SchemaElement retainedCity = new SchemaElement("city", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement retainedSkip = new SchemaElement("skip", PhysicalType.INT64, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement items = new SchemaElement("items", null, null, RepetitionType.OPTIONAL,
                1, ConvertedType.LIST, null, null, null, new LogicalType.ListType());
        SchemaElement list = new SchemaElement("list", null, null, RepetitionType.REPEATED,
                1, null, null, null, null, null);
        SchemaElement itemAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                2, null, null, null, null, null);
        SchemaElement itemCity = new SchemaElement("city", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement itemSkip = new SchemaElement("skip", PhysicalType.INT64, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement people = new SchemaElement("people", null, null, RepetitionType.OPTIONAL,
                1, ConvertedType.MAP, null, null, null, new LogicalType.MapType());
        SchemaElement keyValue = new SchemaElement("key_value", null, null, RepetitionType.REPEATED,
                2, null, null, null, null, null);
        SchemaElement key = new SchemaElement("key", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.REQUIRED, null, null, null, null, null, new LogicalType.StringType());
        SchemaElement valueAddress = new SchemaElement("address", null, null, RepetitionType.OPTIONAL,
                2, null, null, null, null, null);
        SchemaElement valueCity = new SchemaElement("city", PhysicalType.INT32, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        SchemaElement valueSkip = new SchemaElement("skip", PhysicalType.INT64, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        return FileSchema.fromSchemaElements(List.of(root, before, beforeAddress, beforeCity, retained,
                retainedAddress, retainedCity, retainedSkip, items, list, itemAddress, itemCity, itemSkip,
                people, keyValue, key, valueAddress, valueCity, valueSkip));
    }

    private static void assertPlansEquivalent(AvroPlanNode expected, AvroPlanNode actual) {
        assertThat(actual.kind()).isEqualTo(expected.kind());
        assertThat(actual.source()).isSameAs(expected.source());
        assertSchemasEquivalent(expected.avro(), actual.avro());
        switch (expected.kind()) {
            case STRUCT -> {
                for (int index = 0; index < expected.avro().getFields().size(); index++) {
                    assertPlansEquivalent(expected.child(index), actual.child(index));
                }
            }
            case LIST -> assertPlansEquivalent(expected.listElement(), actual.listElement());
            case MAP -> assertPlansEquivalent(expected.mapValue(), actual.mapValue());
            default -> {
            }
        }
    }

    private static void assertSchemasEquivalent(Schema expected, Schema actual) {
        assertThat(actual.getType()).isEqualTo(expected.getType());
        switch (expected.getType()) {
            case RECORD -> {
                assertThat(actual.getFields()).extracting(Schema.Field::name)
                        .containsExactlyElementsOf(expected.getFields().stream().map(Schema.Field::name).toList());
                for (int index = 0; index < expected.getFields().size(); index++) {
                    assertSchemasEquivalent(expected.getFields().get(index).schema(), actual.getFields().get(index).schema());
                }
            }
            case ARRAY -> assertSchemasEquivalent(expected.getElementType(), actual.getElementType());
            case MAP -> assertSchemasEquivalent(expected.getValueType(), actual.getValueType());
            case UNION -> {
                assertThat(actual.getTypes()).hasSameSizeAs(expected.getTypes());
                for (int index = 0; index < expected.getTypes().size(); index++) {
                    assertSchemasEquivalent(expected.getTypes().get(index), actual.getTypes().get(index));
                }
            }
            case FIXED -> {
                assertThat(actual.getFixedSize()).isEqualTo(expected.getFixedSize());
                assertThat(actual.getLogicalType()).isEqualTo(expected.getLogicalType());
            }
            default -> {
            }
        }
    }

    private static Callable<String> task(CountDownLatch start) {
        return () -> {
            start.await();
            Schema schema = AvroSchemaConverter.planForParquetAvroCompatibility(
                    repeatedAddressSchema(), ColumnProjection.all()).avro();
            return pickRecordBranch(schema.getField("work").schema()).getField("address").schema()
                    .getTypes().get(1).getFullName();
        };
    }
}
