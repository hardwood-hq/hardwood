/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Version;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Pins the observable named-type behavior of parquet-avro `1.17.1`.
///
/// The golden records names, namespaces, traversal sites, and fixed widths rather
/// than the full schema JSON, whose unrelated defaults are outside issue #925.
class AvroNamedTypeOracleTest {

    private static final Path GOLDEN = Path.of("src", "test", "resources", "avro-named-types",
            "parquet-avro-1.17.1-avro-1.11.4.txt");

    private static final String LICENSE_HEADER = """
            #
            #  SPDX-License-Identifier: Apache-2.0
            #
            #  Copyright The original authors
            #
            #  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
            #

            """;

    private static final Set<String> REQUIRED_IDS = Set.of(
            "nested-different", "nested-identical", "list-and-map", "variant-canonical",
            "variant-shredded", "fixed", "logical-fixed", "qualified-root", "address",
            "address-preceded", "int96-fixed", "int96-rejected", "record-fixed-collision");

    @Test
    void matchesParquetAvroNamedTypeGolden() throws IOException {
        assertThat(Version.VERSION_NUMBER).isEqualTo("1.17.1");
        assertThat(Schema.class.getPackage().getImplementationVersion()).isEqualTo("1.11.4");

        List<Fixture> fixtures = fixtures();
        assertThat(fixtures).extracting(Fixture::id).containsExactlyInAnyOrderElementsOf(REQUIRED_IDS);
        assertThat(fixtures).extracting(Fixture::id).doesNotHaveDuplicates();

        String actual = renderFixtures(fixtures);
        if (Boolean.getBoolean("hardwood.updateAvroNamedTypesGolden")) {
            Files.createDirectories(GOLDEN.getParent());
            Files.writeString(GOLDEN, LICENSE_HEADER + actual.stripTrailing() + "\n", StandardCharsets.UTF_8);
            throw new AssertionError("Updated " + GOLDEN + "; review it and re-run without the property");
        }
        assertThat(GOLDEN).exists();
        String expected = Files.readString(GOLDEN, StandardCharsets.UTF_8);
        assertGoldenSections(expected, fixtures);
        assertThat(stripLicenseHeader(expected).stripTrailing()).isEqualTo(actual.stripTrailing());

        assertThat(actual).contains("address2.address");
        assertThat(actual).contains("address3.address");
        assertThat(actual).contains("unsupported | java.lang.IllegalArgumentException | "
                + "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.");
    }

    @Test
    void fixedModeInt96GoldenRecordsRepeatedName() {
        Fixture fixture = fixtures().stream().filter(candidate -> candidate.id().equals("int96-fixed"))
                .findFirst().orElseThrow();

        assertThat(renderNamedTypeOccurrences(fixture.descriptor(), fixture.converter().convert(fixture.schema())))
                .contains("/1:nested/0:instant/ | instant | instant | instant2 | instant2.instant | FIXED | 12");
    }

    @Test
    void hardwoodCompatibilityMatchesReferenceForNestedAndProjectedSchemas() {
        MessageType reference = parse("""
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """);
        FileSchema hardwood = nestedAddressSchema();

        SourceDescriptor descriptor = bind(reference, layout("root", Role.RECORD,
                layout("home", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))),
                layout("work", Role.RECORD, layout("address", Role.RECORD, layout("zip", Role.LEAF)))));
        assertThat(renderNamedTypeOccurrences(descriptor,
                new AvroSchemaConverter().convert(reference)))
                .containsExactlyElementsOf(renderNamedTypeOccurrences(descriptor,
                        dev.hardwood.avro.internal.AvroSchemaConverter.planForParquetAvroCompatibility(
                                hardwood, ColumnProjection.all()).avro()));

        MessageType projectedReference = parse("""
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                }
                """);
        SourceDescriptor projectedDescriptor = bind(projectedReference, layout("root", Role.RECORD,
                layout("home", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF)))));
        assertThat(renderNamedTypeOccurrences(projectedDescriptor, new AvroSchemaConverter().convert(projectedReference)))
                .containsExactlyElementsOf(renderNamedTypeOccurrences(projectedDescriptor,
                        dev.hardwood.avro.internal.AvroSchemaConverter.planForParquetAvroCompatibility(
                                hardwood, ColumnProjection.columns("home.address.city")).avro()));
    }

    @Test
    void hardwoodCompatibilityMatchesEverySupportedReferenceFixture() {
        for (Fixture fixture : fixtures()) {
            Schema reference;
            try {
                reference = fixture.converter().convert(fixture.schema());
            }
            catch (RuntimeException unsupported) {
                continue;
            }
            FileSchema hardwood = hardwoodSchema(fixture.schema());
            validateDescriptor(fixture.descriptor());
            Schema compatibility = dev.hardwood.avro.internal.AvroSchemaConverter
                    .planForParquetAvroCompatibility(hardwood, ColumnProjection.all(), fixture.readInt96AsFixed())
                    .avro();
            assertThat(renderNamedTypeOccurrences(fixture.descriptor(), compatibility))
                    .as(fixture.id())
                    .containsExactlyElementsOf(renderNamedTypeOccurrences(fixture.descriptor(), reference));
            assertThat(renderFixedLogicalTypes(compatibility))
                    .as(fixture.id() + " fixed logical types")
                    .containsExactlyElementsOf(renderFixedLogicalTypes(reference));
        }
    }

    @Test
    void hardwoodCompatibilityRejectsBareInt96LikeParquetAvro() {
        MessageType int96 = parse("""
                message root {
                  optional int96 instant;
                }
                """);
        FileSchema hardwood = hardwoodSchema(int96);

        // Capture parquet-avro's own rejection message so the assertion pins true parity,
        // not merely "throws something".
        String referenceMessage = null;
        try {
            new AvroSchemaConverter().convert(int96);
        }
        catch (IllegalArgumentException rejected) {
            referenceMessage = rejected.getMessage();
        }
        assertThat(referenceMessage).isNotNull();

        assertThatThrownBy(() -> dev.hardwood.avro.internal.AvroSchemaConverter
                .planForParquetAvroCompatibility(hardwood, ColumnProjection.all()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(referenceMessage);

        // The flag-on overload reads INT96 as a 12-byte fixed, like parquet-avro with
        // READ_INT96_AS_FIXED enabled.
        Schema instant = dev.hardwood.avro.internal.AvroSchemaConverter
                .planForParquetAvroCompatibility(hardwood, ColumnProjection.all(), true).avro()
                .getField("instant").schema().getTypes().get(1);
        assertThat(instant.getType()).isEqualTo(Schema.Type.FIXED);
        assertThat(instant.getFixedSize()).isEqualTo(12);
    }

    @Test
    void hardwoodCompatibilityMatchesProjectedListMapAndKeyOnlyMapReference() {
        MessageType projectedNested = parse("""
                message root {
                  optional group retained { optional group address { optional int32 city; } }
                  optional group items (LIST) {
                    repeated group list {
                      optional group address { optional int32 city; }
                    }
                  }
                  optional group people (MAP) {
                    repeated group key_value {
                      required binary key (STRING);
                      optional group value { optional int32 city; }
                    }
                  }
                }
                """);
        FileSchema full = hardwoodSchema(parse("""
                message root {
                  optional group before { optional group address { optional int32 ignored; } }
                  optional group retained { optional group address { optional int32 city; optional int64 skip; } }
                  optional group items (LIST) {
                    repeated group list {
                      optional group address { optional int32 city; optional int64 skip; }
                      optional int64 skip;
                    }
                  }
                  optional group people (MAP) {
                    repeated group key_value {
                      required binary key (STRING);
                      optional group value { optional int32 city; optional int64 skip; }
                    }
                  }
                }
                """));
        ColumnProjection projection = ColumnProjection.columns(
                "retained.address.city", "items.list.address.city", "people.key_value.value.city");
        SourceDescriptor projectedDescriptor = bind(projectedNested, layout("root", Role.RECORD,
                layout("retained", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))),
                layout("items", Role.ARRAY, layout("list", Role.RECORD,
                        layout("address", Role.RECORD, layout("city", Role.LEAF)))),
                layout("people", Role.MAP, layout("key_value", Role.RECORD, layout("key", Role.LEAF),
                        layout("value", Role.RECORD, layout("city", Role.LEAF))))));
        assertThat(renderNamedTypeOccurrences(projectedDescriptor, new AvroSchemaConverter().convert(projectedNested)))
                .containsExactlyElementsOf(renderNamedTypeOccurrences(projectedDescriptor,
                        dev.hardwood.avro.internal.AvroSchemaConverter
                                .planForParquetAvroCompatibility(full, projection).avro()));
        Schema projectedItems = new AvroSchemaConverter().convert(projectedNested)
                .getField("items").schema().getTypes().get(1).getElementType();
        Schema projectedPeople = new AvroSchemaConverter().convert(projectedNested)
                .getField("people").schema().getTypes().get(1).getValueType().getTypes().get(1);
        assertThat(projectedItems.getFields()).extracting(Schema.Field::name).containsExactly("address");
        assertThat(projectedItems.getField("address").schema().getTypes().get(1).getFields())
                .extracting(Schema.Field::name).containsExactly("city");
        assertThat(projectedPeople.getFields()).extracting(Schema.Field::name).containsExactly("city");

        MessageType fullMap = parse("""
                message root {
                  optional group people (MAP) {
                    repeated group key_value {
                      required binary key (STRING);
                      optional fixed_len_byte_array(4) value;
                    }
                  }
                }
                """);
        FileSchema mapSchema = hardwoodSchema(fullMap);
        SourceDescriptor mapDescriptor = bind(fullMap, layout("root", Role.RECORD,
                layout("people", Role.MAP, layout("key_value", Role.RECORD, layout("key", Role.LEAF),
                        layout("value", Role.FIXED)))));
        assertThat(renderNamedTypeOccurrences(mapDescriptor, new AvroSchemaConverter().convert(fullMap)))
                .containsExactlyElementsOf(renderNamedTypeOccurrences(mapDescriptor,
                        dev.hardwood.avro.internal.AvroSchemaConverter.planForParquetAvroCompatibility(
                                mapSchema, ColumnProjection.columns("people.key_value.key")).avro()));
    }

    @Test
    void fixedLogicalTypeRendererPreservesDecimalPrecisionAndScale() {
        List<String> decimalNineTwo = renderFixedLogicalTypes(decimalFixed(9, 2));
        List<String> decimalEightTwo = renderFixedLogicalTypes(decimalFixed(8, 2));
        List<String> decimalNineThree = renderFixedLogicalTypes(decimalFixed(9, 3));

        assertThat(decimalNineTwo).containsExactly("amount | 5 | decimal(9,2)");
        assertThat(decimalEightTwo).isNotEqualTo(decimalNineTwo);
        assertThat(decimalNineThree).isNotEqualTo(decimalNineTwo);
    }

    @Test
    void variantDescriptorRejectsPhysicalMutations() {
        Fixture fixture = fixtures().stream().filter(candidate -> candidate.id().equals("variant-shredded"))
                .findFirst().orElseThrow();
        SourceDescriptor root = fixture.descriptor();
        SourceDescriptor payload = root.children().getFirst();

        SourceDescriptor missingChild = new SourceDescriptor(payload.source(), payload.name(),
                payload.children().subList(0, 2), Role.VARIANT);
        assertThatThrownBy(() -> validateDescriptor(new SourceDescriptor(root.source(), root.name(),
                List.of(missingChild, root.children().get(1)), Role.RECORD)))
                .isInstanceOf(AssertionError.class);

        List<SourceDescriptor> reordered = new ArrayList<>(payload.children());
        SourceDescriptor first = reordered.removeFirst();
        reordered.add(first);
        SourceDescriptor reorderedPayload = new SourceDescriptor(payload.source(), payload.name(), reordered,
                Role.VARIANT);
        assertThatThrownBy(() -> validateDescriptor(new SourceDescriptor(root.source(), root.name(),
                List.of(reorderedPayload, root.children().get(1)), Role.RECORD)))
                .isInstanceOf(AssertionError.class);

        SourceDescriptor typedValue = payload.children().get(2);
        SourceDescriptor missingSuppressedAddress = new SourceDescriptor(typedValue.source(), typedValue.name(),
                List.of(), Role.SUPPRESSED);
        SourceDescriptor missingSuppressedPayload = new SourceDescriptor(payload.source(), payload.name(), List.of(
                payload.children().get(0), payload.children().get(1), missingSuppressedAddress), Role.VARIANT);
        assertThatThrownBy(() -> validateDescriptor(new SourceDescriptor(root.source(), root.name(),
                List.of(missingSuppressedPayload, root.children().get(1)), Role.RECORD)))
                .isInstanceOf(AssertionError.class);

        SourceDescriptor suppressedAddress = typedValue.children().getFirst();
        SourceDescriptor renamedSuppressedAddress = new SourceDescriptor(suppressedAddress.source(), "renamed",
                suppressedAddress.children(), Role.SUPPRESSED);
        SourceDescriptor renamedTypedValue = new SourceDescriptor(typedValue.source(), typedValue.name(),
                List.of(renamedSuppressedAddress), Role.SUPPRESSED);
        SourceDescriptor renamedPayload = new SourceDescriptor(payload.source(), payload.name(), List.of(
                payload.children().get(0), payload.children().get(1), renamedTypedValue), Role.VARIANT);
        assertThatThrownBy(() -> validateDescriptor(new SourceDescriptor(root.source(), root.name(),
                List.of(renamedPayload, root.children().get(1)), Role.RECORD)))
                .isInstanceOf(AssertionError.class);

        MessageType optionalMetadata = parse("""
                message root {
                  optional group payload (VARIANT(1)) {
                    optional binary metadata;
                    required binary value;
                  }
                }
                """);
        assertThatThrownBy(() -> validateDescriptor(bind(optionalMetadata, layout("root", Role.RECORD,
                layout("payload", Role.VARIANT, layout("metadata", Role.LEAF), layout("value", Role.LEAF))))))
                .isInstanceOf(AssertionError.class);

        MessageType optionalValue = parse("""
                message root {
                  optional group payload (VARIANT(1)) {
                    required binary metadata;
                    optional binary value;
                  }
                }
                """);
        assertThatThrownBy(() -> validateDescriptor(bind(optionalValue, layout("root", Role.RECORD,
                layout("payload", Role.VARIANT, layout("metadata", Role.LEAF), layout("value", Role.LEAF))))))
                .isInstanceOf(AssertionError.class);

        Schema nullableMetadata = variantSchema(Schema.createUnion(Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.BYTES)), Schema.create(Schema.Type.BYTES));
        assertThatThrownBy(() -> renderType("variant", "/0:payload/", payload, nullableMetadata, new ArrayList<>()))
                .isInstanceOf(AssertionError.class);

        Schema stringValue = variantSchema(Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.STRING));
        assertThatThrownBy(() -> renderType("variant", "/0:payload/", payload, stringValue, new ArrayList<>()))
                .isInstanceOf(AssertionError.class);
    }

    private static List<String> renderNamedTypeOccurrences(SourceDescriptor source, Schema schema) {
        List<String> occurrences = new ArrayList<>();
        renderRoot("comparison", source, schema, occurrences);
        return occurrences;
    }

    private static List<String> renderFixedLogicalTypes(Schema schema) {
        List<String> fixedTypes = new ArrayList<>();
        collectFixedLogicalTypes(schema, fixedTypes, new HashSet<>());
        return fixedTypes;
    }

    private static void collectFixedLogicalTypes(Schema schema, List<String> fixedTypes, Set<Schema> visited) {
        schema = unwrap(schema);
        if (!visited.add(schema)) {
            return;
        }
        switch (schema.getType()) {
            case RECORD -> schema.getFields().forEach(field -> collectFixedLogicalTypes(field.schema(), fixedTypes, visited));
            case FIXED -> fixedTypes.add(schema.getFullName() + " | " + schema.getFixedSize() + " | "
                    + fixedLogicalTypeAttributes(schema));
            case ARRAY -> collectFixedLogicalTypes(schema.getElementType(), fixedTypes, visited);
            case MAP -> collectFixedLogicalTypes(schema.getValueType(), fixedTypes, visited);
            default -> {
            }
        }
    }

    private static String fixedLogicalTypeAttributes(Schema schema) {
        org.apache.avro.LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal decimal) {
            return "decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
        }
        return nullText(logicalType == null ? null : logicalType.getName());
    }

    private static Schema decimalFixed(int precision, int scale) {
        return LogicalTypes.decimal(precision, scale).addToSchema(Schema.createFixed("amount", null, null, 5));
    }

    private static Schema variantSchema(Schema metadata, Schema value) {
        return Schema.createRecord("payload", null, null, false, List.of(
                new Schema.Field("metadata", metadata, null, null),
                new Schema.Field("value", value, null, null)));
    }

    private static FileSchema nestedAddressSchema() {
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

    private static FileSchema hardwoodSchema(MessageType source) {
        List<SchemaElement> elements = new ArrayList<>();
        elements.add(new SchemaElement(source.getName(), null, null, null, source.getFieldCount(), null,
                null, null, null, null));
        appendElements(source.getFields(), elements);
        return FileSchema.fromSchemaElements(elements);
    }

    private static void appendElements(List<Type> types, List<SchemaElement> elements) {
        for (Type type : types) {
            RepetitionType repetition = switch (type.getRepetition()) {
                case REQUIRED -> RepetitionType.REQUIRED;
                case OPTIONAL -> RepetitionType.OPTIONAL;
                case REPEATED -> RepetitionType.REPEATED;
            };
            if (type.isPrimitive()) {
                PrimitiveType primitive = type.asPrimitiveType();
                LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
                elements.add(new SchemaElement(type.getName(), physicalType(primitive), primitive.getTypeLength(),
                        repetition, null, null, decimalScale(annotation), decimalPrecision(annotation), null,
                        logicalType(annotation)));
            }
            else {
                GroupType group = type.asGroupType();
                LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
                elements.add(new SchemaElement(type.getName(), null, null, repetition, group.getFieldCount(),
                        null, null, null, null, logicalType(annotation)));
                appendElements(group.getFields(), elements);
            }
        }
    }

    private static PhysicalType physicalType(PrimitiveType primitive) {
        return switch (primitive.getPrimitiveTypeName()) {
            case BOOLEAN -> PhysicalType.BOOLEAN;
            case INT32 -> PhysicalType.INT32;
            case INT64 -> PhysicalType.INT64;
            case INT96 -> PhysicalType.INT96;
            case FLOAT -> PhysicalType.FLOAT;
            case DOUBLE -> PhysicalType.DOUBLE;
            case BINARY -> PhysicalType.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PhysicalType.FIXED_LEN_BYTE_ARRAY;
        };
    }

    private static Integer decimalScale(LogicalTypeAnnotation annotation) {
        return annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal
                ? decimal.getScale() : null;
    }

    private static Integer decimalPrecision(LogicalTypeAnnotation annotation) {
        return annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal
                ? decimal.getPrecision() : null;
    }

    private static LogicalType logicalType(LogicalTypeAnnotation annotation) {
        if (annotation == null) {
            return null;
        }
        if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return new LogicalType.StringType();
        }
        if (annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            return new LogicalType.EnumType();
        }
        if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
            return new LogicalType.UuidType();
        }
        if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return new LogicalType.DecimalType(decimal.getScale(), decimal.getPrecision());
        }
        if (annotation instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation) {
            return new LogicalType.IntervalType();
        }
        if (annotation instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return new LogicalType.Float16Type();
        }
        if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            return new LogicalType.ListType();
        }
        if (annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            return new LogicalType.MapType();
        }
        if (annotation instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation variant) {
            return new LogicalType.VariantType(variant.getSpecVersion());
        }
        return null;
    }

    private static String renderFixtures(List<Fixture> fixtures) {
        StringBuilder output = new StringBuilder();
        for (Fixture fixture : fixtures) {
            output.append("## ").append(fixture.id()).append('\n');
            try {
                SourceDescriptor descriptor = fixture.descriptor();
                validateDescriptor(descriptor);
                Schema avro = fixture.converter().convert(fixture.schema());
                output.append("supported\n");
                List<String> occurrences = new ArrayList<>();
                renderRoot(fixture.id(), descriptor, avro, occurrences);
                for (int i = 0; i < occurrences.size(); i++) {
                    output.append(i + 1).append(" | ").append(occurrences.get(i)).append('\n');
                }
            }
            catch (RuntimeException e) {
                output.append("unsupported | ").append(e.getClass().getName()).append(" | ")
                        .append(e.getMessage()).append('\n');
            }
            output.append('\n');
        }
        return output.toString();
    }

    private static void assertGoldenSections(String golden, List<Fixture> fixtures) {
        List<String> sections = new ArrayList<>();
        for (String line : golden.split("\\R")) {
            if (line.startsWith("## ")) {
                sections.add(line.substring(3));
            }
        }
        assertThat(sections).doesNotHaveDuplicates();
        assertThat(sections).containsExactlyElementsOf(fixtures.stream().map(Fixture::id).toList());
    }

    private static String stripLicenseHeader(String text) {
        int firstFixture = text.indexOf("## ");
        if (firstFixture < 0) {
            throw new IllegalArgumentException("Golden has no fixture sections: " + GOLDEN);
        }
        return text.substring(firstFixture);
    }

    private static void renderRoot(String fixtureId, SourceDescriptor source, Schema avro,
            List<String> occurrences) {
        record(fixtureId, "/", source.source(), avro, occurrences);
        renderFields(fixtureId, "/", source.children(), avro.getFields(), occurrences);
    }

    private static void renderFields(String fixtureId, String parentSite, List<SourceDescriptor> sourceFields,
            List<Schema.Field> avroFields, List<String> occurrences) {
        assertThat(avroFields).hasSize(sourceFields.size());
        for (int index = 0; index < sourceFields.size(); index++) {
            SourceDescriptor source = sourceFields.get(index);
            Schema.Field avro = avroFields.get(index);
            assertThat(avro.name()).isEqualTo(source.name());
            renderType(fixtureId, site(parentSite, index, source.source()), source, unwrap(avro.schema()), occurrences);
        }
    }

    private static void renderType(String fixtureId, String sourceSite, SourceDescriptor descriptor, Schema avro,
            List<String> occurrences) {
        Type source = descriptor.source();
        if (source.isPrimitive()) {
            if (avro.getType() == Schema.Type.FIXED) {
                fixed(fixtureId, sourceSite, source, avro, occurrences);
            }
            return;
        }

        GroupType group = source.asGroupType();
        LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
        if (annotation instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation) {
            assertThat(avro.getType()).isEqualTo(Schema.Type.RECORD);
            assertThat(avro.getFields()).extracting(Schema.Field::name).containsExactly("metadata", "value");
            assertThat(avro.getFields().getFirst().schema().getType()).isEqualTo(Schema.Type.BYTES);
            assertThat(avro.getFields().get(1).schema().getType()).isEqualTo(Schema.Type.BYTES);
            record(fixtureId, sourceSite, source, avro, occurrences);
            return;
        }
        if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            assertThat(avro.getType()).isEqualTo(Schema.Type.ARRAY);
            SourceDescriptor element = descriptor.children().get(0);
            renderType(fixtureId, site(sourceSite, 0, element.source()), element,
                    unwrap(avro.getElementType()), occurrences);
            return;
        }
        if (annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            assertThat(avro.getType()).isEqualTo(Schema.Type.MAP);
            GroupType keyValue = group.getType(0).asGroupType();
            SourceDescriptor value = descriptor.children().get(0).children().get(1);
            renderType(fixtureId, site(site(sourceSite, 0, keyValue), 1, value.source()), value,
                    unwrap(avro.getValueType()), occurrences);
            return;
        }

        assertThat(avro.getType()).isEqualTo(Schema.Type.RECORD);
        record(fixtureId, sourceSite, source, avro, occurrences);
        renderFields(fixtureId, sourceSite, descriptor.children(), avro.getFields(), occurrences);
    }

    private static SourceDescriptor bind(MessageType source, SourceLayout layout) {
        return bind(source, layout, false);
    }

    private static SourceDescriptor bind(Type source, SourceLayout layout, boolean suppressed) {
        assertThat(layout.name()).isEqualTo(source.getName());
        Role role = suppressed ? Role.SUPPRESSED : layout.role();
        if (source.isPrimitive()) {
            assertThat(layout.children()).isEmpty();
            return new SourceDescriptor(source, layout.name(), List.of(), role);
        }
        GroupType group = source.asGroupType();
        assertThat(layout.children()).hasSize(group.getFieldCount());
        boolean variant = group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
        List<SourceDescriptor> children = new ArrayList<>();
        for (int index = 0; index < group.getFieldCount(); index++) {
            SourceLayout child = layout.children().get(index);
            children.add(bind(group.getType(index), child,
                    suppressed || (variant && "typed_value".equals(child.name()))));
        }
        return new SourceDescriptor(source, layout.name(), List.copyOf(children), role);
    }

    private static SourceLayout layout(String name, Role role, SourceLayout... children) {
        return new SourceLayout(name, role, List.of(children));
    }

    private static void validateDescriptor(SourceDescriptor descriptor) {
        validateDescriptor(descriptor, false);
    }

    private static void validateDescriptor(SourceDescriptor descriptor, boolean suppressed) {
        Type source = descriptor.source();
        assertThat(descriptor.name()).isEqualTo(source.getName());
        if (source.isPrimitive()) {
            assertThat(descriptor.children()).isEmpty();
            if (suppressed) {
                assertThat(descriptor.role()).isEqualTo(Role.SUPPRESSED);
            }
            else if (source.asPrimitiveType().getPrimitiveTypeName()
                    == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                    || source.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                assertThat(descriptor.role()).isEqualTo(Role.FIXED);
            }
            else {
                assertThat(descriptor.role()).isEqualTo(Role.LEAF);
            }
            return;
        }
        GroupType group = source.asGroupType();
        assertThat(descriptor.children()).hasSize(group.getFieldCount());
        boolean variant = group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
        for (int index = 0; index < group.getFieldCount(); index++) {
            assertThat(descriptor.children().get(index).source()).isSameAs(group.getType(index));
            validateDescriptor(descriptor.children().get(index), suppressed
                    || (variant && "typed_value".equals(descriptor.children().get(index).name())));
        }
        if (group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation) {
            assertThat(descriptor.role()).isEqualTo(Role.VARIANT);
            if (group.getFieldCount() == 2) {
                assertThat(group.getFields()).extracting(Type::getName).containsExactly("metadata", "value");
            }
            else {
                assertThat(group.getFields()).extracting(Type::getName)
                        .containsExactly("metadata", "value", "typed_value");
            }
            assertThat(group.getType(0).isPrimitive()).isTrue();
            assertThat(group.getType(1).isPrimitive()).isTrue();
            assertThat(group.getType(0).asPrimitiveType().getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
            assertThat(group.getType(0).getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
            assertThat(group.getType(1).asPrimitiveType().getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
            assertThat(group.getType(1).getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
            if (group.getFieldCount() == 3) {
                assertThat(descriptor.children().get(2).role()).isEqualTo(Role.SUPPRESSED);
                validateDescriptor(descriptor.children().get(2), true);
            }
        }
        else if (group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            assertThat(descriptor.role()).isEqualTo(Role.ARRAY);
            assertThat(group.getFieldCount()).isEqualTo(1);
        }
        else if (group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            assertThat(descriptor.role()).isEqualTo(Role.MAP);
            assertThat(group.getFieldCount()).isEqualTo(1);
            assertThat(group.getType(0).isPrimitive()).isFalse();
            assertThat(group.getType(0).asGroupType().getFieldCount()).isGreaterThanOrEqualTo(2);
        }
        else if (suppressed) {
            assertThat(descriptor.role()).isEqualTo(Role.SUPPRESSED);
        }
        else {
            assertThat(descriptor.role()).isEqualTo(suppressed ? Role.SUPPRESSED : Role.RECORD);
        }
    }

    private static void record(String fixtureId, String sourceSite, Type source, Schema avro,
            List<String> occurrences) {
        occurrences.add(render(fixtureId, sourceSite, source, avro, "RECORD", "-"));
    }

    private static void fixed(String fixtureId, String sourceSite, Type source, Schema avro,
            List<String> occurrences) {
        occurrences.add(render(fixtureId, sourceSite, source, avro, "FIXED", Integer.toString(avro.getFixedSize())));
    }

    private static String render(String fixtureId, String sourceSite, Type source, Schema avro, String kind,
            String size) {
        return sourceSite + " | " + source.getName() + " | " + avro.getName() + " | "
                + nullText(avro.getNamespace()) + " | " + avro.getFullName() + " | " + kind + " | " + size;
    }

    private static String site(String parent, int index, Type source) {
        return parent + index + ":" + source.getName() + "/";
    }

    private static String nullText(String value) {
        return value == null ? "null" : value;
    }

    private static Schema unwrap(Schema schema) {
        if (!schema.isUnion()) {
            return schema;
        }
        return schema.getTypes().stream().filter(type -> type.getType() != Schema.Type.NULL).findFirst()
                .orElseThrow();
    }

    private static List<Fixture> fixtures() {
        List<Fixture> fixtures = new ArrayList<>();
        fixtures.add(fixture("nested-different", """
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """, layout("root", Role.RECORD,
                layout("home", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))),
                layout("work", Role.RECORD, layout("address", Role.RECORD, layout("zip", Role.LEAF))))));
        fixtures.add(fixture("nested-identical", """
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional binary city (STRING); } }
                }
                """, layout("root", Role.RECORD,
                layout("home", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))),
                layout("work", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))))));
        fixtures.add(fixture("list-and-map", """
                message root {
                  optional group homes (LIST) {
                    repeated group array { optional binary city (STRING); optional int32 zip; }
                  }
                  optional group offices (MAP) {
                    repeated group key_value {
                      required binary key (STRING);
                      optional group value { optional binary city (STRING); }
                    }
                  }
                }
                """, layout("root", Role.RECORD,
                layout("homes", Role.ARRAY, layout("array", Role.RECORD, layout("city", Role.LEAF),
                        layout("zip", Role.LEAF))),
                layout("offices", Role.MAP, layout("key_value", Role.RECORD, layout("key", Role.LEAF),
                        layout("value", Role.RECORD, layout("city", Role.LEAF)))))));
        fixtures.add(fixture("variant-canonical", """
                message root {
                  optional group payload (VARIANT(1)) {
                    required binary metadata;
                    required binary value;
                  }
                }
                """, layout("root", Role.RECORD, layout("payload", Role.VARIANT,
                layout("metadata", Role.LEAF), layout("value", Role.LEAF)))));
        fixtures.add(fixture("variant-shredded", """
                message root {
                  optional group payload (VARIANT(1)) {
                    required binary metadata;
                    required binary value;
                    optional group typed_value { optional fixed_len_byte_array(4) address; }
                  }
                  optional fixed_len_byte_array(4) address;
                }
                """, layout("root", Role.RECORD, layout("payload", Role.VARIANT,
                layout("metadata", Role.LEAF), layout("value", Role.LEAF),
                layout("typed_value", Role.SUPPRESSED, layout("address", Role.SUPPRESSED))),
                layout("address", Role.FIXED))));
        fixtures.add(fixture("fixed", """
                message root {
                  optional fixed_len_byte_array(4) token;
                  optional group nested { optional fixed_len_byte_array(8) token; }
                  optional fixed_len_byte_array(5) amount (DECIMAL(9,2));
                }
                """, layout("root", Role.RECORD, layout("token", Role.FIXED),
                layout("nested", Role.RECORD, layout("token", Role.FIXED)), layout("amount", Role.FIXED))));
        fixtures.add(fixture("logical-fixed", """
                message root {
                  optional group interval_holder {
                    optional fixed_len_byte_array(12) special (INTERVAL);
                  }
                  optional group interval_plain_holder {
                    optional fixed_len_byte_array(12) special;
                  }
                  optional group float_holder {
                    optional fixed_len_byte_array(2) special (FLOAT16);
                  }
                  optional group float_plain_holder {
                    optional fixed_len_byte_array(2) special;
                  }
                  optional group uuid_holder {
                    optional fixed_len_byte_array(16) special (UUID);
                  }
                  optional group uuid_plain_holder {
                    optional fixed_len_byte_array(16) special;
                  }
                }
                """, layout("root", Role.RECORD,
                layout("interval_holder", Role.RECORD, layout("special", Role.FIXED)),
                layout("interval_plain_holder", Role.RECORD, layout("special", Role.FIXED)),
                layout("float_holder", Role.RECORD, layout("special", Role.FIXED)),
                layout("float_plain_holder", Role.RECORD, layout("special", Role.FIXED)),
                layout("uuid_holder", Role.RECORD, layout("special", Role.FIXED)),
                layout("uuid_plain_holder", Role.RECORD, layout("special", Role.FIXED)))));
        fixtures.add(fixture("qualified-root", """
                message acme.row {
                  optional group acme { optional group row { optional int32 value; } }
                }
                """, layout("acme.row", Role.RECORD,
                layout("acme", Role.RECORD, layout("row", Role.RECORD, layout("value", Role.LEAF))))));
        fixtures.add(fixture("address", """
                message schema {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """, layout("schema", Role.RECORD,
                layout("home", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))),
                layout("work", Role.RECORD, layout("address", Role.RECORD, layout("zip", Role.LEAF))))));
        fixtures.add(fixture("address-preceded", """
                message schema {
                  optional group before { optional group address { optional boolean flag; } }
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """, layout("schema", Role.RECORD,
                layout("before", Role.RECORD, layout("address", Role.RECORD, layout("flag", Role.LEAF))),
                layout("home", Role.RECORD, layout("address", Role.RECORD, layout("city", Role.LEAF))),
                layout("work", Role.RECORD, layout("address", Role.RECORD, layout("zip", Role.LEAF))))));
        MessageType int96 = parse("""
                message root {
                  optional int96 instant;
                  optional group nested { optional int96 instant; }
                }
                """);
        fixtures.add(new Fixture("int96-fixed", int96, int96Converter(),
                bind(int96, layout("root", Role.RECORD, layout("instant", Role.FIXED),
                        layout("nested", Role.RECORD, layout("instant", Role.FIXED)))), true));
        fixtures.add(fixture("int96-rejected", """
                message root {
                  optional int96 instant;
                }
                """, layout("root", Role.RECORD, layout("instant", Role.FIXED))));
        fixtures.add(fixture("record-fixed-collision", """
                message root {
                  optional group a { optional group token { optional int32 x; } }
                  optional group b { optional fixed_len_byte_array(4) token; }
                }
                """, layout("root", Role.RECORD,
                layout("a", Role.RECORD, layout("token", Role.RECORD, layout("x", Role.LEAF))),
                layout("b", Role.RECORD, layout("token", Role.FIXED)))));
        return fixtures;
    }

    private static Fixture fixture(String id, String schema, SourceLayout layout) {
        MessageType message = parse(schema);
        return new Fixture(id, message, new AvroSchemaConverter(), bind(message, layout), false);
    }

    private static MessageType parse(String schema) {
        return MessageTypeParser.parseMessageType(schema);
    }

    private static AvroSchemaConverter int96Converter() {
        Configuration configuration = new Configuration(false);
        configuration.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        return new AvroSchemaConverter(configuration);
    }

    private record Fixture(String id, MessageType schema, AvroSchemaConverter converter,
            SourceDescriptor descriptor, boolean readInt96AsFixed) {
    }

    private enum Role {
        RECORD,
        FIXED,
        LEAF,
        ARRAY,
        MAP,
        VARIANT,
        SUPPRESSED
    }

    private record SourceDescriptor(Type source, String name, List<SourceDescriptor> children, Role role) {
    }

    private record SourceLayout(String name, Role role, List<SourceLayout> children) {
    }
}
