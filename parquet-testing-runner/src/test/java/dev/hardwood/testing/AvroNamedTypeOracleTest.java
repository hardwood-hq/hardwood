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

    private static final Set<String> REQUIRED_IDS = Set.of(
            "nested-different", "nested-identical", "list-and-map", "variant-canonical",
            "variant-shredded", "fixed", "logical-fixed", "qualified-root", "address",
            "address-preceded", "int96-fixed", "int96-rejected");

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
            Files.writeString(GOLDEN, actual, StandardCharsets.UTF_8);
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
    void hardwoodCompatibilityMatchesReferenceForNestedAndProjectedSchemas() {
        MessageType reference = parse("""
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """);
        FileSchema hardwood = nestedAddressSchema();

        assertThat(renderNamedTypeAttributes(new AvroSchemaConverter().convert(reference)))
                .containsExactlyElementsOf(renderNamedTypeAttributes(
                        dev.hardwood.avro.internal.AvroSchemaConverter.planForParquetAvroCompatibility(
                                hardwood, ColumnProjection.all()).avro()));

        MessageType projectedReference = parse("""
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                }
                """);
        assertThat(renderNamedTypeAttributes(new AvroSchemaConverter().convert(projectedReference)))
                .containsExactlyElementsOf(renderNamedTypeAttributes(
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
            assertThat(renderNamedTypeAttributes(dev.hardwood.avro.internal.AvroSchemaConverter
                    .planForParquetAvroCompatibility(hardwood, ColumnProjection.all()).avro()))
                    .as(fixture.id())
                    .containsExactlyElementsOf(renderNamedTypeAttributes(reference));
        }
    }

    @Test
    void hardwoodCompatibilityMatchesProjectedListMapAndKeyOnlyMapReference() {
        MessageType projectedNested = parse("""
                message root {
                  optional group retained { optional group address { optional int32 city; } }
                  optional group items (LIST) {
                    repeated group list {
                      optional group address { optional int32 city; }
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
        assertThat(renderNamedTypeAttributes(new AvroSchemaConverter().convert(projectedNested)))
                .containsExactlyElementsOf(renderNamedTypeAttributes(
                        dev.hardwood.avro.internal.AvroSchemaConverter
                                .planForParquetAvroCompatibility(full, projection).avro()));

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
        assertThat(renderNamedTypeAttributes(new AvroSchemaConverter().convert(fullMap)))
                .containsExactlyElementsOf(renderNamedTypeAttributes(
                        dev.hardwood.avro.internal.AvroSchemaConverter.planForParquetAvroCompatibility(
                                mapSchema, ColumnProjection.columns("people.key_value.key")).avro()));
    }

    @Test
    void variantDescriptorRejectsPhysicalMutations() {
        Fixture fixture = fixtures().stream().filter(candidate -> candidate.id().equals("variant-shredded"))
                .findFirst().orElseThrow();
        SourceDescriptor root = describeRoot(fixture.schema());
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

        SourceDescriptor renamedPayload = new SourceDescriptor(payload.source(), "renamed", payload.children(),
                Role.VARIANT);
        assertThatThrownBy(() -> validateDescriptor(new SourceDescriptor(root.source(), root.name(),
                List.of(renamedPayload, root.children().get(1)), Role.RECORD)))
                .isInstanceOf(AssertionError.class);
    }

    private static List<String> renderNamedTypes(Schema schema) {
        List<String> namedTypes = new ArrayList<>();
        collectNamedTypes(schema, namedTypes, new HashSet<>());
        return namedTypes;
    }

    private static List<String> renderNamedTypeAttributes(Schema schema) {
        List<String> namedTypes = new ArrayList<>();
        collectNamedTypeAttributes(schema, namedTypes, new HashSet<>());
        return namedTypes;
    }

    private static void collectNamedTypeAttributes(Schema schema, List<String> namedTypes, Set<Schema> visited) {
        if (!visited.add(schema)) {
            return;
        }
        switch (schema.getType()) {
            case RECORD -> {
                namedTypes.add(schema.getFullName() + " | RECORD | -");
                for (Schema.Field field : schema.getFields()) {
                    collectNamedTypeAttributes(field.schema(), namedTypes, visited);
                }
            }
            case FIXED -> namedTypes.add(schema.getFullName() + " | FIXED | " + schema.getFixedSize());
            case ARRAY -> collectNamedTypeAttributes(schema.getElementType(), namedTypes, visited);
            case MAP -> collectNamedTypeAttributes(schema.getValueType(), namedTypes, visited);
            case UNION -> schema.getTypes().forEach(type -> collectNamedTypeAttributes(type, namedTypes, visited));
            default -> {
            }
        }
    }

    private static void collectNamedTypes(Schema schema, List<String> namedTypes, Set<Schema> visited) {
        if (!visited.add(schema)) {
            return;
        }
        switch (schema.getType()) {
            case RECORD -> {
                namedTypes.add(schema.getFullName());
                for (Schema.Field field : schema.getFields()) {
                    collectNamedTypes(field.schema(), namedTypes, visited);
                }
            }
            case FIXED -> namedTypes.add(schema.getFullName());
            case ARRAY -> collectNamedTypes(schema.getElementType(), namedTypes, visited);
            case MAP -> collectNamedTypes(schema.getValueType(), namedTypes, visited);
            case UNION -> schema.getTypes().forEach(type -> collectNamedTypes(type, namedTypes, visited));
            default -> {
            }
        }
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
                SourceDescriptor descriptor = describeRoot(fixture.schema());
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

    private static SourceDescriptor describeRoot(MessageType source) {
        return new SourceDescriptor(source, source.getName(), source.getFields().stream()
                .map(field -> describe(field, false)).toList(), Role.RECORD);
    }

    private static SourceDescriptor describe(Type source, boolean suppressed) {
        boolean physicallySuppressed = suppressed;
        Role role;
        if (physicallySuppressed) {
            role = Role.SUPPRESSED;
        }
        else if (source.isPrimitive()) {
            PrimitiveType primitive = source.asPrimitiveType();
            role = primitive.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                    || primitive.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96
                    ? Role.FIXED : Role.LEAF;
        }
        else {
            GroupType group = source.asGroupType();
            LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
            role = annotation instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation ? Role.VARIANT
                    : annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation ? Role.ARRAY
                    : annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation ? Role.MAP
                    : Role.RECORD;
        }
        boolean variant = !source.isPrimitive()
                && source.asGroupType().getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
        List<SourceDescriptor> children = source.isPrimitive() ? List.of()
                : source.asGroupType().getFields().stream().map(child -> describe(child,
                        physicallySuppressed || (variant && "typed_value".equals(child.getName())))).toList();
        return new SourceDescriptor(source, source.getName(), children, role);
    }

    private static void validateDescriptor(SourceDescriptor descriptor) {
        validateDescriptor(descriptor, false);
    }

    private static void validateDescriptor(SourceDescriptor descriptor, boolean suppressed) {
        Type source = descriptor.source();
        assertThat(descriptor.name()).isEqualTo(source.getName());
        if (source.isPrimitive()) {
            assertThat(descriptor.children()).isEmpty();
            assertThat(descriptor.role()).isIn(Role.LEAF, Role.FIXED, Role.SUPPRESSED);
            if (suppressed) {
                assertThat(descriptor.role()).isEqualTo(Role.SUPPRESSED);
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
            assertThat(group.getType(1).asPrimitiveType().getPrimitiveTypeName())
                    .isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
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
                """));
        fixtures.add(fixture("nested-identical", """
                message root {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional binary city (STRING); } }
                }
                """));
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
                """));
        fixtures.add(fixture("variant-canonical", """
                message root {
                  optional group payload (VARIANT(1)) {
                    required binary metadata;
                    required binary value;
                  }
                }
                """));
        fixtures.add(fixture("variant-shredded", """
                message root {
                  optional group payload (VARIANT(1)) {
                    required binary metadata;
                    required binary value;
                    optional group typed_value { optional fixed_len_byte_array(4) address; }
                  }
                  optional fixed_len_byte_array(4) address;
                }
                """));
        fixtures.add(fixture("fixed", """
                message root {
                  optional fixed_len_byte_array(4) token;
                  optional group nested { optional fixed_len_byte_array(8) token; }
                  optional fixed_len_byte_array(5) amount (DECIMAL(9,2));
                }
                """));
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
                """));
        fixtures.add(fixture("qualified-root", """
                message acme.row {
                  optional group acme { optional group row { optional int32 value; } }
                }
                """));
        fixtures.add(fixture("address", """
                message schema {
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """));
        fixtures.add(fixture("address-preceded", """
                message schema {
                  optional group before { optional group address { optional boolean flag; } }
                  optional group home { optional group address { optional binary city (STRING); } }
                  optional group work { optional group address { optional int32 zip; } }
                }
                """));
        fixtures.add(new Fixture("int96-fixed", parse("""
                message root {
                  optional int96 instant;
                }
                """), int96Converter()));
        fixtures.add(fixture("int96-rejected", """
                message root {
                  optional int96 instant;
                }
                """));
        return fixtures;
    }

    private static Fixture fixture(String id, String schema) {
        return new Fixture(id, parse(schema), new AvroSchemaConverter());
    }

    private static MessageType parse(String schema) {
        return MessageTypeParser.parseMessageType(schema);
    }

    private static AvroSchemaConverter int96Converter() {
        Configuration configuration = new Configuration(false);
        configuration.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        return new AvroSchemaConverter(configuration);
    }

    private record Fixture(String id, MessageType schema, AvroSchemaConverter converter) {
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
}
