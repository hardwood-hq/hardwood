/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(expected).isEqualTo(actual);

        assertThat(actual).contains("address2.address");
        assertThat(actual).contains("address3.address");
        assertThat(actual).contains("unsupported | java.lang.IllegalArgumentException | "
                + "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.");
    }

    private static String renderFixtures(List<Fixture> fixtures) {
        StringBuilder output = new StringBuilder();
        for (Fixture fixture : fixtures) {
            output.append("## ").append(fixture.id()).append('\n');
            try {
                Schema avro = fixture.converter().convert(fixture.schema());
                output.append("supported\n");
                List<String> occurrences = new ArrayList<>();
                renderRoot(fixture.id(), fixture.schema(), avro, occurrences);
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

    private static void renderRoot(String fixtureId, MessageType source, Schema avro, List<String> occurrences) {
        record(fixtureId, "/", source, avro, occurrences);
        renderFields(fixtureId, "/", source.getFields(), avro.getFields(), occurrences);
    }

    private static void renderFields(String fixtureId, String parentSite, List<Type> sourceFields,
            List<Schema.Field> avroFields, List<String> occurrences) {
        assertThat(avroFields).hasSize(sourceFields.size());
        for (int index = 0; index < sourceFields.size(); index++) {
            Type source = sourceFields.get(index);
            Schema.Field avro = avroFields.get(index);
            assertThat(avro.name()).isEqualTo(source.getName());
            renderType(fixtureId, site(parentSite, index, source), source, unwrap(avro.schema()), occurrences);
        }
    }

    private static void renderType(String fixtureId, String sourceSite, Type source, Schema avro,
            List<String> occurrences) {
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
            Type element = group.getType(0);
            renderType(fixtureId, site(sourceSite, 0, element), element, unwrap(avro.getElementType()), occurrences);
            return;
        }
        if (annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            assertThat(avro.getType()).isEqualTo(Schema.Type.MAP);
            GroupType keyValue = group.getType(0).asGroupType();
            Type value = keyValue.getType(1);
            renderType(fixtureId, site(site(sourceSite, 0, keyValue), 1, value), value,
                    unwrap(avro.getValueType()), occurrences);
            return;
        }

        assertThat(avro.getType()).isEqualTo(Schema.Type.RECORD);
        record(fixtureId, sourceSite, source, avro, occurrences);
        renderFields(fixtureId, sourceSite, group.getFields(), avro.getFields(), occurrences);
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
}
