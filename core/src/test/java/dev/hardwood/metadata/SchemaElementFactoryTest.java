/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.schema.FileSchema;

import static dev.hardwood.metadata.SchemaElement.fixedLengthPrimitive;
import static dev.hardwood.metadata.SchemaElement.group;
import static dev.hardwood.metadata.SchemaElement.primitive;
import static dev.hardwood.metadata.SchemaElement.root;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaElementFactoryTest {

    @Test
    void groupEqualsCanonicalConstruction() {
        assertThat(group("address", RepetitionType.OPTIONAL, 3))
                .isEqualTo(new SchemaElement("address", null, null, RepetitionType.OPTIONAL, 3, null, null, null, null,
                        null));
    }

    @Test
    void annotatedGroupEqualsCanonicalConstruction() {
        LogicalType listType = new LogicalType.ListType();
        assertThat(group("items", RepetitionType.OPTIONAL, 1, listType))
                .isEqualTo(new SchemaElement("items", null, null, RepetitionType.OPTIONAL, 1, null, null, null, null,
                        listType));
    }

    @Test
    void primitiveEqualsCanonicalConstruction() {
        assertThat(primitive("zip", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .isEqualTo(new SchemaElement("zip", PhysicalType.INT32, null, RepetitionType.OPTIONAL, null, null, null,
                        null, null, null));
    }

    @Test
    void annotatedPrimitiveEqualsCanonicalConstruction() {
        LogicalType stringType = new LogicalType.StringType();
        assertThat(primitive("city", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, stringType))
                .isEqualTo(new SchemaElement("city", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL, null, null,
                        null, null, null, stringType));
    }

    @Test
    void fixedLengthPrimitiveEqualsCanonicalConstruction() {
        assertThat(fixedLengthPrimitive("token", 4, RepetitionType.OPTIONAL))
                .isEqualTo(new SchemaElement("token", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, RepetitionType.OPTIONAL,
                        null, null, null, null, null, null));
    }

    @Test
    void annotatedFixedLengthPrimitiveEqualsCanonicalConstruction() {
        LogicalType uuidType = new LogicalType.UuidType();
        assertThat(fixedLengthPrimitive("id", 16, RepetitionType.REQUIRED, uuidType))
                .isEqualTo(new SchemaElement("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, RepetitionType.REQUIRED, null,
                        null, null, null, null, uuidType));
    }

    @Test
    void groupIsAGroupAndPrimitivesArePrimitive() {
        assertThat(group("g", RepetitionType.OPTIONAL, 0).isGroup()).isTrue();
        assertThat(group("g", RepetitionType.OPTIONAL, 0).isPrimitive()).isFalse();
        assertThat(primitive("p", PhysicalType.INT32, RepetitionType.OPTIONAL).isPrimitive()).isTrue();
        assertThat(primitive("p", PhysicalType.INT32, RepetitionType.OPTIONAL).isGroup()).isFalse();
        assertThat(fixedLengthPrimitive("f", 4, RepetitionType.OPTIONAL).isPrimitive()).isTrue();
        assertThat(fixedLengthPrimitive("f", 4, RepetitionType.OPTIONAL).isGroup()).isFalse();
    }

    /// A root element carries no repetition. `RecordFilterMicroBenchmark` and many fixtures
    /// build one that way, and `FileSchema.fromSchemaElements` defaults it to `REQUIRED`.
    @Test
    void rootKeepsANullRepetition() {
        SchemaElement rootElement = root("root", 4);

        assertThat(rootElement.repetitionType()).isNull();
        assertThat(rootElement).isEqualTo(new SchemaElement("root", null, null, null, 4, null, null, null, null, null));
    }

    /// Only the root element may omit `repetition_type`; every other element must carry one,
    /// so the non-root factories reject a null rather than let `fromSchemaElements` silently
    /// default it to `OPTIONAL`.
    @Test
    void everyNonRootFactoryRejectsANullRepetition() {
        assertThatThrownBy(() -> group("g", null, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Element g requires a repetition level; only the root element may omit it")
                ;
        assertThatThrownBy(() -> group("g", null, 1, new LogicalType.ListType()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Element g requires a repetition level; only the root element may omit it");
        assertThatThrownBy(() -> primitive("p", PhysicalType.INT32, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Element p requires a repetition level; only the root element may omit it");
        assertThatThrownBy(() -> fixedLengthPrimitive("f", 4, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Element f requires a repetition level; only the root element may omit it");
    }

    @Test
    void aNullLogicalTypeMatchesTheShorterOverload() {
        assertThat(group("g", RepetitionType.OPTIONAL, 2, null)).isEqualTo(group("g", RepetitionType.OPTIONAL, 2));
        assertThat(primitive("p", PhysicalType.INT64, RepetitionType.REQUIRED, null))
                .isEqualTo(primitive("p", PhysicalType.INT64, RepetitionType.REQUIRED));
        assertThat(fixedLengthPrimitive("f", 8, RepetitionType.REQUIRED, null))
                .isEqualTo(fixedLengthPrimitive("f", 8, RepetitionType.REQUIRED));
    }

    /// The record accepts a null name, and so does the read path: a footer missing Thrift
    /// field 4 decodes to a null-named element that `FileSchema.toSchemaElements()` writes
    /// back out. A factory that rejected null would stop that round trip.
    @Test
    void everyFactoryAcceptsANullName() {
        assertThat(group(null, RepetitionType.REQUIRED, 1))
                .isEqualTo(new SchemaElement(null, null, null, RepetitionType.REQUIRED, 1, null, null, null, null,
                        null));
        assertThat(primitive(null, PhysicalType.INT32, RepetitionType.REQUIRED))
                .isEqualTo(new SchemaElement(null, PhysicalType.INT32, null, RepetitionType.REQUIRED, null, null, null,
                        null, null, null));
        assertThat(fixedLengthPrimitive(null, 4, RepetitionType.REQUIRED))
                .isEqualTo(new SchemaElement(null, PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, RepetitionType.REQUIRED, null,
                        null, null, null, null, null));
    }

    @Test
    void primitiveRejectsANullType() {
        assertThatThrownBy(() -> primitive("value", null, RepetitionType.OPTIONAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Primitive element value requires a physical type; a null type denotes a group")
                ;
    }

    @Test
    void primitiveRejectsFixedLenByteArray() {
        assertThatThrownBy(() -> primitive("token", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY column token requires a positive type length; use "
                         + "fixedLengthPrimitive instead")
                ;
    }

    @Test
    void groupRejectsANegativeChildCount() {
        assertThatThrownBy(() -> group("address", RepetitionType.OPTIONAL, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Group address requires a child count of zero or more, not -1");
    }

    @Test
    void groupAcceptsNoChildren() {
        assertThat(group("empty", RepetitionType.OPTIONAL, 0).numChildren()).isZero();
    }

    @Test
    void fixedLengthPrimitiveRejectsANonPositiveWidth() {
        assertThatThrownBy(() -> fixedLengthPrimitive("token", 0, RepetitionType.OPTIONAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY column token requires a positive type length, not 0");
        assertThatThrownBy(() -> fixedLengthPrimitive("token", -4, RepetitionType.OPTIONAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY column token requires a positive type length, not -4");
    }

    /// Record equality proves the factories match the constructor. The round trip also
    /// preserves the logical annotations, including their legacy converted-type forms.
    @Test
    void factoryBuiltElementsRoundTripThroughFileSchema() {
        List<SchemaElement> elements = List.of(
                root("root", 3),
                group("items", RepetitionType.OPTIONAL, 1, new LogicalType.ListType()),
                primitive("element", PhysicalType.INT32, RepetitionType.REQUIRED),
                primitive("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType()),
                fixedLengthPrimitive("id", 16, RepetitionType.REQUIRED, new LogicalType.UuidType()));

        FileSchema schema = FileSchema.fromSchemaElements(elements);

        assertThat(schema.getName()).isEqualTo("root");
        // The root comes back with REQUIRED because fromSchemaElements defaults an absent
        // root repetition; logical annotations also gain their legacy converted-type forms.
        assertThat(schema.toSchemaElements()).containsExactly(
                new SchemaElement("root", null, null, RepetitionType.REQUIRED, 3, null, null, null, null, null),
                new SchemaElement("items", null, null, RepetitionType.OPTIONAL, 1, ConvertedType.LIST, null, null,
                        null, new LogicalType.ListType()),
                elements.get(2),
                new SchemaElement("name", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL, null,
                        ConvertedType.UTF8, null, null, null, new LogicalType.StringType()),
                elements.get(4));
    }
}
