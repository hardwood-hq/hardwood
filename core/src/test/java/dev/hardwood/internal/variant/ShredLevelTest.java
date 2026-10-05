/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.LogicalType.TimeUnit;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A malformed shredding shape in a file's schema is a read failure.
class ShredLevelTest {

    @Test
    void unexpectedNestedShreddedChildIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of(binary("unexpected")));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Unexpected shredded-Variant child 'unexpected' in group 'field'");
    }

    @Test
    void nestedGroupWithNeitherValueNorTypedValueIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of());

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant group 'field' must contain at least one of 'value' or 'typed_value'");
    }

    @Test
    void valueColumnOfTheWrongPhysicalTypeIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of(
                new SchemaNode.PrimitiveNode("value", PhysicalType.INT32, RepetitionType.OPTIONAL, null, 0, 1, 0, null)));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant 'value' column must be BYTE_ARRAY, got INT32");
    }

    @Test
    void valueGroupIsAReadFailure() {
        SchemaNode.GroupNode field = group("field", List.of(group("value", List.of(binary("inner")))));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant 'value' child must be a primitive");
    }

    @Test
    void arrayTypedValueWithAPrimitiveElementIsAReadFailure() {
        SchemaNode.PrimitiveNode element =
                new SchemaNode.PrimitiveNode("element", PhysicalType.INT32, RepetitionType.OPTIONAL, null, 0, 3, 1, null);
        SchemaNode.GroupNode repeated = new SchemaNode.GroupNode(
                "list", RepetitionType.REPEATED, null, null, List.of(element), 2, 1, null);
        SchemaNode.GroupNode typedValue = new SchemaNode.GroupNode(
                "typed_value", RepetitionType.OPTIONAL, null, LogicalType.list(), List.of(repeated), 1, 0, null);
        SchemaNode.GroupNode field = group("field", List.of(typedValue));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant array typed_value must have a group element, got " + element);
    }

    @Test
    void objectTypedValueWithAPrimitiveFieldIsAReadFailure() {
        SchemaNode.GroupNode typedValue = group("typed_value", List.of(binary("name")));
        SchemaNode.GroupNode field = group("field", List.of(typedValue));

        assertThatThrownBy(() -> ShredLevel.buildNested(field, null))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded-Variant object field 'name' must be a group");
    }

    private static SchemaNode.GroupNode group(String name, List<SchemaNode> children) {
        return new SchemaNode.GroupNode(name, RepetitionType.OPTIONAL, null, null, children, 1, 0, null);
    }

    private static SchemaNode.PrimitiveNode binary(String name) {
        return new SchemaNode.PrimitiveNode(name, PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, null, 0, 1, 0, null);
    }

    /// Carriers the Variant shredding spec does not allow as a `typed_value`: a file using
    /// one is not valid, so each is rejected.
    static Stream<Arguments> unsupportedTypedValueCarriers() {
        return Stream.of(
                Arguments.of(PhysicalType.INT32, LogicalType.time(false, TimeUnit.MILLIS), "INT32 TIME(MILLIS, local)"),
                Arguments.of(PhysicalType.INT32, LogicalType.intType(8, false), "INT32 UINT_8"),
                Arguments.of(PhysicalType.INT32, LogicalType.intType(16, false), "INT32 UINT_16"),
                Arguments.of(PhysicalType.INT32, LogicalType.intType(32, false), "INT32 UINT_32"),
                Arguments.of(PhysicalType.INT64, LogicalType.time(false, TimeUnit.NANOS), "INT64 TIME(NANOS, local)"),
                Arguments.of(PhysicalType.INT64, LogicalType.time(true, TimeUnit.MICROS), "INT64 TIME(MICROS, UTC)"),
                Arguments.of(PhysicalType.INT64, LogicalType.intType(64, false), "INT64 UINT_64"),
                Arguments.of(PhysicalType.INT64, LogicalType.timestamp(true, TimeUnit.MILLIS),
                        "INT64 TIMESTAMP(MILLIS, UTC)"),
                Arguments.of(PhysicalType.INT96, null, "INT96"),
                Arguments.of(PhysicalType.BYTE_ARRAY, LogicalType.bson(), "BYTE_ARRAY BSON"),
                Arguments.of(PhysicalType.BYTE_ARRAY, LogicalType.json(), "BYTE_ARRAY JSON"),
                Arguments.of(PhysicalType.BYTE_ARRAY, LogicalType.enumType(), "BYTE_ARRAY ENUM"),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.timestamp(true, TimeUnit.NANOS),
                        "FIXED_LEN_BYTE_ARRAY TIMESTAMP(NANOS, UTC)"),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.float16(), "FIXED_LEN_BYTE_ARRAY FLOAT16"),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, null, "FIXED_LEN_BYTE_ARRAY"));
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypedValueCarriers")
    void typedValueCarrierWithoutAVariantTypeIsRejected(PhysicalType physicalType, LogicalType logicalType,
                                                        String carrier) {
        assertThatThrownBy(() -> new ShredLevel.Typed.Primitive(0, 1, physicalType, logicalType))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("Shredded Variant typed_value has type " + carrier
                        + ", which the Variant shredding specification does not allow");
    }
}
