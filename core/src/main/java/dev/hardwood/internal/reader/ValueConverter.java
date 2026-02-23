/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.SchemaNode;

/**
 * Shared validation and conversion logic for PqStruct, PqList, and PqMap implementations.
 */
public final class ValueConverter {

    private ValueConverter() {
    }

    // ==================== Primitive Type Conversions ====================

    public static Integer convertToInt(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validatePhysicalType(schema, PhysicalType.INT32);
        return (Integer) rawValue;
    }

    public static Long convertToLong(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validatePhysicalType(schema, PhysicalType.INT64);
        return (Long) rawValue;
    }

    public static Float convertToFloat(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validatePhysicalType(schema, PhysicalType.FLOAT);
        return (Float) rawValue;
    }

    public static Double convertToDouble(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validatePhysicalType(schema, PhysicalType.DOUBLE);
        return (Double) rawValue;
    }

    public static Boolean convertToBoolean(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validatePhysicalType(schema, PhysicalType.BOOLEAN);
        return (Boolean) rawValue;
    }

    // ==================== Object Type Conversions ====================

    public static String convertToString(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateStringType(schema);
        if (rawValue instanceof String) {
            return (String) rawValue;
        }
        return new String((byte[]) rawValue, StandardCharsets.UTF_8);
    }

    public static byte[] convertToBinary(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validatePhysicalType(schema, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        return (byte[]) rawValue;
    }

    public static LocalDate convertToDate(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateLogicalType(schema, LogicalType.DateType.class);
        return convertLogicalType(rawValue, schema, LocalDate.class);
    }

    public static LocalTime convertToTime(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateLogicalType(schema, LogicalType.TimeType.class);
        return convertLogicalType(rawValue, schema, LocalTime.class);
    }

    public static Instant convertToTimestamp(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateLogicalType(schema, LogicalType.TimestampType.class);
        return convertLogicalType(rawValue, schema, Instant.class);
    }

    public static BigDecimal convertToDecimal(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateLogicalType(schema, LogicalType.DecimalType.class);
        return convertLogicalType(rawValue, schema, BigDecimal.class);
    }

    public static UUID convertToUuid(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateLogicalType(schema, LogicalType.UuidType.class);
        return convertLogicalType(rawValue, schema, UUID.class);
    }

    // ==================== Generic Type Conversion ====================

    /**
     * Convert a primitive value based on schema type.
     * Group types (struct/list/map) are handled directly by flyweight implementations.
     */
    static Object convertValue(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }

        if (schema instanceof SchemaNode.GroupNode) {
            // Group types should not pass through ValueConverter in the flyweight path;
            // return raw value as-is.
            return rawValue;
        }

        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
        LogicalType logicalType = primitive.logicalType();

        // Handle logical types first
        if (logicalType instanceof LogicalType.DateType) {
            return convertToDate(rawValue, schema);
        }
        else if (logicalType instanceof LogicalType.TimeType) {
            return convertToTime(rawValue, schema);
        }
        else if (logicalType instanceof LogicalType.TimestampType) {
            return convertToTimestamp(rawValue, schema);
        }
        else if (logicalType instanceof LogicalType.DecimalType) {
            return convertToDecimal(rawValue, schema);
        }
        else if (logicalType instanceof LogicalType.UuidType) {
            return convertToUuid(rawValue, schema);
        }
        else if (logicalType instanceof LogicalType.StringType) {
            return convertToString(rawValue, schema);
        }

        // Fall back to physical type
        return switch (primitive.type()) {
            case INT32 -> convertToInt(rawValue, schema);
            case INT64 -> convertToLong(rawValue, schema);
            case FLOAT -> convertToFloat(rawValue, schema);
            case DOUBLE -> convertToDouble(rawValue, schema);
            case BOOLEAN -> convertToBoolean(rawValue, schema);
            case BYTE_ARRAY -> convertToString(rawValue, schema);
            case FIXED_LEN_BYTE_ARRAY -> convertToBinary(rawValue, schema);
            case INT96 -> rawValue; // Legacy timestamp, return as-is
        };
    }

    // ==================== Validation Helpers ====================

    static void validatePhysicalType(SchemaNode schema, PhysicalType... expectedTypes) {
        if (!(schema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a primitive type");
        }
        for (PhysicalType expected : expectedTypes) {
            if (primitive.type() == expected) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "Field '" + schema.name() + "' has physical type " + primitive.type()
                        + ", expected " + (expectedTypes.length == 1 ? expectedTypes[0] : java.util.Arrays.toString(expectedTypes)));
    }

    private static void validateStringType(SchemaNode schema) {
        if (!(schema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a primitive type");
        }
        // STRING can be BYTE_ARRAY with or without STRING logical type annotation
        if (primitive.type() != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' has physical type " + primitive.type()
                            + ", expected BYTE_ARRAY for STRING");
        }
    }

    static void validateLogicalType(SchemaNode schema, Class<? extends LogicalType> expectedType) {
        if (!(schema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a primitive type");
        }
        LogicalType logicalType = primitive.logicalType();
        if (logicalType == null || !expectedType.isInstance(logicalType)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' has logical type "
                            + (logicalType == null ? "none" : logicalType.getClass().getSimpleName())
                            + ", expected " + expectedType.getSimpleName());
        }
    }

    static void validateGroupType(SchemaNode schema, boolean expectList, boolean expectMap) {
        if (!(schema instanceof SchemaNode.GroupNode group)) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a group type");
        }
        if (expectList && !group.isList()) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a list");
        }
        if (expectMap && !group.isMap()) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is not a map");
        }
        if (!expectList && !expectMap && (group.isList() || group.isMap())) {
            throw new IllegalArgumentException(
                    "Field '" + schema.name() + "' is a list or map, not a struct");
        }
    }

    private static <T> T convertLogicalType(Object rawValue, SchemaNode schema, Class<T> expectedClass) {
        // If already converted (e.g., by RecordAssembler for nested structures), return as-is
        if (expectedClass.isInstance(rawValue)) {
            return expectedClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
        Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
        return expectedClass.cast(converted);
    }
}
