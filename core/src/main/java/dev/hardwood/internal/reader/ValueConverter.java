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
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.SchemaNode;

/// Shared validation and conversion logic for PqStruct, PqList, and PqMap implementations.
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
        // Accept either physical FLOAT or FLBA(2) annotated FLOAT16; the latter
        // decodes the half-precision payload to a single-precision Float so
        // callers don't need to know the on-disk encoding.
        if (schema instanceof SchemaNode.PrimitiveNode primitive
                && primitive.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                && primitive.logicalType() instanceof LogicalType.Float16Type) {
            return convertLogicalType(rawValue, schema, Float.class);
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
        if (schema instanceof SchemaNode.PrimitiveNode primitive && primitive.type() == PhysicalType.INT96) {
            return LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
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

    public static PqInterval convertToInterval(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        validateLogicalType(schema, LogicalType.IntervalType.class);
        return convertLogicalType(rawValue, schema, PqInterval.class);
    }

    // ==================== Generic Type Conversion ====================

    /// Convert a primitive value based on schema type.
    /// Group types (struct/list/map) are handled directly by flyweight implementations.
    ///
    /// Thin SchemaNode-aware shim over [LogicalTypeConverter#convert]: the
    /// underlying decode table lives there so flat and nested reader paths
    /// share a single source of truth. The shim only handles the SchemaNode
    /// unwrap, the unannotated-INT96 → [Instant] convention, and short-circuits
    /// group nodes for the flyweight path.
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

        if (logicalType == null && primitive.type() == PhysicalType.INT96) {
            // INT96 carries no logical type but is conventionally a TIMESTAMP.
            return LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
        }

        return LogicalTypeConverter.convert(rawValue, primitive.type(), logicalType);
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

    /// Package-private decode helper that skips schema validation. Intended for
    /// hot-path call sites (typed [dev.hardwood.row.PqList] iterators) that
    /// have already validated `schema` against the expected logical type — pre-flight
    /// validation hoists out of the per-element lambda, leaving each lambda call
    /// at one `instanceof` short-circuit plus a delegate to [LogicalTypeConverter].
    static <T> T convertLogicalType(Object rawValue, SchemaNode schema, Class<T> expectedClass) {
        // If already converted (e.g., by RecordAssembler for nested structures), return as-is
        if (expectedClass.isInstance(rawValue)) {
            return expectedClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) schema;
        Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
        return expectedClass.cast(converted);
    }
}
