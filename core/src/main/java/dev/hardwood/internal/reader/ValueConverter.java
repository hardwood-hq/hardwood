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

/// Shared decode helpers used by `PqStruct`, `PqList`, and `PqMap` flyweights.
public final class ValueConverter {

    private ValueConverter() {
    }

    // ==================== Primitive Type Conversions ====================

    public static Integer convertToInt(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return (Integer) rawValue;
    }

    public static Long convertToLong(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return (Long) rawValue;
    }

    public static Float convertToFloat(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        // FLBA(2) annotated FLOAT16 decodes the half-precision payload to a
        // single-precision Float so callers don't need to know the on-disk encoding.
        if (schema instanceof SchemaNode.PrimitiveNode primitive
                && primitive.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                && primitive.logicalType() instanceof LogicalType.Float16Type) {
            return convertLogicalType(rawValue, schema, Float.class);
        }
        return (Float) rawValue;
    }

    public static Double convertToDouble(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return (Double) rawValue;
    }

    public static Boolean convertToBoolean(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return (Boolean) rawValue;
    }

    // ==================== Object Type Conversions ====================

    public static String convertToString(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        if (rawValue instanceof String) {
            return (String) rawValue;
        }
        return new String((byte[]) rawValue, StandardCharsets.UTF_8);
    }

    public static byte[] convertToBinary(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return (byte[]) rawValue;
    }

    public static LocalDate convertToDate(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return convertLogicalType(rawValue, schema, LocalDate.class);
    }

    public static LocalTime convertToTime(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return convertLogicalType(rawValue, schema, LocalTime.class);
    }

    public static Instant convertToTimestamp(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        if (schema instanceof SchemaNode.PrimitiveNode primitive && primitive.type() == PhysicalType.INT96) {
            return LogicalTypeConverter.int96ToInstant((byte[]) rawValue);
        }
        return convertLogicalType(rawValue, schema, Instant.class);
    }

    public static BigDecimal convertToDecimal(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return convertLogicalType(rawValue, schema, BigDecimal.class);
    }

    public static UUID convertToUuid(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
        return convertLogicalType(rawValue, schema, UUID.class);
    }

    public static PqInterval convertToInterval(Object rawValue, SchemaNode schema) {
        if (rawValue == null) {
            return null;
        }
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
