/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqRow interface.
 */
public class PqRowImpl implements PqRow {

    private final Object[] values;
    private final SchemaNode.GroupNode schema;

    /**
     * Constructor for top-level rows.
     */
    public PqRowImpl(Object[] values, FileSchema fileSchema) {
        this.values = values;
        this.schema = fileSchema.getRootNode();
    }

    /**
     * Constructor for nested struct rows.
     */
    public PqRowImpl(Object[] values, SchemaNode.GroupNode structSchema) {
        this.values = values;
        this.schema = structSchema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(PqType<T> type, int index) {
        Object rawValue = values[index];
        SchemaNode fieldSchema = schema.children().get(index);
        return (T) convertValue(rawValue, type, fieldSchema);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(PqType<T> type, String name) {
        int index = getFieldIndex(name);
        Object rawValue = values[index];
        SchemaNode fieldSchema = schema.children().get(index);
        return (T) convertValue(rawValue, type, fieldSchema);
    }

    @Override
    public boolean isNull(int index) {
        return values[index] == null;
    }

    @Override
    public boolean isNull(String name) {
        return values[getFieldIndex(name)] == null;
    }

    @Override
    public int getFieldCount() {
        return schema.children().size();
    }

    @Override
    public String getFieldName(int index) {
        return schema.children().get(index).name();
    }

    private int getFieldIndex(String name) {
        List<SchemaNode> children = schema.children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    @SuppressWarnings("unchecked")
    private Object convertValue(Object rawValue, PqType<?> type, SchemaNode fieldSchema) {
        if (rawValue == null) {
            return null;
        }

        // Validate and convert based on PqType using instanceof checks (Java 17 compatible)
        if (type instanceof PqType.BooleanType) {
            validatePhysicalType(fieldSchema, PhysicalType.BOOLEAN);
            return (Boolean) rawValue;
        }
        else if (type instanceof PqType.Int32Type) {
            validatePhysicalType(fieldSchema, PhysicalType.INT32);
            return (Integer) rawValue;
        }
        else if (type instanceof PqType.Int64Type) {
            validatePhysicalType(fieldSchema, PhysicalType.INT64);
            return (Long) rawValue;
        }
        else if (type instanceof PqType.FloatType) {
            validatePhysicalType(fieldSchema, PhysicalType.FLOAT);
            return (Float) rawValue;
        }
        else if (type instanceof PqType.DoubleType) {
            validatePhysicalType(fieldSchema, PhysicalType.DOUBLE);
            return (Double) rawValue;
        }
        else if (type instanceof PqType.BinaryType) {
            validatePhysicalType(fieldSchema, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
            return (byte[]) rawValue;
        }
        else if (type instanceof PqType.StringType) {
            validateLogicalType(fieldSchema, LogicalType.StringType.class, true);
            if (rawValue instanceof String) {
                return rawValue;
            }
            return new String((byte[]) rawValue, StandardCharsets.UTF_8);
        }
        else if (type instanceof PqType.DateType) {
            validateLogicalType(fieldSchema, LogicalType.DateType.class, false);
            return convertLogicalType(rawValue, fieldSchema, LocalDate.class);
        }
        else if (type instanceof PqType.TimeType) {
            validateLogicalType(fieldSchema, LogicalType.TimeType.class, false);
            return convertLogicalType(rawValue, fieldSchema, LocalTime.class);
        }
        else if (type instanceof PqType.TimestampType) {
            validateLogicalType(fieldSchema, LogicalType.TimestampType.class, false);
            return convertLogicalType(rawValue, fieldSchema, Instant.class);
        }
        else if (type instanceof PqType.DecimalType) {
            validateLogicalType(fieldSchema, LogicalType.DecimalType.class, false);
            return convertLogicalType(rawValue, fieldSchema, BigDecimal.class);
        }
        else if (type instanceof PqType.UuidType) {
            validateLogicalType(fieldSchema, LogicalType.UuidType.class, false);
            return convertLogicalType(rawValue, fieldSchema, UUID.class);
        }
        else if (type instanceof PqType.RowType) {
            validateGroupNode(fieldSchema, false);
            Object[] arrayValue = (Object[]) rawValue;
            SchemaNode.GroupNode groupSchema = (SchemaNode.GroupNode) fieldSchema;
            return new PqRowImpl(arrayValue, groupSchema);
        }
        else if (type instanceof PqType.ListType) {
            validateGroupNode(fieldSchema, true);
            List<?> listValue = (List<?>) rawValue;
            SchemaNode.GroupNode listSchema = (SchemaNode.GroupNode) fieldSchema;
            return new PqListImpl(listValue, listSchema);
        }
        else {
            throw new IllegalArgumentException("Unknown PqType: " + type.getClass().getSimpleName());
        }
    }

    private void validatePhysicalType(SchemaNode fieldSchema, PhysicalType... expectedTypes) {
        if (!(fieldSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + fieldSchema.name() + "' is not a primitive type");
        }
        for (PhysicalType expected : expectedTypes) {
            if (primitive.type() == expected) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "Field '" + fieldSchema.name() + "' has physical type " + primitive.type()
                        + ", expected one of " + java.util.Arrays.toString(expectedTypes));
    }

    private void validateLogicalType(SchemaNode fieldSchema, Class<? extends LogicalType> expectedType,
                                     boolean allowNoLogicalType) {
        if (!(fieldSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException(
                    "Field '" + fieldSchema.name() + "' is not a primitive type");
        }
        LogicalType logicalType = primitive.logicalType();
        if (logicalType == null) {
            if (!allowNoLogicalType) {
                throw new IllegalArgumentException(
                        "Field '" + fieldSchema.name() + "' has no logical type, expected " + expectedType.getSimpleName());
            }
            return;
        }
        if (!expectedType.isInstance(logicalType)) {
            throw new IllegalArgumentException(
                    "Field '" + fieldSchema.name() + "' has logical type " + logicalType.getClass().getSimpleName()
                            + ", expected " + expectedType.getSimpleName());
        }
    }

    private void validateGroupNode(SchemaNode fieldSchema, boolean expectList) {
        if (!(fieldSchema instanceof SchemaNode.GroupNode group)) {
            throw new IllegalArgumentException(
                    "Field '" + fieldSchema.name() + "' is not a group type");
        }
        if (expectList && !group.isList()) {
            throw new IllegalArgumentException(
                    "Field '" + fieldSchema.name() + "' is not a list");
        }
        if (!expectList && group.isList()) {
            throw new IllegalArgumentException(
                    "Field '" + fieldSchema.name() + "' is a list, not a struct");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T convertLogicalType(Object rawValue, SchemaNode fieldSchema, Class<T> expectedClass) {
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) fieldSchema;
        Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
        if (!expectedClass.isInstance(converted)) {
            throw new IllegalArgumentException(
                    "Conversion failed for field '" + fieldSchema.name() + "': expected " + expectedClass.getSimpleName()
                            + " but got " + converted.getClass().getSimpleName());
        }
        return (T) converted;
    }
}
