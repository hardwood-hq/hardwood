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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqList interface.
 */
public class PqListImpl implements PqList {

    private final List<?> elements;
    private final SchemaNode.GroupNode listSchema;
    private final SchemaNode elementSchema;

    public PqListImpl(List<?> elements, SchemaNode.GroupNode listSchema) {
        this.elements = elements;
        this.listSchema = listSchema;
        this.elementSchema = listSchema.getListElement();
    }

    @Override
    public <T> Iterable<T> getValues(PqType<T> elementType) {
        validateElementType(elementType);
        return () -> new ConvertingIterator<>(elements.iterator(), elementType);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    private void validateElementType(PqType<?> type) {
        if (elementSchema == null) {
            throw new IllegalStateException("List has no element schema");
        }

        // Validate using instanceof checks (Java 17 compatible)
        if (type instanceof PqType.BooleanType) {
            validatePrimitiveElement(PhysicalType.BOOLEAN);
        }
        else if (type instanceof PqType.Int32Type) {
            validatePrimitiveElement(PhysicalType.INT32);
        }
        else if (type instanceof PqType.Int64Type) {
            validatePrimitiveElement(PhysicalType.INT64);
        }
        else if (type instanceof PqType.FloatType) {
            validatePrimitiveElement(PhysicalType.FLOAT);
        }
        else if (type instanceof PqType.DoubleType) {
            validatePrimitiveElement(PhysicalType.DOUBLE);
        }
        else if (type instanceof PqType.BinaryType) {
            validatePrimitiveElement(PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        }
        else if (type instanceof PqType.StringType) {
            validateStringElement();
        }
        else if (type instanceof PqType.DateType) {
            validateLogicalElement(LogicalType.DateType.class);
        }
        else if (type instanceof PqType.TimeType) {
            validateLogicalElement(LogicalType.TimeType.class);
        }
        else if (type instanceof PqType.TimestampType) {
            validateLogicalElement(LogicalType.TimestampType.class);
        }
        else if (type instanceof PqType.DecimalType) {
            validateLogicalElement(LogicalType.DecimalType.class);
        }
        else if (type instanceof PqType.UuidType) {
            validateLogicalElement(LogicalType.UuidType.class);
        }
        else if (type instanceof PqType.RowType) {
            validateGroupElement(false);
        }
        else if (type instanceof PqType.ListType) {
            validateGroupElement(true);
        }
    }

    private void validatePrimitiveElement(PhysicalType... expectedTypes) {
        if (!(elementSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException("List elements are not primitive types");
        }
        for (PhysicalType expected : expectedTypes) {
            if (primitive.type() == expected) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "List elements have physical type " + primitive.type()
                        + ", expected one of " + java.util.Arrays.toString(expectedTypes));
    }

    private void validateStringElement() {
        if (!(elementSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException("List elements are not primitive types");
        }
        // Accept BYTE_ARRAY with or without STRING logical type
        if (primitive.type() != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "List elements have physical type " + primitive.type() + ", expected BYTE_ARRAY for STRING");
        }
    }

    private void validateLogicalElement(Class<? extends LogicalType> expectedType) {
        if (!(elementSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException("List elements are not primitive types");
        }
        LogicalType logicalType = primitive.logicalType();
        if (logicalType == null || !expectedType.isInstance(logicalType)) {
            throw new IllegalArgumentException(
                    "List elements have logical type " + (logicalType == null ? "none" : logicalType.getClass().getSimpleName())
                            + ", expected " + expectedType.getSimpleName());
        }
    }

    private void validateGroupElement(boolean expectList) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group)) {
            throw new IllegalArgumentException("List elements are not group types");
        }
        if (expectList && !group.isList()) {
            throw new IllegalArgumentException("List elements are not lists");
        }
        if (!expectList && group.isList()) {
            throw new IllegalArgumentException("List elements are lists, not structs");
        }
    }

    private class ConvertingIterator<T> implements Iterator<T> {
        private final Iterator<?> delegate;
        private final PqType<T> type;

        ConvertingIterator(Iterator<?> delegate, PqType<T> type) {
            this.delegate = delegate;
            this.type = type;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            Object rawValue = delegate.next();
            if (rawValue == null) {
                return null;
            }
            return (T) convertElement(rawValue, type);
        }
    }

    @SuppressWarnings("unchecked")
    private Object convertElement(Object rawValue, PqType<?> type) {
        // Convert using instanceof checks (Java 17 compatible)
        if (type instanceof PqType.BooleanType) {
            return (Boolean) rawValue;
        }
        else if (type instanceof PqType.Int32Type) {
            return (Integer) rawValue;
        }
        else if (type instanceof PqType.Int64Type) {
            return (Long) rawValue;
        }
        else if (type instanceof PqType.FloatType) {
            return (Float) rawValue;
        }
        else if (type instanceof PqType.DoubleType) {
            return (Double) rawValue;
        }
        else if (type instanceof PqType.BinaryType) {
            return (byte[]) rawValue;
        }
        else if (type instanceof PqType.StringType) {
            if (rawValue instanceof String) {
                return rawValue;
            }
            return new String((byte[]) rawValue, StandardCharsets.UTF_8);
        }
        else if (type instanceof PqType.DateType) {
            return convertLogicalElement(rawValue, LocalDate.class);
        }
        else if (type instanceof PqType.TimeType) {
            return convertLogicalElement(rawValue, LocalTime.class);
        }
        else if (type instanceof PqType.TimestampType) {
            return convertLogicalElement(rawValue, Instant.class);
        }
        else if (type instanceof PqType.DecimalType) {
            return convertLogicalElement(rawValue, BigDecimal.class);
        }
        else if (type instanceof PqType.UuidType) {
            return convertLogicalElement(rawValue, UUID.class);
        }
        else if (type instanceof PqType.RowType) {
            Object[] arrayValue = (Object[]) rawValue;
            SchemaNode.GroupNode groupSchema = (SchemaNode.GroupNode) elementSchema;
            return new PqRowImpl(arrayValue, groupSchema);
        }
        else if (type instanceof PqType.ListType) {
            List<?> listValue = (List<?>) rawValue;
            SchemaNode.GroupNode nestedListSchema = (SchemaNode.GroupNode) elementSchema;
            return new PqListImpl(listValue, nestedListSchema);
        }
        else {
            throw new IllegalArgumentException("Unknown PqType: " + type.getClass().getSimpleName());
        }
    }

    private <T> T convertLogicalElement(Object rawValue, Class<T> expectedClass) {
        // If already converted (e.g., by RecordAssembler for nested lists), return as-is
        if (expectedClass.isInstance(rawValue)) {
            return expectedClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) elementSchema;
        Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
        return expectedClass.cast(converted);
    }
}
