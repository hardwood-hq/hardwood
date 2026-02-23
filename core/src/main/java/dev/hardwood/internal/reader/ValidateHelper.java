/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.SchemaNode;

/**
 * Validation helpers for columnar flyweight implementations.
 * Delegates to {@link ValueConverter} validation where possible.
 */
final class ValidateHelper {

    private ValidateHelper() {
    }

    static void validateInt(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.INT32);
    }

    static void validateLong(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.INT64);
    }

    static void validateFloat(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.FLOAT);
    }

    static void validateDouble(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.DOUBLE);
    }

    static void validateBoolean(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.BOOLEAN);
    }

    static void validateString(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.BYTE_ARRAY);
    }

    static void validateBinary(SchemaNode schema) {
        ValueConverter.validatePhysicalType(schema, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }

    static void validateLogicalType(SchemaNode schema, Class<? extends LogicalType> expectedType) {
        ValueConverter.validateLogicalType(schema, expectedType);
    }

    static void validateGroupType(SchemaNode schema, boolean expectList, boolean expectMap) {
        ValueConverter.validateGroupType(schema, expectList, expectMap);
    }
}
