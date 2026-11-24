/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.metadata;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/// Factories for the common kinds of [SchemaElement], checking what one element can check on its own.
/// A complete list is checked by `FileSchema.fromSchemaElements`.
public final class SchemaElements {

    private SchemaElements() {
    }

    /// Creates a root group element, which carries no repetition.
    ///
    /// @param name        group name; the root group is named `"schema"` by convention
    /// @param numChildren number of child elements, zero or more
    /// @return the group element
    /// @throws IllegalArgumentException if `numChildren` is negative
    public static SchemaElement root(String name, int numChildren) {
        return groupElement(name, null, numChildren, null);
    }

    /// Creates a group element.
    ///
    /// @param name group name
    /// @param repetitionType repetition level; only the root element may omit it, via [#root]
    /// @param numChildren number of child elements, zero or more
    /// @return the group element
    /// @throws IllegalArgumentException if `repetitionType` is `null` or `numChildren` is negative
    public static SchemaElement group(String name, RepetitionType repetitionType, int numChildren) {
        return group(name, repetitionType, numChildren, null);
    }

    /// Creates a group element.
    ///
    /// @param name group name
    /// @param repetitionType repetition level; only the root element may omit it, via [#root]
    /// @param numChildren number of child elements, zero or more
    /// @param logicalType logical type annotation, or `null` for none
    /// @return the group element
    /// @throws IllegalArgumentException if `repetitionType` is `null` or `numChildren` is negative
    public static SchemaElement group(String name, RepetitionType repetitionType, int numChildren,
            LogicalType logicalType) {
        requireRepetition(name, repetitionType);
        return groupElement(name, repetitionType, numChildren, logicalType);
    }

    private static SchemaElement groupElement(String name, RepetitionType repetitionType, int numChildren,
            LogicalType logicalType) {
        if (numChildren < 0) {
            throw new IllegalArgumentException(
                    "Group " + name + " requires a child count of zero or more, not " + numChildren);
        }
        return new SchemaElement(name, null, null, repetitionType, numChildren, null, null, null, null, logicalType);
    }

    /// Creates a primitive element
    ///
    /// @param name column name
    /// @param type physical type
    /// @param repetitionType repetition level
    /// @return the primitive element
    /// @throws IllegalArgumentException if `repetitionType` is `null`, or `type` is `null` or is
    ///         [PhysicalType#FIXED_LEN_BYTE_ARRAY], which needs a width
    public static SchemaElement primitive(String name, PhysicalType type, RepetitionType repetitionType) {
        return primitive(name, type, repetitionType, null);
    }

    /// Creates a primitive element.
    ///
    /// @param name column name
    /// @param type physical type
    /// @param repetitionType repetition level
    /// @param logicalType nullable logical type annotation
    /// @return the primitive element
    /// @throws IllegalArgumentException if `repetitionType` is `null`, or `type` is `null` or is
    ///         [PhysicalType#FIXED_LEN_BYTE_ARRAY], which needs a width
    public static SchemaElement primitive(String name, PhysicalType type, RepetitionType repetitionType,
            LogicalType logicalType) {
        requireRepetition(name, repetitionType);
        if (type == null) {
            throw new IllegalArgumentException(
                    "Primitive element " + name + " requires a physical type; a null type denotes a group");
        }
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY column " + name
                    + " requires a positive type length; use fixedLengthPrimitive instead");
        }
        return new SchemaElement(name, type, null, repetitionType, null, null, null, null, null, logicalType);
    }

    /// Creates a [PhysicalType#FIXED_LEN_BYTE_ARRAY] element.
    ///
    /// @param name column name
    /// @param typeLength fixed byte length: must be positive
    /// @param repetitionType repetition level
    /// @return the fixed-length primitive element
    /// @throws IllegalArgumentException if `repetitionType` is `null` or `typeLength` is not positive
    public static SchemaElement fixedLengthPrimitive(String name, int typeLength, RepetitionType repetitionType) {
        return fixedLengthPrimitive(name, typeLength, repetitionType, null);
    }

    /// Creates a [PhysicalType#FIXED_LEN_BYTE_ARRAY] element.
    ///
    /// @param name column name
    /// @param typeLength fixed byte length: must be positive
    /// @param repetitionType repetition level
    /// @param logicalType nullable logical type annotation
    /// @return the fixed-length primitive element
    /// @throws IllegalArgumentException if `repetitionType` is `null` or `typeLength` is not positive
    public static SchemaElement fixedLengthPrimitive(String name, int typeLength, RepetitionType repetitionType,
            LogicalType logicalType) {
        requireRepetition(name, repetitionType);
        if (typeLength <= 0) {
            throw new IllegalArgumentException(
                    "FIXED_LEN_BYTE_ARRAY column " + name + " requires a positive type length, not " + typeLength);
        }
        return new SchemaElement(name, PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength, repetitionType, null, null, null,
                null, null, logicalType);
    }

    private static void requireRepetition(String name, RepetitionType repetitionType) {
        if (repetitionType == null) {
            throw new IllegalArgumentException(
                    "Element " + name + " requires a repetition level; only the root element may omit it");
        }
    }
}
