/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.schema;

/**
 * Parquet primitive type (leaf column).
 * <p>
 * Represents a primitive column type such as INT32, INT64, BOOLEAN, etc.
 * </p>
 */
public class PrimitiveType extends Type {

    /**
     * Primitive type names as defined in the Parquet specification.
     */
    public enum PrimitiveTypeName {
        /** Boolean */
        BOOLEAN,
        /** 32-bit signed integer */
        INT32,
        /** 64-bit signed integer */
        INT64,
        /** 96-bit integer (legacy timestamp) */
        INT96,
        /** 32-bit floating point */
        FLOAT,
        /** 64-bit floating point */
        DOUBLE,
        /** Variable length byte array */
        BINARY,
        /** Fixed length byte array */
        FIXED_LEN_BYTE_ARRAY
    }

    private final PrimitiveTypeName primitiveTypeName;
    private final OriginalType originalType;
    private final int length;

    /**
     * Create a primitive type without an original type.
     *
     * @param repetition the repetition
     * @param primitive the primitive type name
     * @param name the field name
     */
    public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive, String name) {
        this(repetition, primitive, name, null);
    }

    /**
     * Create a primitive type with an original type.
     *
     * @param repetition the repetition
     * @param primitive the primitive type name
     * @param name the field name
     * @param originalType the original/converted type
     */
    public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                         String name, OriginalType originalType) {
        this(repetition, primitive, 0, name, originalType);
    }

    /**
     * Create a primitive type with length (for FIXED_LEN_BYTE_ARRAY).
     *
     * @param repetition the repetition
     * @param primitive the primitive type name
     * @param length the fixed length (for FIXED_LEN_BYTE_ARRAY)
     * @param name the field name
     * @param originalType the original/converted type
     */
    public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                         int length, String name, OriginalType originalType) {
        super(name, repetition);
        this.primitiveTypeName = primitive;
        this.originalType = originalType;
        this.length = length;
    }

    /**
     * Get the primitive type name.
     *
     * @return the primitive type name
     */
    public PrimitiveTypeName getPrimitiveTypeName() {
        return primitiveTypeName;
    }

    /**
     * Get the original/converted type.
     *
     * @return the original type, or null if none
     */
    public OriginalType getOriginalType() {
        return originalType;
    }

    /**
     * Get the type length (for FIXED_LEN_BYTE_ARRAY).
     *
     * @return the length, or 0 if not applicable
     */
    public int getTypeLength() {
        return length;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public PrimitiveType asPrimitiveType() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getRepetition()).append(" ");
        sb.append(primitiveTypeName);
        if (length > 0) {
            sb.append("(").append(length).append(")");
        }
        sb.append(" ").append(getName());
        if (originalType != null) {
            sb.append(" (").append(originalType).append(")");
        }
        return sb.toString();
    }
}
