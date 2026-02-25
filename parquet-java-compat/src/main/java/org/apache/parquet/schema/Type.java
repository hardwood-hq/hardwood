/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.schema;

/**
 * Base class for Parquet schema types.
 * <p>
 * This provides API compatibility with parquet-java's Type class.
 * </p>
 */
public abstract class Type {

    /**
     * Repetition types for Parquet fields.
     */
    public enum Repetition {
        /** Field must be present exactly once */
        REQUIRED,
        /** Field may be absent (null) */
        OPTIONAL,
        /** Field may appear zero or more times */
        REPEATED
    }

    private final String name;
    private final Repetition repetition;

    /**
     * Create a new type.
     *
     * @param name the field name
     * @param repetition the repetition type
     */
    protected Type(String name, Repetition repetition) {
        this.name = name;
        this.repetition = repetition;
    }

    /**
     * Get the field name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the repetition type.
     *
     * @return the repetition
     */
    public Repetition getRepetition() {
        return repetition;
    }

    /**
     * Check if this is a primitive type.
     *
     * @return true if primitive, false if group
     */
    public boolean isPrimitive() {
        return false;
    }

    /**
     * Cast to PrimitiveType.
     *
     * @return this as a PrimitiveType
     * @throws ClassCastException if not a primitive type
     */
    public PrimitiveType asPrimitiveType() {
        throw new ClassCastException("Not a primitive type: " + this);
    }

    /**
     * Cast to GroupType.
     *
     * @return this as a GroupType
     * @throws ClassCastException if not a group type
     */
    public GroupType asGroupType() {
        throw new ClassCastException("Not a group type: " + this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{name='" + name + "', repetition=" + repetition + "}";
    }
}
