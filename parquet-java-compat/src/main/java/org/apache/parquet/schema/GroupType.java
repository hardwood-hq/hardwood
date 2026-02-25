/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parquet group type (struct, list, or map).
 * <p>
 * Represents a group of fields, which can be a struct, list, or map depending
 * on the original type annotation.
 * </p>
 */
public class GroupType extends Type {

    private final List<Type> fields;
    private final Map<String, Integer> fieldsByName;
    private final OriginalType originalType;

    /**
     * Create a group type without an original type.
     *
     * @param repetition the repetition
     * @param name the field name
     * @param fields the child fields
     */
    public GroupType(Repetition repetition, String name, List<Type> fields) {
        this(repetition, name, null, fields);
    }

    /**
     * Create a group type with an original type.
     *
     * @param repetition the repetition
     * @param name the field name
     * @param originalType the original/converted type (LIST, MAP, etc.)
     * @param fields the child fields
     */
    public GroupType(Repetition repetition, String name, OriginalType originalType, List<Type> fields) {
        super(name, repetition);
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.originalType = originalType;

        Map<String, Integer> byName = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            byName.put(fields.get(i).getName(), i);
        }
        this.fieldsByName = Collections.unmodifiableMap(byName);
    }

    /**
     * Create a group type with varargs fields.
     *
     * @param repetition the repetition
     * @param name the field name
     * @param fields the child fields
     */
    public GroupType(Repetition repetition, String name, Type... fields) {
        this(repetition, name, null, List.of(fields));
    }

    /**
     * Get the list of child fields.
     *
     * @return the fields
     */
    public List<Type> getFields() {
        return fields;
    }

    /**
     * Get the number of child fields.
     *
     * @return the field count
     */
    public int getFieldCount() {
        return fields.size();
    }

    /**
     * Get a field by index.
     *
     * @param index the field index
     * @return the field type
     */
    public Type getType(int index) {
        return fields.get(index);
    }

    /**
     * Get a field by name.
     *
     * @param name the field name
     * @return the field type
     * @throws IllegalArgumentException if the field is not found
     */
    public Type getType(String name) {
        Integer index = fieldsByName.get(name);
        if (index == null) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return fields.get(index);
    }

    /**
     * Get the index of a field by name.
     *
     * @param name the field name
     * @return the field index
     * @throws IllegalArgumentException if the field is not found
     */
    public int getFieldIndex(String name) {
        Integer index = fieldsByName.get(name);
        if (index == null) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return index;
    }

    /**
     * Check if a field exists.
     *
     * @param name the field name
     * @return true if the field exists
     */
    public boolean containsField(String name) {
        return fieldsByName.containsKey(name);
    }

    /**
     * Get the original/converted type.
     *
     * @return the original type (LIST, MAP, etc.), or null if plain struct
     */
    public OriginalType getOriginalType() {
        return originalType;
    }

    @Override
    public GroupType asGroupType() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getRepetition()).append(" group ").append(getName());
        if (originalType != null) {
            sb.append(" (").append(originalType).append(")");
        }
        sb.append(" {\n");
        for (Type field : fields) {
            sb.append("  ").append(field).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
