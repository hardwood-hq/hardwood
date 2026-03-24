/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.example.data;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;

/// Abstract class for reading Parquet record data.
///
/// Compatible with parquet-java's Group API. Provides type-safe access to field
/// values by name or index. The by-name methods delegate to the by-index methods
/// via [#getType()].[GroupType#getFieldIndex(String)].
public abstract class Group {

    // ---- Schema access ----

    /// Get the schema type for this group.
    ///
    /// @return the group type schema
    public abstract GroupType getType();

    // ---- Field repetition count ----

    /// Get the repetition count for a field by index.
    ///
    /// @param fieldIndex the field index
    /// @return the repetition count
    public abstract int getFieldRepetitionCount(int fieldIndex);

    /// Get the repetition count for a field by name.
    ///
    /// @param field the field name
    /// @return the repetition count
    public int getFieldRepetitionCount(String field) {
        return getFieldRepetitionCount(getType().getFieldIndex(field));
    }

    // ---- Abstract by-index accessors ----

    public abstract String getString(int fieldIndex, int index);

    public abstract int getInteger(int fieldIndex, int index);

    public abstract long getLong(int fieldIndex, int index);

    public abstract double getDouble(int fieldIndex, int index);

    public abstract float getFloat(int fieldIndex, int index);

    public abstract boolean getBoolean(int fieldIndex, int index);

    public abstract Binary getBinary(int fieldIndex, int index);

    public abstract Group getGroup(int fieldIndex, int index);

    // ---- Concrete by-name accessors (delegate to by-index) ----

    public String getString(String field, int index) {
        return getString(getType().getFieldIndex(field), index);
    }

    public int getInteger(String field, int index) {
        return getInteger(getType().getFieldIndex(field), index);
    }

    public long getLong(String field, int index) {
        return getLong(getType().getFieldIndex(field), index);
    }

    public double getDouble(String field, int index) {
        return getDouble(getType().getFieldIndex(field), index);
    }

    public float getFloat(String field, int index) {
        return getFloat(getType().getFieldIndex(field), index);
    }

    public boolean getBoolean(String field, int index) {
        return getBoolean(getType().getFieldIndex(field), index);
    }

    public Binary getBinary(String field, int index) {
        return getBinary(getType().getFieldIndex(field), index);
    }

    public Group getGroup(String field, int index) {
        return getGroup(getType().getFieldIndex(field), index);
    }
}
