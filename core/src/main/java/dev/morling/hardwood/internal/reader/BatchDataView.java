/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqStruct;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Provides typed access to a batch of column data.
 * Implementations handle flat vs nested schema differences.
 */
public sealed interface BatchDataView permits FlatBatchDataView, NestedBatchDataView {

    /**
     * Creates the appropriate BatchDataView for the given schema.
     */
    static BatchDataView create(FileSchema schema, ProjectedSchema projectedSchema) {
        return schema.isFlatSchema()
                ? new FlatBatchDataView(schema, projectedSchema)
                : new NestedBatchDataView(schema, projectedSchema);
    }

    /**
     * Update with new batch data after loading.
     */
    void setBatchData(TypedColumnData[] columnData);

    /**
     * Called after advancing to a new row.
     */
    void setRowIndex(int rowIndex);

    // Primitive accessors (by name)
    int getInt(String name);
    long getLong(String name);
    float getFloat(String name);
    double getDouble(String name);
    boolean getBoolean(String name);

    // Primitive accessors (by projected index)
    int getInt(int projectedIndex);
    long getLong(int projectedIndex);
    float getFloat(int projectedIndex);
    double getDouble(int projectedIndex);
    boolean getBoolean(int projectedIndex);

    // Object accessors (by name)
    String getString(String name);
    byte[] getBinary(String name);
    LocalDate getDate(String name);
    LocalTime getTime(String name);
    Instant getTimestamp(String name);
    BigDecimal getDecimal(String name);
    UUID getUuid(String name);

    // Object accessors (by projected index)
    String getString(int projectedIndex);
    byte[] getBinary(int projectedIndex);
    LocalDate getDate(int projectedIndex);
    LocalTime getTime(int projectedIndex);
    Instant getTimestamp(int projectedIndex);
    BigDecimal getDecimal(int projectedIndex);
    UUID getUuid(int projectedIndex);

    // Nested type accessors (by name - not supported for flat schemas)
    PqStruct getStruct(String name);
    PqIntList getListOfInts(String name);
    PqLongList getListOfLongs(String name);
    PqDoubleList getListOfDoubles(String name);
    PqList getList(String name);
    PqMap getMap(String name);

    // Nested type accessors (by projected index - not supported for flat schemas)
    PqStruct getStruct(int projectedIndex);
    PqIntList getListOfInts(int projectedIndex);
    PqLongList getListOfLongs(int projectedIndex);
    PqDoubleList getListOfDoubles(int projectedIndex);
    PqList getList(int projectedIndex);
    PqMap getMap(int projectedIndex);

    // Null check
    boolean isNull(String name);
    boolean isNull(int projectedIndex);

    // Generic value access
    Object getValue(String name);
    Object getValue(int projectedIndex);

    // Metadata
    int getFieldCount();
    String getFieldName(int projectedIndex);

    /**
     * Get the underlying flat column data arrays for direct primitive access.
     *
     * @return the flat column data array, or null if nested schema
     */
    FlatColumnData[] getFlatColumnData();
}
