/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.schema;

import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RepetitionType;

/**
 * Represents a primitive column in a Parquet schema.
 * Stores computed definition and repetition levels based on schema hierarchy.
 */
public record ColumnSchema(
        String name,
        PhysicalType type,
        RepetitionType repetitionType,
        Integer typeLength,
        int columnIndex,
        int maxDefinitionLevel,
        int maxRepetitionLevel,
        LogicalType logicalType) {

    /**
     * Constructor for flat schemas (backward compatibility).
     */
    public ColumnSchema(
            String name,
            PhysicalType type,
            RepetitionType repetitionType,
            Integer typeLength,
            int columnIndex,
            LogicalType logicalType) {
        this(name, type, repetitionType, typeLength, columnIndex,
                repetitionType == RepetitionType.REQUIRED ? 0 : 1,
                0,
                logicalType);
    }

    public int getMaxDefinitionLevel() {
        return maxDefinitionLevel;
    }

    public int getMaxRepetitionLevel() {
        return maxRepetitionLevel;
    }

    public boolean isRequired() {
        return repetitionType == RepetitionType.REQUIRED;
    }

    public boolean isOptional() {
        return repetitionType == RepetitionType.OPTIONAL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(repetitionType.name().toLowerCase());
        sb.append(" ");
        sb.append(type.name().toLowerCase());
        if (typeLength != null) {
            sb.append("(").append(typeLength).append(")");
        }
        sb.append(" ");
        sb.append(name);
        if (logicalType != null) {
            sb.append(" (").append(logicalType).append(")");
        }
        sb.append(";");
        return sb.toString();
    }
}
