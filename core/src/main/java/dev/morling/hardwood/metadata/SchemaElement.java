/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

/**
 * Schema element in Parquet file metadata.
 */
public record SchemaElement(
        String name,
        PhysicalType type,
        Integer typeLength,
        RepetitionType repetitionType,
        Integer numChildren,
        ConvertedType convertedType,
        Integer scale,
        Integer precision,
        Integer fieldId,
        LogicalType logicalType) {

    public boolean isGroup() {
        return type == null;
    }

    public boolean isPrimitive() {
        return type != null;
    }
}
