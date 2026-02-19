/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;

/**
 * Tree-based representation of Parquet schema for nested data support.
 * Each node represents either a primitive column or a group (struct/list/map).
 */
public sealed

interface SchemaNode {

    String name();

    RepetitionType repetitionType();

    int maxDefinitionLevel();

    int maxRepetitionLevel();

    /**
     * Primitive leaf node representing an actual data column.
     */
    record PrimitiveNode(
            String name,
            PhysicalType type,
            RepetitionType repetitionType,
            LogicalType logicalType,
            int columnIndex,
            int maxDefinitionLevel,
            int maxRepetitionLevel) implements SchemaNode {
    }

    /**
     * Group node representing a struct, list, or map.
     */
    record GroupNode(
            String name,
            RepetitionType repetitionType,
            ConvertedType convertedType,
            List<SchemaNode> children,
            int maxDefinitionLevel,
            int maxRepetitionLevel) implements SchemaNode {

    /**
         * Returns true if this is a LIST group.
         */
        public boolean isList() {
            return convertedType == ConvertedType.LIST;
        }

    /**
         * Returns true if this is a MAP group.
         */
        public boolean isMap() {
            return convertedType == ConvertedType.MAP;
        }

    /**
         * Returns true if this is a plain struct (no converted type).
         */
        public boolean isStruct() {
            return convertedType == null;
        }

    /**
         * For LIST groups, returns the element node (skipping intermediate 'list' group).
         * Returns null if not a list or improperly structured.
         */
        public SchemaNode getListElement() {
            if (!isList() || children.isEmpty()) {
                return null;
            }
            // Standard 3-level list encoding: LIST -> list (repeated) -> element
            SchemaNode inner = children.get(0);
            if (inner instanceof GroupNode innerGroup && innerGroup.repetitionType() == RepetitionType.REPEATED) {
                if (!innerGroup.children().isEmpty()) {
                    return innerGroup.children().get(0);
                }
            }
            // 2-level list encoding: LIST -> repeated element (less common)
            if (inner.repetitionType() == RepetitionType.REPEATED) {
                return inner;
            }
            return null;
        }
}}
