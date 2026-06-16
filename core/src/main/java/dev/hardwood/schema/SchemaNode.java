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

/// Tree-based representation of Parquet schema for nested data support.
/// Each node represents either a primitive column or a group (struct/list/map).
///
/// @see <a href="https://parquet.apache.org/docs/file-format/nestedencoding/">File Format – Nested Encoding</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public sealed interface SchemaNode {

    /// Returns the field name.
    String name();

    /// Returns the repetition type (required, optional, or repeated).
    RepetitionType repetitionType();

    /// Returns the maximum definition level, computed from the schema hierarchy.
    int maxDefinitionLevel();

    /// Returns the maximum repetition level, computed from the schema hierarchy.
    int maxRepetitionLevel();

    /// Primitive leaf node representing an actual data column.
    ///
    /// @param name field name
    /// @param type physical (storage) type
    /// @param repetitionType whether the field is required, optional, or repeated
    /// @param logicalType logical type annotation, or `null` if absent
    /// @param columnIndex zero-based index among all leaf columns
    /// @param maxDefinitionLevel maximum definition level
    /// @param maxRepetitionLevel maximum repetition level
    record PrimitiveNode(
            String name,
            PhysicalType type,
            RepetitionType repetitionType,
            LogicalType logicalType,
            int columnIndex,
            int maxDefinitionLevel,
            int maxRepetitionLevel) implements SchemaNode {
    }

    /// Group node representing a struct, list, map, or variant.
    ///
    /// @param name field name
    /// @param repetitionType whether the group is required, optional, or repeated
    /// @param convertedType legacy annotation indicating list, map, or map-key-value semantics, or `null` for plain structs
    /// @param logicalType modern logical-type annotation applied to the group (e.g. [LogicalType.VariantType]), or `null` if unannotated
    /// @param children child nodes of this group
    /// @param maxDefinitionLevel maximum definition level
    /// @param maxRepetitionLevel maximum repetition level
    record GroupNode(
            String name,
            RepetitionType repetitionType,
            ConvertedType convertedType,
            LogicalType logicalType,
            List<SchemaNode> children,
            int maxDefinitionLevel,
            int maxRepetitionLevel) implements SchemaNode {

    /// Returns true if this is a LIST group.
        public boolean isList() {
            return convertedType == ConvertedType.LIST
                    || logicalType instanceof LogicalType.ListType;
        }

    /// Returns true if this is a MAP group.
        public boolean isMap() {
            return convertedType == ConvertedType.MAP
                    || logicalType instanceof LogicalType.MapType;
        }

    /// Returns true if this is a plain struct (no converted type and no modern logical-type annotation).
        public boolean isStruct() {
            return convertedType == null && logicalType == null;
        }

    /// Returns true if this group carries the [LogicalType.VariantType] annotation.
        public boolean isVariant() {
            return logicalType instanceof LogicalType.VariantType;
        }

    /// For LIST groups, returns the list's element node — the logical element,
    /// independent of whether the list uses 2-level or 3-level encoding. Returns
    /// `null` if not a list or improperly structured.
    ///
    /// Applies the Parquet backward-compatibility rules for legacy 2-level
    /// encodings as defined in the format spec; see
    /// [Backward-compatibility rules](https://parquet.apache.org/docs/file-format/types/logicaltypes/#backward-compatibility-rules):
    ///
    /// 1. If the repeated field is not a group, the repeated field's type is the element type.
    /// 2. If the repeated field is a group with multiple fields, the repeated group is the element.
    /// 3. If the repeated field is a group with one field and that field is itself `repeated`, the
    ///    repeated group is the element — it is a genuine element struct, not a synthetic
    ///    single-field wrapper (legacy 2-level encoding of a list whose element is itself a list).
    /// 4. If the repeated field is a group with one field and is named either `array` or uses
    ///    the LIST-annotated group's name with `_tuple` appended, the repeated group is the element.
    /// 5. Otherwise, the repeated field is a wrapper and its single child is the element — the
    ///    standard `list`/`element` structure and any other single-field 3-level shape not matched
    ///    by rules 1–4 (the child need not be named `element`).
    ///
    /// The returned node is an existing schema node, so its repetition is authentic: a 2-level
    /// list's element is itself `REPEATED` (the repeated field is both the element and the list's
    /// repetition carrier), while a 3-level list's element is the non-repeated child of the
    /// intermediate `list` group. A list's nesting depth is the count of `repeated` nodes on the
    /// path (the leaf's max repetition level), not the count of `LIST` annotations. Since the
    /// element may itself be a list, walk to the leaf by recursing while it is a list — uniform
    /// across both encodings:
    ///
    /// ```java
    /// SchemaNode node = listGroup;
    /// while (node instanceof GroupNode g && g.isList()) {
    ///     node = g.getListElement();
    /// }
    /// ```
        public SchemaNode getListElement() {
            if (!isList() || children.isEmpty()) {
                return null;
            }
            SchemaNode inner = children.get(0);
            if (inner.repetitionType() != RepetitionType.REPEATED) {
                return null;
            }
            // Rule 1: repeated primitive — the repeated field is the element.
            if (!(inner instanceof GroupNode innerGroup)) {
                return inner;
            }
            // Rule 2: repeated group with multiple fields — the repeated group is the element.
            if (innerGroup.children().size() != 1) {
                return innerGroup;
            }
            // Rule 3: repeated group whose single field is itself repeated — the repeated group
            // is the element. A synthetic 3-level wrapper never has a repeated child, so a
            // repeated child marks this group as a real element (a list whose element is a list).
            if (innerGroup.children().get(0).repetitionType() == RepetitionType.REPEATED) {
                return innerGroup;
            }
            // Rule 4: repeated group with one field named 'array' or '<listName>_tuple' —
            // the repeated group is the element (legacy 2-level encoding).
            String innerName = innerGroup.name();
            if ("array".equals(innerName) || (name() + "_tuple").equals(innerName)) {
                return innerGroup;
            }
            // Rule 5: wrapper group (standard `list`/`element` or any other single-field 3-level
            // shape) — descend to the repeated group's single child.
            return innerGroup.children().get(0);
        }

    /// For MAP groups, returns the key node from the standard encoding
    /// (`map.key_value.key`).
    ///
    /// Returns `null` if this group is not a MAP, or if the structure does not
    /// match the standard encoding (a single REPEATED `key_value` child group).
    /// Symmetric with [#getListElement()] in returning `null` rather than
    /// throwing — callers decide whether a malformed schema is fatal at their
    /// layer.
        public SchemaNode getMapKey() {
            return mapChild(0);
        }

    /// For MAP groups, returns the value node from the standard encoding
    /// (`map.key_value.value`), or `null` if the `key_value` group has no value
    /// field. The Parquet spec permits a key-only `key_value` group, which
    /// represents a set of keys or a map with all-null values. See
    /// [#getMapKey()] for the other null cases.
        public SchemaNode getMapValue() {
            return mapChild(1);
        }

        private SchemaNode mapChild(int index) {
            if (!isMap() || children.isEmpty()) {
                return null;
            }
            SchemaNode keyValue = children.get(0);
            if (!(keyValue instanceof GroupNode keyValueGroup)
                    || keyValueGroup.repetitionType() != RepetitionType.REPEATED) {
                return null;
            }
            List<SchemaNode> kvChildren = keyValueGroup.children();
            if (index >= kvChildren.size()) {
                return null;
            }
            return kvChildren.get(index);
        }
}}
