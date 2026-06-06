/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/// Root schema container representing the complete Parquet schema.
/// Supports both flat schemas and nested structures (structs, lists).
///
/// @see <a href="https://parquet.apache.org/docs/file-format/">File Format</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public class FileSchema {

    private final String name;
    private final List<ColumnSchema> columns;
    private final StringToIntMap columnPathToIndex;
    private final SchemaNode.GroupNode rootNode;

    private FileSchema(String name, List<ColumnSchema> columns, SchemaNode.GroupNode rootNode) {
        this.name = name;
        this.columns = columns;
        this.rootNode = rootNode;

        // Pre-compute field path -> index mapping for O(1) lookup.
        // Uses the dot-separated field path (e.g. "address.zip") as key,
        // which is unambiguous even when multiple nested columns share a leaf name.
        this.columnPathToIndex = new StringToIntMap(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            columnPathToIndex.put(columns.get(i).fieldPath().toString(), i);
        }
    }

    /// Returns the schema name (typically "schema" or "message").
    public String getName() {
        return name;
    }

    /// Returns an unmodifiable list of all leaf columns in schema order.
    public List<ColumnSchema> getColumns() {
        return columns;
    }

    /// Returns the column at the given zero-based index.
    ///
    /// @param index zero-based column index
    public ColumnSchema getColumn(int index) {
        return columns.get(index);
    }

    /// Returns the column with the given name or dot-separated path.
    ///
    /// For flat schemas, the name is the column name (e.g. `"passenger_count"`).
    /// For nested schemas, use the dot-separated field path (e.g. `"address.zip"`)
    /// to avoid ambiguity when multiple nested columns share a leaf name.
    ///
    /// @param name column name or dot-separated field path
    /// @throws IllegalArgumentException if no column with the given name exists
    public ColumnSchema getColumn(String name) {
        int index = columnPathToIndex.get(name);
        if (index < 0) {
            throw new IllegalArgumentException("Column not found: " + name);
        }
        return columns.get(index);
    }

    /// Returns the column with the given field path.
    ///
    /// @param fieldPath path from schema root to leaf column
    /// @throws IllegalArgumentException if no column with the given path exists
    public ColumnSchema getColumn(FieldPath fieldPath) {
        return getColumn(fieldPath.toString());
    }

    /// Returns the total number of leaf columns in this schema.
    public int getColumnCount() {
        return columns.size();
    }

    /// Returns the hierarchical schema tree representation.
    public SchemaNode.GroupNode getRootNode() {
        return rootNode;
    }

    /// Finds a top-level field by name in the schema tree.
    public SchemaNode getField(String name) {
        for (SchemaNode child : rootNode.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    /// Returns true if this schema supports direct columnar access.
    /// For such schemas, enabling direct columnar access without record assembly.
    ///
    /// A schema supports columnar access if all top-level fields are primitives
    /// (no nested structs, lists, or maps) and no columns have repetition.
    public boolean isFlatSchema() {
        // Check that all top-level fields are primitives (no nested structs)
        for (SchemaNode child : rootNode.children()) {
            if (child instanceof SchemaNode.GroupNode) {
                return false;
            }
        }
        // Also check repetition levels
        for (ColumnSchema col : columns) {
            if (col.maxRepetitionLevel() > 0) {
                return false;
            }
        }
        return true;
    }

    /// Reconstruct schema from Thrift SchemaElement list.
    public static FileSchema fromSchemaElements(List<SchemaElement> elements) {
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Schema elements list is empty");
        }

        SchemaElement root = elements.get(0);
        if (root.isPrimitive()) {
            throw new IllegalArgumentException("Root schema element must be a group");
        }

        // Build hierarchical tree and flat column list simultaneously
        List<ColumnSchema> columns = new ArrayList<>();
        int[] columnIndex = { 0 }; // Mutable counter for column indexing

        // Shared read position into the flat element list; start past the root at index 0.
        int[] cursor = { 1 };
        List<SchemaNode> rootChildren = buildChildren(elements, cursor, root.numChildren() != null ? root.numChildren() : 0, 0, 0, List.of(), columns, columnIndex);

        SchemaNode.GroupNode rootNode = new SchemaNode.GroupNode(
                root.name(),
                root.repetitionType() != null ? root.repetitionType() : RepetitionType.REQUIRED,
                root.convertedType(),
                root.logicalType(),
                rootChildren,
                0, // Root has def level 0
                0 // Root has rep level 0
        );

        return new FileSchema(root.name(), columns, rootNode);
    }

    /// Build children nodes from schema elements.
    private static List<SchemaNode> buildChildren(
                                                  List<SchemaElement> elements,
                                                  int[] cursor,
                                                  int numChildren,
                                                  int parentDefLevel,
                                                  int parentRepLevel,
                                                  List<String> parentPath,
                                                  List<ColumnSchema> columns,
                                                  int[] columnIndex) {

        List<SchemaNode> children = new ArrayList<>();

        for (int i = 0; i < numChildren; i++) {
            SchemaElement element = elements.get(cursor[0]);
            RepetitionType repType = element.repetitionType() != null ? element.repetitionType() : RepetitionType.OPTIONAL;

            // Calculate levels for this node
            int defLevel = parentDefLevel + (repType != RepetitionType.REQUIRED ? 1 : 0);
            int repLevel = parentRepLevel + (repType == RepetitionType.REPEATED ? 1 : 0);

            // Build path for this node
            List<String> currentPath = new ArrayList<>(parentPath.size() + 1);
            currentPath.addAll(parentPath);
            currentPath.add(element.name());

            if (element.isPrimitive()) {
                // Primitive node - represents an actual column
                int colIdx = columnIndex[0]++;
                LogicalType effectiveLogicalType = effectiveLogicalType(element);
                columns.add(new ColumnSchema(
                        new FieldPath(List.copyOf(currentPath)),
                        element.type(),
                        repType,
                        element.typeLength(),
                        colIdx,
                        defLevel,
                        repLevel,
                        effectiveLogicalType));

                children.add(new SchemaNode.PrimitiveNode(
                        element.name(),
                        element.type(),
                        repType,
                        effectiveLogicalType,
                        colIdx,
                        defLevel,
                        repLevel));

                cursor[0]++;
            }
            else {
                // Group node - recurse into children
                int groupNumChildren = element.numChildren() != null ? element.numChildren() : 0;
                cursor[0]++; // Consume the group header; cursor now points at its first child

                List<SchemaNode> groupChildren = buildChildren(
                        elements,
                        cursor,
                        groupNumChildren,
                        defLevel,
                        repLevel,
                        currentPath,
                        columns,
                        columnIndex);

                SchemaNode.GroupNode groupNode = new SchemaNode.GroupNode(
                        element.name(),
                        repType,
                        element.convertedType(),
                        effectiveGroupLogicalType(element, groupChildren),
                        groupChildren,
                        defLevel,
                        repLevel);
                if (groupNode.isVariant()) {
                    validateVariantGroup(groupNode);
                }
                children.add(groupNode);
            }
        }

        return children;
    }

    /// Resolve the effective logical type of a primitive element, falling back
    /// to the legacy `converted_type` annotation when the modern logical-type
    /// union is absent. Older writers (parquet-mr, Spark, Hive) only set
    /// `converted_type`, which would otherwise leave the column read as a bare
    /// physical column and decoded wrong or not at all (e.g. a `DECIMAL` read as
    /// an unscaled integer, a `DATE` as a raw `INT32`).
    ///
    /// When both annotations are present the modern `logicalType()` wins. Only
    /// primitive-level annotations are mapped here; the group-level `LIST`, `MAP`,
    /// and `MAP_KEY_VALUE` are consulted directly on [SchemaNode.GroupNode].
    private static LogicalType effectiveLogicalType(SchemaElement element) {
        if (element.logicalType() != null) {
            return element.logicalType();
        }
        ConvertedType converted = element.convertedType();
        if (converted == null) {
            return null;
        }
        return switch (converted) {
            case UTF8 -> new LogicalType.StringType();
            case ENUM -> new LogicalType.EnumType();
            case JSON -> new LogicalType.JsonType();
            case BSON -> new LogicalType.BsonType();
            case INTERVAL -> new LogicalType.IntervalType();
            case DATE -> new LogicalType.DateType();
            case DECIMAL -> decimalFromElement(element);
            // The parquet-format backward-compatibility rule maps the legacy
            // TIME_*/TIMESTAMP_* converted types to isAdjustedToUTC=true; these
            // annotations always denoted UTC-normalized values.
            case TIME_MILLIS -> new LogicalType.TimeType(true, LogicalType.TimeUnit.MILLIS);
            case TIME_MICROS -> new LogicalType.TimeType(true, LogicalType.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS -> new LogicalType.TimestampType(true, LogicalType.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS -> new LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS);
            case INT_8 -> new LogicalType.IntType(8, true);
            case INT_16 -> new LogicalType.IntType(16, true);
            case INT_32 -> new LogicalType.IntType(32, true);
            case INT_64 -> new LogicalType.IntType(64, true);
            case UINT_8 -> new LogicalType.IntType(8, false);
            case UINT_16 -> new LogicalType.IntType(16, false);
            case UINT_32 -> new LogicalType.IntType(32, false);
            case UINT_64 -> new LogicalType.IntType(64, false);
            // Group-level annotations are handled on GroupNode, not here.
            case LIST, MAP, MAP_KEY_VALUE -> null;
        };
    }

    /// Resolve the effective logical type of a group element. The modern
    /// `logicalType()` wins when present. Otherwise, recognise the legacy MAP
    /// encoding in which only the inner repeated `key_value` group carries the
    /// `MAP_KEY_VALUE` converted type, with no `MAP` annotation on the outer
    /// group. Older parquet-mr / Hive / Impala writers emit this form; surfacing
    /// it as a [LogicalType.MapType] lets the rest of the reader treat it
    /// identically to a MAP-annotated group.
    private static LogicalType effectiveGroupLogicalType(SchemaElement element, List<SchemaNode> children) {
        if (element.logicalType() != null) {
            return element.logicalType();
        }
        if (hasMapKeyValueChild(children)) {
            return new LogicalType.MapType();
        }
        return null;
    }

    /// Returns true if `children` is a single REPEATED group annotated with the
    /// legacy `MAP_KEY_VALUE` converted type, the hallmark of the legacy MAP
    /// encoding.
    private static boolean hasMapKeyValueChild(List<SchemaNode> children) {
        return children.size() == 1
                && children.get(0) instanceof SchemaNode.GroupNode child
                && child.repetitionType() == RepetitionType.REPEATED
                && child.convertedType() == ConvertedType.MAP_KEY_VALUE;
    }

    /// Build a [LogicalType.DecimalType] from a legacy `DECIMAL` converted-type
    /// element, reading `scale`/`precision` off the schema element. A missing
    /// scale defaults to `0`; a missing precision is a malformed schema.
    private static LogicalType.DecimalType decimalFromElement(SchemaElement element) {
        if (element.precision() == null) {
            throw new IllegalArgumentException(
                    "DECIMAL converted type requires a precision: " + element.name());
        }
        int scale = element.scale() != null ? element.scale() : 0;
        return new LogicalType.DecimalType(scale, element.precision());
    }

    /// Validate a Variant-annotated group's shape: required `metadata` binary
    /// child, required `value` binary child, and at most one optional `typed_value`
    /// sibling (reassembled in Phase 2; permitted but not yet consulted).
    private static void validateVariantGroup(SchemaNode.GroupNode group) {
        List<SchemaNode> kids = group.children();
        if (kids.size() < 2 || kids.size() > 3) {
            throw new IllegalArgumentException(
                    "Variant group '" + group.name() + "' must have 2 or 3 children (metadata, value[, typed_value]), found: " + kids.size());
        }
        requireVariantBinaryChild(group, kids.get(0), "metadata");
        requireVariantBinaryChild(group, kids.get(1), "value");
        if (kids.size() == 3 && !"typed_value".equals(kids.get(2).name())) {
            throw new IllegalArgumentException(
                    "Variant group '" + group.name() + "' third child must be named 'typed_value', found: " + kids.get(2).name());
        }
    }

    private static void requireVariantBinaryChild(SchemaNode.GroupNode group, SchemaNode child, String expectedName) {
        if (!expectedName.equals(child.name())) {
            throw new IllegalArgumentException(
                    "Variant group '" + group.name() + "' expected child '" + expectedName + "', found: " + child.name());
        }
        if (!(child instanceof SchemaNode.PrimitiveNode prim) || prim.type() != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "Variant group '" + group.name() + "' child '" + expectedName + "' must be a BYTE_ARRAY primitive");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("message ").append(name).append(" {\n");
        for (SchemaNode child : rootNode.children()) {
            appendNode(sb, child, 1);
        }
        sb.append("}");
        return sb.toString();
    }

    private void appendNode(StringBuilder sb, SchemaNode node, int indent) {
        String prefix = "  ".repeat(indent);
        switch (node) {
            case SchemaNode.GroupNode group -> {
                sb.append(prefix);
                sb.append(group.repetitionType().name().toLowerCase(Locale.ROOT));
                sb.append(" group ").append(group.name());
                if (group.logicalType() != null) {
                    sb.append(" (").append(group.logicalType()).append(")");
                }
                else if (group.convertedType() != null) {
                    sb.append(" (").append(group.convertedType()).append(")");
                }
                sb.append(" {\n");
                for (SchemaNode child : group.children()) {
                    appendNode(sb, child, indent + 1);
                }
                sb.append(prefix).append("}\n");
            }
            case SchemaNode.PrimitiveNode prim -> {
                sb.append(prefix);
                sb.append(prim.repetitionType().name().toLowerCase(Locale.ROOT));
                sb.append(" ").append(prim.type().name().toLowerCase(Locale.ROOT));
                sb.append(" ").append(prim.name());
                if (prim.logicalType() != null) {
                    sb.append(" (").append(prim.logicalType()).append(")");
                }
                sb.append(";\n");
            }
        }
    }
}
