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
import java.util.function.Consumer;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.schema.LogicalTypeAnnotations;
import dev.hardwood.internal.schema.LogicalTypeValidator;
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

    private static final System.Logger LOG =
            System.getLogger(FileSchema.class.getName());

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

    /// Creates a builder for constructing a flat schema programmatically, for use
    /// with the writer.
    ///
    /// @param name the schema (message) name, conventionally `"schema"`
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /// Flattens this schema back into the depth-first [SchemaElement] list written
    /// to the file footer, the inverse of [#fromSchemaElements]: the root element
    /// followed by each node in pre-order, groups carrying their child count.
    public List<SchemaElement> toSchemaElements() {
        List<SchemaElement> elements = new ArrayList<>(columns.size() + 1);
        elements.add(SchemaElement.group(name, rootNode.repetitionType(), rootNode.children().size()));
        appendElements(rootNode.children(), elements);
        return elements;
    }

    private void appendElements(List<SchemaNode> nodes, List<SchemaElement> out) {
        for (SchemaNode node : nodes) {
            switch (node) {
                case SchemaNode.PrimitiveNode leaf -> {
                    LogicalTypeAnnotations annotations = LogicalTypeAnnotations.of(leaf.logicalType());
                    out.add(new SchemaElement(leaf.name(), leaf.type(),
                            columns.get(leaf.columnIndex()).typeLength(), leaf.repetitionType(), null,
                            annotations.convertedType(), annotations.scale(), annotations.precision(),
                            null, annotations.union()));
                }
                case SchemaNode.GroupNode group -> {
                    LogicalTypeAnnotations annotations =
                            LogicalTypeAnnotations.ofGroup(group.convertedType(), group.logicalType());
                    out.add(new SchemaElement(group.name(), null, null, group.repetitionType(),
                            group.children().size(), annotations.convertedType(), annotations.scale(),
                            annotations.precision(), null, annotations.union()));
                    appendElements(group.children(), out);
                }
            }
        }
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
        // One line per file rather than one per column: a file written against a newer
        // format version can carry many, and they say the same thing about all of them.
        List<String> dropped = new ArrayList<>();
        List<SchemaNode> rootChildren = buildChildren(elements, cursor, root.numChildren() != null ? root.numChildren() : 0, 0, 0, List.of(), columns, columnIndex, dropped);
        if (!dropped.isEmpty()) {
            LOG.log(System.Logger.Level.WARNING,
                    "Ignoring {0} logical type annotation(s) the column''s physical type"
                    + " cannot carry; those columns are read as their physical type: {1}",
                    dropped.size(), String.join("; ", dropped));
        }

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
                                                  int[] columnIndex,
                                                  List<String> dropped) {

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
                LogicalType effectiveLogicalType = readableLogicalType(element, currentPath, dropped);
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
                        columnIndex,
                        dropped);

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

    /// The column's annotation, or `null` where its physical type cannot carry it.
    ///
    /// `FLOAT16` is defined as a two-byte payload, so a column annotated `FLOAT16` that
    /// declares twelve bytes is invalid, and no reading of it produces the value the
    /// annotation promises. The format says to read past the annotation rather than refuse
    /// the file: see the "Unsupported Logical Types" section of `LogicalTypes.md`, adopted in
    /// parquet-format PR 606, which has readers "ignore both the logical type annotation and
    /// column order for that column. Only the physical type information should be used to
    /// process the column's data."
    ///
    /// So the column is reported and read as though the footer had not annotated it. Every
    /// consequence follows from the annotation's absence rather than from a check: the
    /// physical accessors work, `getValue` yields the physical value, a logical accessor
    /// fails exactly as it would on any unannotated column of that type, and the column's
    /// statistics are compared under its physical type's ordering.
    ///
    /// The sibling case — an annotation this version does not recognize at all — is dropped
    /// where it is parsed, in `LogicalTypeReader`. Both end here as an unannotated column;
    /// they differ only in what the warning can tell the reader, because one is a file from
    /// a newer writer and the other is a file from a broken one.
    ///
    /// Column order needs no separate handling: the only thing it decides is whether a float
    /// predicate compares under IEEE 754 total order, and a column that has lost its
    /// `FLOAT16` annotation is rejected by `FilterPredicateResolver` before that flag is
    /// consulted.
    private static LogicalType readableLogicalType(SchemaElement element, List<String> path,
                                                   List<String> dropped) {
        LogicalType annotation = effectiveLogicalType(element);
        if (annotation == null) {
            return null;
        }
        String fault = LogicalTypeConverter.conversionFault(
                element.type(), element.typeLength(), annotation);
        if (fault == null) {
            return annotation;
        }
        dropped.add(String.join(".", path) + " (" + fault + ")");
        return null;
    }

    /// Guards the read-side leniency against a schema the caller declared.
    ///
    /// [#readableLogicalType] drops an annotation the column's physical type cannot carry,
    /// which is right for a file already on disk and wrong for a schema being declared here:
    /// the caller would get a file written without the annotation they asked for, and only a
    /// warning to say so. [LogicalTypeValidator] already refuses every such pairing when the
    /// leaf is declared, so reaching this means the writer's rule and the reader's have
    /// drifted apart rather than that the caller did anything wrong.
    private static void requireNoAnnotationDropped(List<SchemaElement> elements) {
        for (SchemaElement element : elements) {
            if (!element.isPrimitive()) {
                continue;
            }
            LogicalType annotation = effectiveLogicalType(element);
            if (annotation == null) {
                continue;
            }
            String fault = LogicalTypeConverter.conversionFault(
                    element.type(), element.typeLength(), annotation);
            if (fault != null) {
                throw new IllegalStateException("Column '" + element.name() + "': " + fault
                        + ". A declared schema must not carry an annotation the reader drops.");
            }
        }
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
    ///
    /// @see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps">Parquet LogicalTypes – Maps backward-compatibility rules</a>
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

    /// Builder for constructing a [FileSchema] programmatically.
    ///
    /// Fields are added as top-level primitive leaves ([#addColumn]), nested `struct`
    /// groups ([#struct]), `LIST` groups ([#list]), or `MAP` groups ([#map]). The `LIST`
    /// and `MAP` groups emit the canonical 3-level and 2-level physical layouts.
    public static final class Builder {

        private final String name;
        private final StructBuilder content = new StructBuilder();

        private Builder(String name) {
            this.name = name;
        }

        /// Append a primitive column.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`
        public Builder addColumn(String columnName, PhysicalType type, RepetitionType repetition) {
            content.addColumn(columnName, type, repetition);
            return this;
        }

        /// Append a primitive column, giving the fixed byte length of a `FIXED_LEN_BYTE_ARRAY`
        /// column.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param typeLength the fixed byte length (required and positive for
        ///        `FIXED_LEN_BYTE_ARRAY`, rejected for any other type)
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or `typeLength` does
        ///         not match the type
        public Builder addColumn(String columnName, PhysicalType type, RepetitionType repetition, int typeLength) {
            content.addColumn(columnName, type, repetition, typeLength);
            return this;
        }

        /// Append a primitive column carrying a logical type annotation.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param logicalType the annotation, which must be legal for `type`
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or the annotation does
        ///         not apply to the physical type
        public Builder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                 LogicalType logicalType) {
            content.addColumn(columnName, type, repetition, logicalType);
            return this;
        }

        /// Append a primitive column carrying a logical type annotation, giving the fixed byte
        /// length of a `FIXED_LEN_BYTE_ARRAY` column.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param typeLength the fixed byte length (required and positive for
        ///        `FIXED_LEN_BYTE_ARRAY`, rejected for any other type)
        /// @param logicalType the annotation, which must be legal for `type` and `typeLength`
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, `typeLength` does not
        ///         match the type, or the annotation does not apply to the physical type
        public Builder addColumn(String columnName, PhysicalType type, RepetitionType repetition, int typeLength,
                                 LogicalType logicalType) {
            content.addColumn(columnName, type, repetition, typeLength, logicalType);
            return this;
        }

        /// Append a `struct` group whose fields are declared by `filler`.
        ///
        /// @param structName the group name
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param filler declares the group's fields
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or the group has no fields
        public Builder struct(String structName, RepetitionType repetition, Consumer<StructBuilder> filler) {
            content.struct(structName, repetition, filler);
            return this;
        }

        /// Append a `LIST` group whose element is declared by `element`.
        ///
        /// @param listName the list group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the list itself may be null)
        /// @param element declares the list's element
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or no element is declared
        public Builder list(String listName, RepetitionType repetition, Consumer<ElementBuilder> element) {
            content.list(listName, repetition, element);
            return this;
        }

        /// Append a `MAP` group whose value is declared by `value`. The key is a required
        /// primitive of `keyType` per the canonical layout.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param keyType the physical type of the required `key`
        /// @param value declares the map's value, like a list element
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or no value is declared
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                           Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, value);
            return this;
        }

        /// Append a `MAP` group whose key carries a logical type annotation — a `STRING` key
        /// being the common case.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param keyType the physical type of the required `key`
        /// @param keyLogicalType the key's annotation, which must be legal for `keyType`
        /// @param value declares the map's value, like a list element
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         or the annotation does not apply to the key's physical type
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                           LogicalType keyLogicalType, Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, keyLogicalType, value);
            return this;
        }

        /// Append a `MAP` group whose key carries a logical type annotation, giving the fixed
        /// byte length of a `FIXED_LEN_BYTE_ARRAY` key.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param keyType the physical type of the required `key`
        /// @param keyTypeLength the key's fixed byte length (required and positive for
        ///        `FIXED_LEN_BYTE_ARRAY`, rejected for any other type)
        /// @param keyLogicalType the key's annotation, which must be legal for `keyType` and
        ///        `keyTypeLength`
        /// @param value declares the map's value, like a list element
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         `keyTypeLength` does not match the key's type, or the annotation does not
        ///         apply to it
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType, int keyTypeLength,
                           LogicalType keyLogicalType, Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, keyTypeLength, keyLogicalType, value);
            return this;
        }

        /// Build the schema.
        ///
        /// @throws IllegalArgumentException if no fields were added
        public FileSchema build() {
            if (content.children.isEmpty()) {
                throw new IllegalArgumentException("Schema must have at least one column");
            }
            List<SchemaElement> elements = new ArrayList<>();
            elements.add(SchemaElement.group(name, RepetitionType.REQUIRED, content.children.size()));
            flatten(content.children, elements);
            requireNoAnnotationDropped(elements);
            return fromSchemaElements(elements);
        }
    }

    /// Declares the fields of a `struct` group. Nested structs compose by calling
    /// [#struct] again inside the filler.
    public static final class StructBuilder {

        private final List<BuilderNode> children = new ArrayList<>();

        private StructBuilder() {
        }

        /// Append a primitive field.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, or the type is
        ///         `FIXED_LEN_BYTE_ARRAY` (use the type-length overload)
        public StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition) {
            return addColumn(columnName, type, repetition, null, null);
        }

        /// Append a primitive field, giving the fixed byte length of a `FIXED_LEN_BYTE_ARRAY`
        /// column.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, or `typeLength` does
        ///         not match the type (required and positive for `FIXED_LEN_BYTE_ARRAY`, absent
        ///         otherwise)
        public StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                       int typeLength) {
            return addColumn(columnName, type, repetition, (Integer) typeLength, null);
        }

        /// Append a primitive field carrying a logical type annotation.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, or the annotation does
        ///         not apply to the physical type
        public StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                       LogicalType logicalType) {
            return addColumn(columnName, type, repetition, null, logicalType);
        }

        /// Append a primitive field carrying a logical type annotation, giving the fixed byte
        /// length of a `FIXED_LEN_BYTE_ARRAY` column.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, `typeLength` does not
        ///         match the type, or the annotation does not apply to the physical type
        public StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                       int typeLength, LogicalType logicalType) {
            return addColumn(columnName, type, repetition, (Integer) typeLength, logicalType);
        }

        private StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                        Integer typeLength, LogicalType logicalType) {
            if (repetition == RepetitionType.REPEATED) {
                throw new IllegalArgumentException(
                        "Repeated columns are not yet supported by the writer: " + columnName);
            }
            children.add(leaf(columnName, type, repetition, typeLength, logicalType));
            return this;
        }

        /// Append a nested `struct` field.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or the group has no fields
        public StructBuilder struct(String structName, RepetitionType repetition, Consumer<StructBuilder> filler) {
            if (repetition == RepetitionType.REPEATED) {
                throw new IllegalArgumentException(
                        "Repeated groups are not yet supported by the writer: " + structName);
            }
            StructBuilder nested = new StructBuilder();
            filler.accept(nested);
            if (nested.children.isEmpty()) {
                throw new IllegalArgumentException("Struct must have at least one field: " + structName);
            }
            children.add(new BuilderStruct(structName, repetition, nested.children));
            return this;
        }

        /// Append a nested `LIST` field.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or no element is declared
        public StructBuilder list(String listName, RepetitionType repetition, Consumer<ElementBuilder> element) {
            if (repetition == RepetitionType.REPEATED) {
                throw new IllegalArgumentException(
                        "Repeated groups are not yet supported by the writer: " + listName);
            }
            ElementBuilder builder = new ElementBuilder();
            element.accept(builder);
            children.add(new BuilderList(listName, repetition, builder.require(listName)));
            return this;
        }

        /// Append a nested `MAP` field.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or no value is declared
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 Consumer<ElementBuilder> value) {
            return map(mapName, repetition, keyType, null, null, value);
        }

        /// Append a nested `MAP` field whose key carries a logical type annotation.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         or the annotation does not apply to the key's physical type
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 LogicalType keyLogicalType, Consumer<ElementBuilder> value) {
            return map(mapName, repetition, keyType, null, keyLogicalType, value);
        }

        /// Append a nested `MAP` field whose key carries a logical type annotation, giving the
        /// fixed byte length of a `FIXED_LEN_BYTE_ARRAY` key.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         `keyTypeLength` does not match the key's type, or the annotation does not
        ///         apply to it
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 int keyTypeLength, LogicalType keyLogicalType,
                                 Consumer<ElementBuilder> value) {
            return map(mapName, repetition, keyType, (Integer) keyTypeLength, keyLogicalType, value);
        }

        private StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                  Integer keyTypeLength, LogicalType keyLogicalType,
                                  Consumer<ElementBuilder> value) {
            if (repetition == RepetitionType.REPEATED) {
                throw new IllegalArgumentException(
                        "Repeated groups are not yet supported by the writer: " + mapName);
            }
            children.add(buildMap(mapName, repetition, keyType, keyTypeLength, keyLogicalType, value));
            return this;
        }
    }

    /// Declares the element of a `LIST`, or the value of a `MAP`: a primitive, a nested
    /// `struct`, a nested `LIST`, or a nested `MAP`. The declared node is named `element`
    /// inside a list and `value` inside a map.
    public static final class ElementBuilder {

        private final String childName;
        private BuilderNode element;

        private ElementBuilder() {
            this("element");
        }

        private ElementBuilder(String childName) {
            this.childName = childName;
        }

        /// Declare a primitive element.
        ///
        /// @throws IllegalArgumentException if the type is `FIXED_LEN_BYTE_ARRAY` (use the
        ///         type-length overload)
        public void primitive(PhysicalType type, RepetitionType repetition) {
            set(leaf(childName, type, repetition, null, null));
        }

        /// Declare a primitive element, giving the fixed byte length of a `FIXED_LEN_BYTE_ARRAY`
        /// element.
        ///
        /// @throws IllegalArgumentException if `typeLength` does not match the type
        public void primitive(PhysicalType type, RepetitionType repetition, int typeLength) {
            set(leaf(childName, type, repetition, (Integer) typeLength, null));
        }

        /// Declare a primitive element carrying a logical type annotation.
        ///
        /// @throws IllegalArgumentException if the annotation does not apply to the physical type
        public void primitive(PhysicalType type, RepetitionType repetition, LogicalType logicalType) {
            set(leaf(childName, type, repetition, null, logicalType));
        }

        /// Declare a primitive element carrying a logical type annotation, giving the fixed byte
        /// length of a `FIXED_LEN_BYTE_ARRAY` element.
        ///
        /// @throws IllegalArgumentException if `typeLength` does not match the type or the
        ///         annotation does not apply to it
        public void primitive(PhysicalType type, RepetitionType repetition, int typeLength,
                              LogicalType logicalType) {
            set(leaf(childName, type, repetition, (Integer) typeLength, logicalType));
        }

        /// Declare a `struct` element.
        ///
        /// @throws IllegalArgumentException if the struct has no fields
        public void struct(RepetitionType repetition, Consumer<StructBuilder> filler) {
            StructBuilder nested = new StructBuilder();
            filler.accept(nested);
            if (nested.children.isEmpty()) {
                throw new IllegalArgumentException(childName + " struct must have at least one field");
            }
            set(new BuilderStruct(childName, repetition, nested.children));
        }

        /// Declare a nested `LIST` element.
        public void list(RepetitionType repetition, Consumer<ElementBuilder> element) {
            ElementBuilder inner = new ElementBuilder();
            element.accept(inner);
            set(new BuilderList(childName, repetition, inner.require(childName)));
        }

        /// Declare a nested `MAP` element.
        ///
        /// @throws IllegalArgumentException if no value is declared
        public void map(RepetitionType repetition, PhysicalType keyType, Consumer<ElementBuilder> value) {
            set(buildMap(childName, repetition, keyType, null, null, value));
        }

        /// Declare a nested `MAP` element whose key carries a logical type annotation.
        ///
        /// @throws IllegalArgumentException if no value is declared or the annotation does not
        ///         apply to the key's physical type
        public void map(RepetitionType repetition, PhysicalType keyType, LogicalType keyLogicalType,
                        Consumer<ElementBuilder> value) {
            set(buildMap(childName, repetition, keyType, null, keyLogicalType, value));
        }

        /// Declare a nested `MAP` element whose key carries a logical type annotation, giving the
        /// fixed byte length of a `FIXED_LEN_BYTE_ARRAY` key.
        ///
        /// @throws IllegalArgumentException if no value is declared, `keyTypeLength` does not
        ///         match the key's type, or the annotation does not apply to it
        public void map(RepetitionType repetition, PhysicalType keyType, int keyTypeLength,
                        LogicalType keyLogicalType, Consumer<ElementBuilder> value) {
            set(buildMap(childName, repetition, keyType, (Integer) keyTypeLength, keyLogicalType, value));
        }

        private void set(BuilderNode node) {
            if (element != null) {
                throw new IllegalArgumentException("The " + childName + " is already declared");
            }
            element = node;
        }

        private BuilderNode require(String name) {
            if (element == null) {
                throw new IllegalArgumentException("Must declare the " + childName + " of " + name);
            }
            return element;
        }
    }

    /// Builds a `MAP` node named `name` with a required `key` primitive of `keyType` and a
    /// `value` declared through the shared [ElementBuilder]. Shared by every `map` verb.
    ///
    /// The key goes through [#leaf] like any other primitive, so its type length and annotation
    /// are validated where the map is declared.
    private static BuilderMap buildMap(String name, RepetitionType repetition, PhysicalType keyType,
                                       Integer keyTypeLength, LogicalType keyLogicalType,
                                       Consumer<ElementBuilder> value) {
        ElementBuilder builder = new ElementBuilder("value");
        value.accept(builder);
        BuilderLeaf key = leaf("key", keyType, RepetitionType.REQUIRED, keyTypeLength, keyLogicalType);
        return new BuilderMap(name, repetition, key, builder.require(name));
    }

    private sealed interface BuilderNode {}

    private record BuilderLeaf(String name, PhysicalType type, RepetitionType repetition, Integer typeLength,
                               LogicalType logicalType) implements BuilderNode {}

    /// Builds a primitive leaf, validating the type length — required and positive for a
    /// `FIXED_LEN_BYTE_ARRAY`, absent for every other type — and that the logical type
    /// annotation, if any, is legal for that physical type.
    private static BuilderLeaf leaf(String name, PhysicalType type, RepetitionType repetition, Integer typeLength,
                                    LogicalType logicalType) {
        if (type == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            if (typeLength == null || typeLength <= 0) {
                throw new IllegalArgumentException(
                        "FIXED_LEN_BYTE_ARRAY column " + name + " requires a positive type length");
            }
        }
        else if (typeLength != null) {
            throw new IllegalArgumentException("A type length is only valid for a FIXED_LEN_BYTE_ARRAY column, not "
                    + type + " (" + name + ")");
        }
        LogicalTypeValidator.validate(name, type, repetition, typeLength, logicalType);
        return new BuilderLeaf(name, type, repetition, typeLength, logicalType);
    }

    /// Lowers a primitive leaf to its [SchemaElement], deriving both annotation representations
    /// from the single declared logical type.
    private static SchemaElement leafElement(BuilderLeaf leaf) {
        LogicalTypeAnnotations annotations = LogicalTypeAnnotations.of(leaf.logicalType());
        return new SchemaElement(leaf.name(), leaf.type(), leaf.typeLength(), leaf.repetition(), null,
                annotations.convertedType(), annotations.scale(), annotations.precision(), null,
                annotations.union());
    }

    private record BuilderStruct(String name, RepetitionType repetition, List<BuilderNode> children)
            implements BuilderNode {}

    private record BuilderList(String name, RepetitionType repetition, BuilderNode element) implements BuilderNode {}

    private record BuilderMap(String name, RepetitionType repetition, BuilderLeaf key, BuilderNode value)
            implements BuilderNode {}

    /// Flattens the builder's field tree into the depth-first [SchemaElement] list
    /// [#fromSchemaElements] consumes, a group element followed by its children. A `LIST`
    /// expands to the canonical 3-level shape (the annotated group, a synthetic `repeated
    /// group list`, then the element); a `MAP` expands to the canonical 2-level shape (the
    /// annotated group, a synthetic `repeated group key_value`, then the required `key` and
    /// the value).
    private static void flatten(List<BuilderNode> nodes, List<SchemaElement> out) {
        for (BuilderNode node : nodes) {
            switch (node) {
                case BuilderLeaf leaf -> out.add(leafElement(leaf));
                case BuilderStruct group -> {
                    out.add(SchemaElement.group(group.name(), group.repetition(), group.children().size()));
                    flatten(group.children(), out);
                }
                case BuilderList list -> {
                    out.add(new SchemaElement(list.name(), null, null, list.repetition(), 1,
                            ConvertedType.LIST, null, null, null, new LogicalType.ListType()));
                    out.add(SchemaElement.group("list", RepetitionType.REPEATED, 1));
                    flatten(List.of(list.element()), out);
                }
                case BuilderMap map -> {
                    out.add(new SchemaElement(map.name(), null, null, map.repetition(), 1,
                            ConvertedType.MAP, null, null, null, new LogicalType.MapType()));
                    out.add(SchemaElement.group("key_value", RepetitionType.REPEATED, 2));
                    out.add(leafElement(map.key()));
                    flatten(List.of(map.value()), out);
                }
            }
        }
    }
}
