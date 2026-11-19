/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;

import dev.hardwood.internal.schema.AnnotationKind;
import dev.hardwood.internal.schema.FileReferences;
import dev.hardwood.internal.schema.LeafAnnotation;
import dev.hardwood.internal.schema.LogicalTypeAnnotations;
import dev.hardwood.internal.schema.LogicalTypeValidator;
import dev.hardwood.internal.schema.UnreadableAnnotations;
import dev.hardwood.internal.schema.VariantSpecVersions;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

/// Root schema container representing the complete Parquet schema.
/// Supports both flat schemas and nested structures (structs, lists, maps).
///
/// @see <a href="https://parquet.apache.org/docs/file-format/">File Format</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public class FileSchema {

    /// Marks a dot-separated path in `columnPathToIndex` that more than one column joins to.
    private static final int AMBIGUOUS_PATH = -2;

    private final String name;
    private final List<ColumnSchema> columns;
    private final StringToIntMap columnPathToIndex;
    private final SchemaNode.GroupNode rootNode;

    private FileSchema(String name, List<ColumnSchema> columns, SchemaNode.GroupNode rootNode) {
        this.name = name;
        this.columns = columns;
        this.rootNode = rootNode;

        // Pre-compute field path -> index mapping for O(1) lookup.
        // Uses the dot-separated field path (e.g. "address.zip") as key, which tells apart
        // nested columns sharing a leaf name. Field names containing dots can still make two
        // fields' paths join to the same key (a flat "a.b" and "b" nested in "a"); a column's
        // key shared with another column or with a group is marked ambiguous.
        List<String> groupPaths = new ArrayList<>();
        collectGroupPaths(rootNode, "", groupPaths);
        this.columnPathToIndex = new StringToIntMap(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            String path = columns.get(i).fieldPath().toString();
            columnPathToIndex.put(path, columnPathToIndex.containsKey(path) ? AMBIGUOUS_PATH : i);
        }
        for (String path : groupPaths) {
            if (columnPathToIndex.containsKey(path)) {
                columnPathToIndex.put(path, AMBIGUOUS_PATH);
            }
        }
    }

    /// Adds the dot-separated path of every group below `group` to `paths`.
    private static void collectGroupPaths(SchemaNode.GroupNode group, String prefix, List<String> paths) {
        for (SchemaNode child : group.children()) {
            if (child instanceof SchemaNode.GroupNode childGroup) {
                String path = prefix.isEmpty() ? childGroup.name() : prefix + "." + childGroup.name();
                paths.add(path);
                collectGroupPaths(childGroup, path, paths);
            }
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
    /// A field name may contain dots itself (e.g. `"sepal.length"`). When the paths of
    /// two fields join to the same dot-separated name, the name is ambiguous; use
    /// [#getColumn(FieldPath)] or [#getColumn(int)] for such a column.
    ///
    /// @param name column name or dot-separated field path
    /// @throws IllegalArgumentException if no column with the given name exists, or the name
    ///         is the path of more than one column
    public ColumnSchema getColumn(String name) {
        int index = columnPathToIndex.get(name);
        if (index == AMBIGUOUS_PATH) {
            throw new IllegalArgumentException("Column name '" + name
                    + "' is ambiguous: it is the dot-separated path of more than one field in the schema");
        }
        if (index < 0) {
            throw new IllegalArgumentException("Column not found: " + name);
        }
        return columns.get(index);
    }

    /// Returns the column with the given field path.
    ///
    /// Sibling fields may share a name, so two columns may share a path; use
    /// [#getColumn(int)] for such a column.
    ///
    /// @param fieldPath path from schema root to leaf column
    /// @throws IllegalArgumentException if no column with the given path exists, or more than one
    ///         column has it
    public ColumnSchema getColumn(FieldPath fieldPath) {
        int index = columnPathToIndex.get(fieldPath.toString());
        if (index >= 0 && columns.get(index).fieldPath().equals(fieldPath)) {
            return columns.get(index);
        }
        ColumnSchema match = null;
        if (index == AMBIGUOUS_PATH) {
            for (ColumnSchema column : columns) {
                if (column.fieldPath().equals(fieldPath)) {
                    if (match != null) {
                        throw new IllegalArgumentException("Column path '" + fieldPath
                                + "' is ambiguous: it is the path of more than one column in the schema");
                    }
                    match = column;
                }
            }
        }
        if (match == null) {
            throw new IllegalArgumentException("Column not found: " + fieldPath);
        }
        return match;
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
    ///
    /// Sibling fields may share a name; use [#getRootNode()] and its children for such a field.
    ///
    /// @throws IllegalArgumentException if no top-level field has the name, or more than one has it
    public SchemaNode getField(String name) {
        SchemaNode match = null;
        for (SchemaNode child : rootNode.children()) {
            if (child.name().equals(name)) {
                if (match != null) {
                    throw new IllegalArgumentException("Field name '" + name
                            + "' is ambiguous: more than one top-level field has it");
                }
                match = child;
            }
        }
        if (match == null) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return match;
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

    /// Creates a builder for constructing a schema, flat or nested, programmatically, for use
    /// with the writer.
    ///
    /// @param name the schema (message) name, conventionally `"schema"`
    /// @throws IllegalArgumentException if `name` is `null`
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /// Creates a builder whose root fields `fields` declares on a [StructBuilder], the type a
    /// nested `struct` is declared on, so code that converts another schema model declares the
    /// root and every struct with the same method. More fields can be added to the returned
    /// builder before [Builder#build()].
    ///
    /// ```java
    /// FileSchema schema = FileSchema.builder("schema", root -> addFields(root, source.fields()))
    ///         .build();
    /// ```
    ///
    /// @param name the schema (message) name, conventionally `"schema"`
    /// @param fields declares the root's fields
    /// @throws IllegalArgumentException if `name` or `fields` is `null`, or `fields` declares an
    ///         invalid field
    public static Builder builder(String name, Consumer<StructBuilder> fields) {
        Builder builder = new Builder(name);
        if (fields == null) {
            throw new IllegalArgumentException("The fields lambda of schema " + name + " must not be null");
        }
        fields.accept(builder.content);
        return builder;
    }

    /// Flattens this schema back into the depth-first [SchemaElement] list written
    /// to the file footer, the inverse of [#fromSchemaElements]: the root element
    /// followed by each node in pre-order, groups carrying their child count.
    public List<SchemaElement> toSchemaElements() {
        List<SchemaElement> elements = new ArrayList<>(columns.size() + 1);
        elements.add(new SchemaElement(name, null, null, rootNode.repetitionType(), rootNode.children().size(),
                null, null, null, rootNode.fieldId(), null));
        appendElements(rootNode.children(), elements);
        return elements;
    }

    private void appendElements(List<SchemaNode> nodes, List<SchemaElement> out) {
        for (SchemaNode node : nodes) {
            switch (node) {
                case SchemaNode.PrimitiveNode leaf -> {
                    LogicalTypeAnnotations annotations = LogicalTypeAnnotations.of(leaf.type(), leaf.logicalType());
                    out.add(new SchemaElement(leaf.name(), leaf.type(),
                            columns.get(leaf.columnIndex()).typeLength(), leaf.repetitionType(), null,
                            annotations.convertedType(), annotations.scale(), annotations.precision(),
                            leaf.fieldId(), annotations.union()));
                }
                case SchemaNode.GroupNode group -> {
                    LogicalTypeAnnotations annotations =
                            LogicalTypeAnnotations.ofGroup(group.convertedType(), group.logicalType());
                    out.add(new SchemaElement(group.name(), null, null, group.repetitionType(),
                            group.children().size(), annotations.convertedType(), annotations.scale(),
                            annotations.precision(), group.fieldId(), annotations.union()));
                    appendElements(group.children(), out);
                }
            }
        }
    }

    /// Reconstructs a schema from the depth-first [SchemaElement] list a footer carries: the root,
    /// then each group followed by as many descendants as its `numChildren` counts.
    ///
    /// @throws IllegalArgumentException if the list is empty, its root is not a group, its child
    ///         counts do not describe exactly the elements it holds, a primitive declares
    ///         children, or an element carries an annotation its field cannot carry, such as
    ///         `STRING` on an `INT32` or `UUID` on a `FIXED_LEN_BYTE_ARRAY` of other than 16
    ///         bytes
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
        List<SchemaNode> rootChildren = buildChildren(elements, cursor, root.numChildren() != null ? root.numChildren() : 0,
                0, 0, root.name(), List.of(), columns, columnIndex);
        if (cursor[0] != elements.size()) {
            throw new IllegalArgumentException("The schema elements hold " + (elements.size() - cursor[0])
                    + " element(s) past the root's last descendant, from '" + elements.get(cursor[0]).name() + "' on");
        }

        String rootName = root.name().intern();
        SchemaNode.GroupNode rootNode = new SchemaNode.GroupNode(
                rootName,
                root.repetitionType() != null ? root.repetitionType() : RepetitionType.REQUIRED,
                root.convertedType(),
                root.logicalType(),
                rootChildren,
                0, // Root has def level 0
                0, // Root has rep level 0
                root.fieldId());

        return new FileSchema(rootName, columns, rootNode);
    }

    /// Build children nodes from schema elements.
    ///
    /// @param groupName the dotted path of the group whose children these are, or the root's name
    private static List<SchemaNode> buildChildren(
                                                  List<SchemaElement> elements,
                                                  int[] cursor,
                                                  int numChildren,
                                                  int parentDefLevel,
                                                  int parentRepLevel,
                                                  String groupName,
                                                  List<String> parentPath,
                                                  List<ColumnSchema> columns,
                                                  int[] columnIndex) {

        List<SchemaNode> children = new ArrayList<>();

        for (int i = 0; i < numChildren; i++) {
            if (cursor[0] == elements.size()) {
                throw new IllegalArgumentException("Group '" + groupName + "' declares " + numChildren
                        + " children, but the schema elements end after " + i);
            }
            SchemaElement element = elements.get(cursor[0]);
            // Interned, as every name of the schema is: a reader resolves a name per row through
            // a map of interned names, which a name taken from the schema then hits by reference.
            String name = element.name().intern();
            RepetitionType repType = element.repetitionType() != null ? element.repetitionType() : RepetitionType.OPTIONAL;

            // Calculate levels for this node
            int defLevel = parentDefLevel + (repType != RepetitionType.REQUIRED ? 1 : 0);
            int repLevel = parentRepLevel + (repType == RepetitionType.REPEATED ? 1 : 0);

            // Build path for this node
            List<String> currentPath = new ArrayList<>(parentPath.size() + 1);
            currentPath.addAll(parentPath);
            currentPath.add(name);

            String fault = UnreadableAnnotations.fault(element);
            if (fault != null) {
                throw new IllegalArgumentException("Schema element '" + String.join(".", currentPath) + "': " + fault
                        + "; a schema carries no annotation its field cannot carry");
            }
            if (element.isPrimitive()) {
                if (element.numChildren() != null && element.numChildren() > 0) {
                    throw new IllegalArgumentException("Primitive element '" + String.join(".", currentPath)
                            + "' declares " + element.numChildren() + " children");
                }
                // Primitive node - represents an actual column
                int colIdx = columnIndex[0]++;
                ColumnSchema column = new ColumnSchema(
                        new FieldPath(List.copyOf(currentPath)),
                        element.type(),
                        repType,
                        element.typeLength(),
                        colIdx,
                        defLevel,
                        repLevel,
                        LeafAnnotation.effective(element),
                        element.fieldId());
                columns.add(column);
                children.add(new SchemaNode.PrimitiveNode(column));

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
                        String.join(".", currentPath),
                        currentPath,
                        columns,
                        columnIndex);

                SchemaNode.GroupNode groupNode = new SchemaNode.GroupNode(
                        name,
                        repType,
                        element.convertedType(),
                        effectiveGroupLogicalType(element.logicalType(), groupChildren),
                        groupChildren,
                        defLevel,
                        repLevel,
                        element.fieldId());
                if (groupNode.logicalType() instanceof LogicalType.VariantType variant) {
                    // Only the shape of the version the reader implements is known; a read of a
                    // group of another version is refused by VariantSpecVersions.
                    if (variant.specVersion() == VariantSpecVersions.SUPPORTED) {
                        validateVariantGroup(groupNode);
                    }
                }
                else if (groupNode.isFile()) {
                    FileReferences.validateGroup(groupNode, String.join(".", currentPath));
                }
                children.add(groupNode);
            }
        }

        return children;
    }

    /// Guards the read-side leniency against a schema the caller declared.
    ///
    /// [#fromSchemaElements] refuses an annotation the column's physical type cannot carry,
    /// and [LogicalTypeValidator] already refuses every such pairing when the leaf is declared,
    /// so reaching this means the writer's rule and the reader's have drifted apart rather than
    /// that the caller did anything wrong. Checked first, so the drift reads as such.
    private static void requireNoAnnotationDropped(List<SchemaElement> elements) {
        for (SchemaElement element : elements) {
            if (!element.isPrimitive()) {
                continue;
            }
            String fault = LeafAnnotation.dropFault(element);
            if (fault != null) {
                throw new IllegalStateException("Column '" + element.name() + "': " + fault
                        + ". A declared schema must not carry an annotation the reader drops.");
            }
        }
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
    private static LogicalType effectiveGroupLogicalType(LogicalType annotation, List<SchemaNode> children) {
        if (annotation != null) {
            return annotation;
        }
        if (hasMapKeyValueChild(children)) {
            return LogicalType.map();
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

    /// Validate a Variant-annotated group's shape: a `metadata` child and a `value`
    /// child, both `BYTE_ARRAY`, followed by at most one `typed_value` child (the
    /// shredded representation, reassembled at read time). Only names, order and
    /// physical types are checked; the children's repetition is not.
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
                appendFieldId(sb, group.fieldId());
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
                appendFieldId(sb, prim.fieldId());
                sb.append(";\n");
            }
        }
    }

    private static void appendFieldId(StringBuilder sb, Integer fieldId) {
        if (fieldId != null) {
            sb.append(" = ").append(fieldId);
        }
    }

    /// Builder for constructing a [FileSchema] programmatically.
    ///
    /// Fields are added as top-level primitive leaves ([#primitive]), nested `struct`
    /// groups ([#struct]), `LIST` groups ([#list]), or `MAP` groups ([#map]). The `LIST`
    /// and `MAP` groups emit the canonical 3-level and 2-level physical layouts.
    ///
    /// Every method of this builder and of the builders its lambdas receive throws
    /// `IllegalArgumentException` for a `null` argument, and for a field name that is empty or
    /// already declared among the same group's fields. Error messages name a nested field by its
    /// dotted path, a list element as `<list>.list.element` and a map value as
    /// `<map>.key_value.value`.
    ///
    /// The name, physical type and repetition of a primitive leaf are positional. Its optional
    /// attributes (logical type, type length, field id) are set in a [PrimitiveBuilder] lambda:
    ///
    /// ```java
    /// FileSchema schema = FileSchema.builder("schema")
    ///         .primitive("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
    ///                 col -> col.logicalType(LogicalType.uuid()).fieldId(1))
    ///         .primitive("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
    ///                 col -> col.logicalType(LogicalType.string()).fieldId(2))
    ///         .build();
    /// ```
    public static final class Builder {

        private final String name;
        private final StructBuilder content;

        private Builder(String name) {
            if (name == null) {
                throw new IllegalArgumentException("The schema name must not be null");
            }
            this.name = name;
            this.content = new StructBuilder("", "schema " + name);
        }

        /// Append a primitive column without optional attributes.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED`, or `type` is
        ///         `FIXED_LEN_BYTE_ARRAY` (which needs a type length or an annotation that
        ///         implies one)
        public Builder primitive(String columnName, PhysicalType type, RepetitionType repetition) {
            content.primitive(columnName, type, repetition);
            return this;
        }

        /// Append a primitive column whose optional attributes are set by `column`.
        ///
        /// A `FIXED_LEN_BYTE_ARRAY` column without a [PrimitiveBuilder#typeLength] takes the length
        /// its annotation implies: 16 bytes for `UUID`, 12 for `INTERVAL`, 2 for `FLOAT16`, and
        /// for `DECIMAL` the fewest bytes that hold its precision.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param column sets the column's logical type, type length and field id
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED`, the type length does
        ///         not match the type, the annotation does not apply to the type, or `type` is
        ///         `FIXED_LEN_BYTE_ARRAY` with no type length and no annotation that implies one
        public Builder primitive(String columnName, PhysicalType type, RepetitionType repetition,
                                 Consumer<PrimitiveBuilder> column) {
            content.primitive(columnName, type, repetition, column);
            return this;
        }

        /// Append a `struct` group whose fields are declared by `filler`.
        ///
        /// @param structName the group name
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param filler declares the group's fields, and optionally its field id
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED` or the group has no fields
        public Builder struct(String structName, RepetitionType repetition, Consumer<StructBuilder> filler) {
            content.struct(structName, repetition, filler);
            return this;
        }

        /// Append a `FILE` group, a reference to a range of bytes stored inline or in an external
        /// file. The group declares all six children the format defines, each `OPTIONAL`:
        /// `uri` and `content_type` and `checksum` (`BYTE_ARRAY` annotated `STRING`), `offset`
        /// and `size` (`INT64`), and `inline` (`BYTE_ARRAY`). A row writes it through
        /// [dev.hardwood.writer.StructBuilder#setFile(String, dev.hardwood.row.PqFile)].
        ///
        /// @param fileName the group name
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, or `repetition` is `REPEATED`
        public Builder file(String fileName, RepetitionType repetition) {
            content.file(fileName, repetition);
            return this;
        }

        /// Append a `FILE` group whose own attributes are set by `group`, as
        /// [#file(String, RepetitionType)] describes. The children of the group have no field id.
        ///
        /// @param fileName the group name
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param group sets the group's field id
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, or `repetition` is `REPEATED`
        public Builder file(String fileName, RepetitionType repetition, Consumer<GroupBuilder> group) {
            content.file(fileName, repetition, group);
            return this;
        }

        /// Append a `LIST` group whose element is declared by `element`.
        ///
        /// @param listName the list group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the list itself may be null)
        /// @param element declares the list's element
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED` or no element is declared
        public Builder list(String listName, RepetitionType repetition, Consumer<ElementBuilder> element) {
            content.list(listName, repetition, element);
            return this;
        }

        /// Append a `LIST` group whose own attributes are set by `group` and whose element is
        /// declared by `element`.
        ///
        /// @param listName the list group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the list itself may be null)
        /// @param group sets the list group's field id
        /// @param element declares the list's element
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED` or no element is declared
        public Builder list(String listName, RepetitionType repetition, Consumer<GroupBuilder> group,
                            Consumer<ElementBuilder> element) {
            content.list(listName, repetition, group, element);
            return this;
        }

        /// Append a `MAP` group whose value is declared by `value`. The key is a required
        /// primitive of `keyType` per the canonical layout.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param keyType the physical type of the required `key`
        /// @param value declares the map's value like a list element
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED`, no value is declared,
        ///         or `keyType` is `FIXED_LEN_BYTE_ARRAY`
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                           Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, value);
            return this;
        }

        /// Append a `MAP` group whose key has optional attributes, set by `key`. A `STRING` key
        /// is the usual case. A `FIXED_LEN_BYTE_ARRAY` key takes a type length as
        /// [#primitive(String, PhysicalType, RepetitionType, Consumer)] describes.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param keyType the physical type of the required `key`
        /// @param key sets the key's logical type, type length and field id
        /// @param value declares the map's value like a list element
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED`, no value is declared,
        ///         or the key's attributes do not match `keyType`
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                           Consumer<PrimitiveBuilder> key, Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, key, value);
            return this;
        }

        /// Append a `MAP` group whose own attributes are set by `group` and whose key has
        /// optional attributes, set by `key`, as
        /// [#map(String, RepetitionType, PhysicalType, Consumer, Consumer)] describes.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param group sets the map group's field id
        /// @param keyType the physical type of the required `key`
        /// @param key sets the key's logical type, type length and field id
        /// @param value declares the map's value like a list element
        /// @throws IllegalArgumentException if the name is `null`, empty or already declared, an
        ///         argument is `null`, `repetition` is `REPEATED`, no value is declared,
        ///         or the key's attributes do not match `keyType`
        public Builder map(String mapName, RepetitionType repetition, Consumer<GroupBuilder> group,
                           PhysicalType keyType, Consumer<PrimitiveBuilder> key, Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, group, keyType, key, value);
            return this;
        }

        /// Build the schema.
        ///
        /// @throws UnsupportedOperationException if no fields were added; the writer does not
        ///         produce a schema with no columns
        public FileSchema build() {
            if (content.children.isEmpty()) {
                throw new UnsupportedOperationException("Schema must have at least one column");
            }
            List<SchemaElement> elements = new ArrayList<>();
            elements.add(SchemaElement.group(name, RepetitionType.REQUIRED, content.children.size()));
            flatten(content.children, elements);
            requireNoAnnotationDropped(elements);
            return fromSchemaElements(elements);
        }
    }

    /// Sets the optional attributes of a primitive leaf: a column, a list element, a map key or
    /// a map value. Each attribute can be set one time.
    public static final class PrimitiveBuilder {

        private final String columnName;
        private Integer typeLength;
        private LogicalType logicalType;
        private Integer fieldId;

        private PrimitiveBuilder(String columnName) {
            this.columnName = columnName;
        }

        /// Set the logical type annotation. It must be legal for the leaf's physical type. A leaf
        /// whose lambda does not call this method is unannotated.
        ///
        /// @throws IllegalArgumentException if `logicalType` is `null`, or the logical type is
        ///         already set
        public PrimitiveBuilder logicalType(LogicalType logicalType) {
            requireArgument(logicalType, "logical type", columnName);
            requireUnset(this.logicalType, "logical type", columnName);
            this.logicalType = logicalType;
            return this;
        }

        /// Set the fixed byte length of a `FIXED_LEN_BYTE_ARRAY` leaf. It is necessary only if the
        /// annotation does not imply a length.
        ///
        /// @throws IllegalArgumentException if the type length is already set. The leaf
        ///         declaration rejects a length that is not positive, or a length on a leaf
        ///         that is not `FIXED_LEN_BYTE_ARRAY`.
        public PrimitiveBuilder typeLength(int typeLength) {
            requireUnset(this.typeLength, "type length", columnName);
            this.typeLength = typeLength;
            return this;
        }

        /// Set the field id (`field_id` in `parquet.thrift`).
        ///
        /// @throws IllegalArgumentException if the field id is already set
        public PrimitiveBuilder fieldId(int fieldId) {
            requireUnset(this.fieldId, "field id", columnName);
            this.fieldId = fieldId;
            return this;
        }
    }

    /// Sets the optional attributes of a `LIST`, `MAP` or `FILE` group itself, as opposed to
    /// its element, value or children. Each attribute can be set one time. A `struct` group sets
    /// its own attributes on the [StructBuilder] its filler receives.
    public static final class GroupBuilder {

        private final String groupName;
        private Integer fieldId;

        private GroupBuilder(String groupName) {
            this.groupName = groupName;
        }

        /// Set the field id (`field_id` in `parquet.thrift`).
        ///
        /// @throws IllegalArgumentException if the field id is already set
        public GroupBuilder fieldId(int fieldId) {
            requireUnset(this.fieldId, "field id", groupName);
            this.fieldId = fieldId;
            return this;
        }
    }

    /// Declares the fields of a `struct` group. Nested structs compose by calling
    /// [#struct] again inside the filler. The root of a schema is declared the same way, through
    /// [FileSchema#builder(String, Consumer)].
    public static final class StructBuilder {

        private final String path;
        private final String scope;
        private final List<BuilderNode> children = new ArrayList<>();
        private final Set<String> names = new HashSet<>();
        private Integer fieldId;

        /// @param path the dotted path of this group, empty for the schema root
        /// @param scope how messages name this group: `struct <path>`, or `schema <name>` for the root
        private StructBuilder(String path, String scope) {
            this.path = path;
            this.scope = scope;
        }

        /// Set the field id of this `struct` group.
        ///
        /// @throws IllegalArgumentException if the field id is already set, or this builder
        ///         declares the fields of the schema root, which has no field id
        public StructBuilder fieldId(int fieldId) {
            if (path.isEmpty()) {
                throw new IllegalArgumentException("The root of " + scope + " has no field id");
            }
            requireUnset(this.fieldId, "field id", path);
            this.fieldId = fieldId;
            return this;
        }

        /// Append a primitive field without optional attributes.
        ///
        /// @throws IllegalArgumentException if `columnName` is `null`, empty or already declared
        ///         in this group, `type` or `repetition` is `null`, `repetition` is `REPEATED`, or
        ///         the type is `FIXED_LEN_BYTE_ARRAY`
        public StructBuilder primitive(String columnName, PhysicalType type, RepetitionType repetition) {
            return primitive(columnName, type, repetition, NO_ATTRIBUTES);
        }

        /// Append a primitive field whose optional attributes are set by `column`, as
        /// [Builder#primitive(String, PhysicalType, RepetitionType, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if `columnName` is `null`, empty or already declared
        ///         in this group, an argument is `null`, `repetition` is `REPEATED`, or the
        ///         attributes do not match the type
        public StructBuilder primitive(String columnName, PhysicalType type, RepetitionType repetition,
                                       Consumer<PrimitiveBuilder> column) {
            String childPath = declare(columnName);
            requireFieldRepetition(childPath, repetition);
            children.add(leaf(columnName, childPath, type, repetition, column));
            return this;
        }

        /// Append a nested `struct` field.
        ///
        /// @throws IllegalArgumentException if `structName` is `null`, empty or already declared
        ///         in this group, an argument is `null`, `repetition` is `REPEATED`, or the group
        ///         has no fields
        public StructBuilder struct(String structName, RepetitionType repetition, Consumer<StructBuilder> filler) {
            String childPath = declare(structName);
            requireFieldRepetition(childPath, repetition);
            children.add(buildStruct(structName, childPath, repetition, filler));
            return this;
        }

        /// Append a nested `FILE` field, as [Builder#file(String, RepetitionType)] describes.
        ///
        /// @throws IllegalArgumentException if `fileName` is `null`, empty or already declared in
        ///         this group, `repetition` is `null` or `REPEATED`
        public StructBuilder file(String fileName, RepetitionType repetition) {
            return file(fileName, repetition, NO_GROUP_ATTRIBUTES);
        }

        /// Append a nested `FILE` field whose own attributes are set by `group`, as
        /// [Builder#file(String, RepetitionType, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if `fileName` is `null`, empty or already declared in
        ///         this group, an argument is `null`, or `repetition` is `REPEATED`
        public StructBuilder file(String fileName, RepetitionType repetition, Consumer<GroupBuilder> group) {
            String childPath = declare(fileName);
            requireFieldRepetition(childPath, repetition);
            children.add(new BuilderFile(fileName, repetition, groupFieldId(childPath, group)));
            return this;
        }

        /// Append a nested `LIST` field.
        ///
        /// @throws IllegalArgumentException if `listName` is `null`, empty or already declared in
        ///         this group, an argument is `null`, `repetition` is `REPEATED`, or no element is
        ///         declared
        public StructBuilder list(String listName, RepetitionType repetition, Consumer<ElementBuilder> element) {
            return list(listName, repetition, NO_GROUP_ATTRIBUTES, element);
        }

        /// Append a nested `LIST` field whose own attributes are set by `group`, as
        /// [Builder#list(String, RepetitionType, Consumer, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if `listName` is `null`, empty or already declared in
        ///         this group, an argument is `null`, `repetition` is `REPEATED`, or no element is
        ///         declared
        public StructBuilder list(String listName, RepetitionType repetition, Consumer<GroupBuilder> group,
                                  Consumer<ElementBuilder> element) {
            String childPath = declare(listName);
            requireFieldRepetition(childPath, repetition);
            children.add(buildList(listName, childPath, repetition, group, element));
            return this;
        }

        /// Append a nested `MAP` field.
        ///
        /// @throws IllegalArgumentException if `mapName` is `null`, empty or already declared in
        ///         this group, an argument is `null`, `repetition` is `REPEATED`, no value is
        ///         declared, or `keyType` is `FIXED_LEN_BYTE_ARRAY`
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 Consumer<ElementBuilder> value) {
            return map(mapName, repetition, keyType, NO_ATTRIBUTES, value);
        }

        /// Append a nested `MAP` field whose key has optional attributes, set by `key`, as
        /// [Builder#map(String, RepetitionType, PhysicalType, Consumer, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if `mapName` is `null`, empty or already declared in
        ///         this group, an argument is `null`, `repetition` is `REPEATED`, no value is
        ///         declared, or the key's attributes do not match `keyType`
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 Consumer<PrimitiveBuilder> key, Consumer<ElementBuilder> value) {
            return map(mapName, repetition, NO_GROUP_ATTRIBUTES, keyType, key, value);
        }

        /// Append a nested `MAP` field whose own attributes are set by `group`, as
        /// [Builder#map(String, RepetitionType, Consumer, PhysicalType, Consumer, Consumer)]
        /// describes.
        ///
        /// @throws IllegalArgumentException if `mapName` is `null`, empty or already declared in
        ///         this group, an argument is `null`, `repetition` is `REPEATED`, no value is
        ///         declared, or the key's attributes do not match `keyType`
        public StructBuilder map(String mapName, RepetitionType repetition, Consumer<GroupBuilder> group,
                                 PhysicalType keyType, Consumer<PrimitiveBuilder> key, Consumer<ElementBuilder> value) {
            String childPath = declare(mapName);
            requireFieldRepetition(childPath, repetition);
            children.add(buildMap(mapName, childPath, repetition, group, keyType, key, value));
            return this;
        }

        /// Checks a new field's name against this group and returns the field's dotted path.
        private String declare(String name) {
            if (name == null) {
                throw new IllegalArgumentException("A field name in " + scope + " must not be null");
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("A field name in " + scope + " must not be empty");
            }
            if (!names.add(name)) {
                throw new IllegalArgumentException("Field '" + name + "' is already declared in " + scope);
            }
            return path.isEmpty() ? name : path + "." + name;
        }
    }

    /// Declares the element of a `LIST`, or the value of a `MAP`: a primitive, a nested
    /// `struct`, a nested `LIST`, or a nested `MAP`. The declared node is named `element`
    /// inside a list and `value` inside a map.
    ///
    /// The field id of the element or value is set where it is declared: in the [PrimitiveBuilder]
    /// of a primitive, with [StructBuilder#fieldId(int)] for a `struct`, and in the
    /// [GroupBuilder] of a `FILE`, `LIST` or `MAP`. The field id of the enclosing `LIST` or `MAP`
    /// is set in its own [GroupBuilder].
    public static final class ElementBuilder {

        private final String childName;
        private final String groupPath;
        private final String path;
        private BuilderNode element;

        /// @param childName `element` or `value`
        /// @param groupPath the dotted path of the enclosing `LIST` or `MAP`
        /// @param path the dotted path of the node this builder declares
        private ElementBuilder(String childName, String groupPath, String path) {
            this.childName = childName;
            this.groupPath = groupPath;
            this.path = path;
        }

        /// Declare a primitive element without optional attributes.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         or the type is `FIXED_LEN_BYTE_ARRAY`
        public void primitive(PhysicalType type, RepetitionType repetition) {
            primitive(type, repetition, NO_ATTRIBUTES);
        }

        /// Declare a primitive element whose optional attributes are set by `column`, as
        /// [Builder#primitive(String, PhysicalType, RepetitionType, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         or the attributes do not match the type
        public void primitive(PhysicalType type, RepetitionType repetition, Consumer<PrimitiveBuilder> column) {
            requireFieldRepetition(path, repetition);
            set(leaf(childName, path, type, repetition, column));
        }

        /// Declare a `struct` element.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         or the struct has no fields
        public void struct(RepetitionType repetition, Consumer<StructBuilder> filler) {
            requireFieldRepetition(path, repetition);
            set(buildStruct(childName, path, repetition, filler));
        }

        /// Declare a `FILE` element, as [Builder#file(String, RepetitionType)] describes.
        ///
        /// @throws IllegalArgumentException if `repetition` is `null` or `REPEATED`
        public void file(RepetitionType repetition) {
            file(repetition, NO_GROUP_ATTRIBUTES);
        }

        /// Declare a `FILE` element whose own attributes are set by `group`, as
        /// [Builder#file(String, RepetitionType, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if an argument is `null` or `repetition` is `REPEATED`
        public void file(RepetitionType repetition, Consumer<GroupBuilder> group) {
            requireFieldRepetition(path, repetition);
            set(new BuilderFile(childName, repetition, groupFieldId(path, group)));
        }

        /// Declare a nested `LIST` element.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         or no element is declared
        public void list(RepetitionType repetition, Consumer<ElementBuilder> element) {
            list(repetition, NO_GROUP_ATTRIBUTES, element);
        }

        /// Declare a nested `LIST` element whose own attributes are set by `group`.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         or no element is declared
        public void list(RepetitionType repetition, Consumer<GroupBuilder> group, Consumer<ElementBuilder> element) {
            requireFieldRepetition(path, repetition);
            set(buildList(childName, path, repetition, group, element));
        }

        /// Declare a nested `MAP` element.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         no value is declared, or `keyType` is `FIXED_LEN_BYTE_ARRAY`
        public void map(RepetitionType repetition, PhysicalType keyType, Consumer<ElementBuilder> value) {
            map(repetition, keyType, NO_ATTRIBUTES, value);
        }

        /// Declare a nested `MAP` element whose key has optional attributes, set by `key`, as
        /// [Builder#map(String, RepetitionType, PhysicalType, Consumer, Consumer)] describes.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         no value is declared, or the key's attributes do not match `keyType`
        public void map(RepetitionType repetition, PhysicalType keyType, Consumer<PrimitiveBuilder> key,
                        Consumer<ElementBuilder> value) {
            map(repetition, NO_GROUP_ATTRIBUTES, keyType, key, value);
        }

        /// Declare a nested `MAP` element whose own attributes are set by `group`, as
        /// [Builder#map(String, RepetitionType, Consumer, PhysicalType, Consumer, Consumer)]
        /// describes.
        ///
        /// @throws IllegalArgumentException if an argument is `null`, `repetition` is `REPEATED`,
        ///         no value is declared, or the key's attributes do not match `keyType`
        public void map(RepetitionType repetition, Consumer<GroupBuilder> group, PhysicalType keyType,
                        Consumer<PrimitiveBuilder> key, Consumer<ElementBuilder> value) {
            requireFieldRepetition(path, repetition);
            set(buildMap(childName, path, repetition, group, keyType, key, value));
        }

        private void set(BuilderNode node) {
            if (element != null) {
                throw new IllegalArgumentException("The " + childName + " of " + groupPath + " is already declared");
            }
            element = node;
        }

        private BuilderNode require() {
            if (element == null) {
                throw new IllegalArgumentException("Must declare the " + childName + " of " + groupPath);
            }
            return element;
        }
    }

    private static final Consumer<PrimitiveBuilder> NO_ATTRIBUTES = column -> {
    };

    private static final Consumer<GroupBuilder> NO_GROUP_ATTRIBUTES = group -> {
    };

    /// Refuses a `null` argument the builder would otherwise reinterpret or fail on later.
    private static <T> T requireArgument(T value, String what, String path) {
        if (value == null) {
            throw new IllegalArgumentException("The " + what + " of " + path + " must not be null");
        }
        return value;
    }

    /// Checks the repetition of a declared field: present, and not `REPEATED`, which the
    /// builder expresses with `list` and `map` only.
    private static void requireFieldRepetition(String path, RepetitionType repetition) {
        requireArgument(repetition, "repetition", path);
        if (repetition == RepetitionType.REPEATED) {
            throw new IllegalArgumentException("Field " + path
                    + " is REPEATED; the builder declares a repeated field with list(...) or map(...)");
        }
    }

    /// Runs `group` against a fresh [GroupBuilder] for the group at `path` and returns the field
    /// id it set, or `null` if it set none.
    private static Integer groupFieldId(String path, Consumer<GroupBuilder> group) {
        GroupBuilder builder = new GroupBuilder(path);
        requireArgument(group, "group lambda", path).accept(builder);
        return builder.fieldId;
    }

    private static void requireUnset(Object current, String attribute, String name) {
        if (current != null) {
            throw alreadySet(attribute, name, current);
        }
    }

    private static IllegalArgumentException alreadySet(String attribute, String name, Object current) {
        return new IllegalArgumentException("The " + attribute + " of '" + name + "' is already set to " + current);
    }

    /// Builds a `struct` node named `name` at `path` whose fields `filler` declares. Shared by
    /// every `struct` verb.
    private static BuilderStruct buildStruct(String name, String path, RepetitionType repetition,
                                             Consumer<StructBuilder> filler) {
        StructBuilder nested = new StructBuilder(path, "struct " + path);
        requireArgument(filler, "filler", path).accept(nested);
        if (nested.children.isEmpty()) {
            throw new IllegalArgumentException("Struct " + path + " must have at least one field");
        }
        return new BuilderStruct(name, repetition, nested.fieldId, nested.children);
    }

    /// Builds a `LIST` node named `name` at `path` whose own attributes `group` sets and whose
    /// element `element` declares. Shared by every `list` verb.
    private static BuilderList buildList(String name, String path, RepetitionType repetition,
                                         Consumer<GroupBuilder> group, Consumer<ElementBuilder> element) {
        Integer fieldId = groupFieldId(path, group);
        ElementBuilder builder = new ElementBuilder("element", path, path + ".list.element");
        requireArgument(element, "element lambda", path).accept(builder);
        return new BuilderList(name, repetition, fieldId, builder.require());
    }

    /// Builds a `MAP` node named `name` at `path` with a required `key` primitive of `keyType`
    /// and a `value` declared through the shared [ElementBuilder]. Shared by every `map` verb.
    ///
    /// The key goes through [#leaf] like any other primitive, so its type length and annotation
    /// are validated where the map is declared.
    private static BuilderMap buildMap(String name, String path, RepetitionType repetition,
                                       Consumer<GroupBuilder> group, PhysicalType keyType,
                                       Consumer<PrimitiveBuilder> key, Consumer<ElementBuilder> value) {
        Integer fieldId = groupFieldId(path, group);
        ElementBuilder builder = new ElementBuilder("value", path, path + ".key_value.value");
        requireArgument(value, "value lambda", path).accept(builder);
        BuilderLeaf keyLeaf = leaf("key", path + ".key_value.key", keyType, RepetitionType.REQUIRED, key);
        return new BuilderMap(name, repetition, fieldId, keyLeaf, builder.require());
    }

    private sealed interface BuilderNode {}

    private record BuilderLeaf(String name, PhysicalType type, RepetitionType repetition, Integer typeLength,
                               LogicalType logicalType, Integer fieldId) implements BuilderNode {}

    /// Builds a primitive leaf named `name` at `path` from the attributes `attributes` sets.
    private static BuilderLeaf leaf(String name, String path, PhysicalType type, RepetitionType repetition,
                                    Consumer<PrimitiveBuilder> attributes) {
        requireArgument(type, "physical type", path);
        PrimitiveBuilder column = new PrimitiveBuilder(path);
        requireArgument(attributes, "attribute lambda", path).accept(column);
        return leaf(name, path, type, repetition, column.typeLength, column.logicalType, column.fieldId);
    }

    /// Builds a primitive leaf, validating the type length — required and positive for a
    /// `FIXED_LEN_BYTE_ARRAY`, absent for every other type — and that the logical type
    /// annotation, if any, is legal for that physical type. A `FIXED_LEN_BYTE_ARRAY` declared
    /// without a length takes the one its annotation implies
    /// ([AnnotationKind#impliedFixedWidth]), where it implies one.
    private static BuilderLeaf leaf(String name, String path, PhysicalType type, RepetitionType repetition,
                                    Integer typeLength, LogicalType logicalType, Integer fieldId) {
        if (type != PhysicalType.FIXED_LEN_BYTE_ARRAY && typeLength != null) {
            throw new IllegalArgumentException("A type length is only valid for a FIXED_LEN_BYTE_ARRAY column, not "
                    + type + " (" + path + ")");
        }
        Integer length = type == PhysicalType.FIXED_LEN_BYTE_ARRAY && typeLength == null
                ? AnnotationKind.impliedFixedWidth(logicalType)
                : typeLength;
        LogicalTypeValidator.validateLeaf(path, type, repetition, length, logicalType);
        return new BuilderLeaf(name, type, repetition, length, logicalType, fieldId);
    }

    /// Lowers a primitive leaf to its [SchemaElement], deriving both annotation representations
    /// from the single declared logical type.
    private static SchemaElement leafElement(BuilderLeaf leaf) {
        LogicalTypeAnnotations annotations = LogicalTypeAnnotations.of(leaf.type(), leaf.logicalType());
        return new SchemaElement(leaf.name(), leaf.type(), leaf.typeLength(), leaf.repetition(), null,
                annotations.convertedType(), annotations.scale(), annotations.precision(), leaf.fieldId(),
                annotations.union());
    }

    private static SchemaElement groupElement(String name, RepetitionType repetition, int numChildren,
                                              ConvertedType convertedType, LogicalType logicalType,
                                              Integer fieldId) {
        return new SchemaElement(name, null, null, repetition, numChildren, convertedType, null, null, fieldId,
                logicalType);
    }

    private record BuilderStruct(String name, RepetitionType repetition, Integer fieldId,
                                 List<BuilderNode> children) implements BuilderNode {}

    private record BuilderFile(String name, RepetitionType repetition, Integer fieldId) implements BuilderNode {}

    private record BuilderList(String name, RepetitionType repetition, Integer fieldId, BuilderNode element)
            implements BuilderNode {}

    private record BuilderMap(String name, RepetitionType repetition, Integer fieldId, BuilderLeaf key,
                              BuilderNode value) implements BuilderNode {}

    /// Flattens the builder's field tree into the depth-first [SchemaElement] list
    /// [#fromSchemaElements] consumes, a group element followed by its children. A `LIST`
    /// expands to the canonical 3-level shape (the annotated group, a synthetic `repeated
    /// group list`, then the element); a `MAP` expands to the canonical 2-level shape (the
    /// annotated group, a synthetic `repeated group key_value`, then the required `key` and
    /// the value). The synthetic groups have no field id.
    private static void flatten(List<BuilderNode> nodes, List<SchemaElement> out) {
        for (BuilderNode node : nodes) {
            switch (node) {
                case BuilderLeaf leaf -> out.add(leafElement(leaf));
                case BuilderStruct group -> {
                    out.add(groupElement(group.name(), group.repetition(), group.children().size(), null, null,
                            group.fieldId()));
                    flatten(group.children(), out);
                }
                case BuilderFile file -> {
                    FileReferences.Child[] fileChildren = FileReferences.Child.values();
                    out.add(groupElement(file.name(), file.repetition(), fileChildren.length, null,
                            LogicalType.file(), file.fieldId()));
                    for (FileReferences.Child child : fileChildren) {
                        out.add(leafElement(leaf(child.fieldName(), file.name() + "." + child.fieldName(),
                                child.type(), RepetitionType.OPTIONAL, null, child.annotation(), null)));
                    }
                }
                case BuilderList list -> {
                    out.add(groupElement(list.name(), list.repetition(), 1, ConvertedType.LIST, LogicalType.list(),
                            list.fieldId()));
                    out.add(SchemaElement.group("list", RepetitionType.REPEATED, 1));
                    flatten(List.of(list.element()), out);
                }
                case BuilderMap map -> {
                    out.add(groupElement(map.name(), map.repetition(), 1, ConvertedType.MAP, LogicalType.map(),
                            map.fieldId()));
                    out.add(SchemaElement.group("key_value", RepetitionType.REPEATED, 2));
                    out.add(leafElement(map.key()));
                    flatten(List.of(map.value()), out);
                }
            }
        }
    }
}
