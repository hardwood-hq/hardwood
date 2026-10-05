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

import dev.hardwood.internal.schema.AnnotationKind;
import dev.hardwood.internal.schema.AnnotationPairings;
import dev.hardwood.internal.schema.LeafAnnotation;
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
/// Supports both flat schemas and nested structures (structs, lists, maps).
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

    /// Creates a builder for constructing a schema, flat or nested, programmatically, for use
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
                    "Ignoring {0} annotation(s) their field cannot carry; each such field is read"
                    + " as though unannotated: {1}",
                    dropped.size(), String.join("; ", dropped));
        }

        SchemaNode.GroupNode rootNode = new SchemaNode.GroupNode(
                root.name(),
                root.repetitionType() != null ? root.repetitionType() : RepetitionType.REQUIRED,
                root.convertedType(),
                root.logicalType(),
                rootChildren,
                0, // Root has def level 0
                0, // Root has rep level 0
                root.fieldId());

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
                        effectiveLogicalType,
                        element.fieldId()));

                children.add(new SchemaNode.PrimitiveNode(
                        element.name(),
                        element.type(),
                        repType,
                        effectiveLogicalType,
                        colIdx,
                        defLevel,
                        repLevel,
                        element.fieldId()));

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

                LogicalType groupAnnotation = readableGroupAnnotation(element, currentPath, dropped);
                SchemaNode.GroupNode groupNode = new SchemaNode.GroupNode(
                        element.name(),
                        repType,
                        readableGroupConvertedType(element, groupAnnotation, currentPath, dropped),
                        effectiveGroupLogicalType(groupAnnotation, groupChildren),
                        groupChildren,
                        defLevel,
                        repLevel,
                        element.fieldId());
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
    /// So the column is reported and read as though the footer had not annotated it. The
    /// physical accessors work, `getValue` yields the physical value, and a logical accessor
    /// fails exactly as it would on any unannotated column of that type.
    ///
    /// The column's statistics are the one consequence that does not follow from the
    /// annotation's absence. The writer recorded them in the order of the annotation, which
    /// the physical type's order need not agree with, so `BoundsReadability` reads no bounds
    /// for such a column. It asks [LeafAnnotation#dropFault] of the footer's schema element,
    /// as this method does.
    ///
    /// The sibling case — an annotation this version does not recognize at all — is dropped
    /// where it is parsed, in `LogicalTypeReader`, and `FileMetaDataReader.ReadFooter` records
    /// the column for `BoundsReadability`. The two differ only in what the warning can tell
    /// the reader, because one is a file from a newer writer and the other is a file from a
    /// broken one.
    private static LogicalType readableLogicalType(SchemaElement element, List<String> path,
                                                   List<String> dropped) {
        String fault = LeafAnnotation.dropFault(element);
        if (fault == null) {
            return LeafAnnotation.effective(element);
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
            String fault = LeafAnnotation.dropFault(element);
            if (fault != null) {
                throw new IllegalStateException("Column '" + element.name() + "': " + fault
                        + ". A declared schema must not carry an annotation the reader drops.");
            }
        }
    }

    /// The group's annotation, or `null` where it is one only a primitive carries, which the
    /// reader drops as it drops an annotation a primitive's physical type cannot carry.
    private static LogicalType readableGroupAnnotation(SchemaElement element, List<String> path,
                                                       List<String> dropped) {
        LogicalType annotation = element.logicalType();
        if (AnnotationPairings.readableGroupAnnotation(annotation) == annotation) {
            return annotation;
        }
        dropped.add(String.join(".", path) + " (" + annotation + " annotates a primitive, but the field is a group)");
        return null;
    }

    /// The group's legacy converted type, or `null` where it is one only a primitive carries, or
    /// names another structure than the group's readable `annotation`, which decides.
    private static ConvertedType readableGroupConvertedType(SchemaElement element, LogicalType annotation,
                                                            List<String> path, List<String> dropped) {
        ConvertedType converted = element.convertedType();
        if (AnnotationPairings.readableGroupConvertedType(annotation, converted) == converted) {
            return converted;
        }
        dropped.add(String.join(".", path) + " (" + converted + (AnnotationPairings.annotatesGroup(converted)
                ? " contradicts the group's " + annotation + ")"
                : " annotates a primitive, but the field is a group)"));
        return null;
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
                appendFieldId(sb, group);
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
                appendFieldId(sb, prim);
                sb.append(";\n");
            }
        }
    }

    private static void appendFieldId(StringBuilder sb, SchemaNode node) {
        if (node.fieldId() != null) {
            sb.append(" = ").append(node.fieldId());
        }
    }

    /// Builder for constructing a [FileSchema] programmatically.
    ///
    /// Fields are added as top-level primitive leaves ([#addColumn]), nested `struct`
    /// groups ([#struct]), `LIST` groups ([#list]), or `MAP` groups ([#map]). The `LIST`
    /// and `MAP` groups emit the canonical 3-level and 2-level physical layouts.
    ///
    /// A field's name, physical type and repetition are positional. A leaf's optional
    /// attributes (logical type, type length, field id) are declared on a [ColumnBuilder],
    /// and a group's field id on the builder that declares the group's content.
    public static final class Builder {

        private final String name;
        private final StructBuilder content;

        private Builder(String name) {
            this.name = name;
            this.content = new StructBuilder(name);
        }

        /// Append a primitive column.
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, or `type` is
        ///         `FIXED_LEN_BYTE_ARRAY`, which needs a type length or an annotation implying one
        public Builder addColumn(String columnName, PhysicalType type, RepetitionType repetition) {
            content.addColumn(columnName, type, repetition);
            return this;
        }

        /// Append a primitive column whose logical type, type length or field id is declared by
        /// `column`.
        ///
        /// ```java
        /// builder.addColumn("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
        ///         column -> column.logicalType(LogicalType.uuid()).fieldId(1));
        /// ```
        ///
        /// @param columnName the column name
        /// @param type the physical type
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param column declares the column's optional attributes
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, the type length does
        ///         not match the type, or the annotation does not apply to the physical type and
        ///         type length
        public Builder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                 Consumer<ColumnBuilder> column) {
            content.addColumn(columnName, type, repetition, column);
            return this;
        }

        /// Append a `struct` group whose fields are declared by `filler`.
        ///
        /// @param structName the group name
        /// @param repetition `REQUIRED` or `OPTIONAL`
        /// @param filler declares the group's fields and, through [StructBuilder#fieldId], its
        ///        field id
        /// @throws IllegalArgumentException if `repetition` is `REPEATED` or the group has no fields
        public Builder struct(String structName, RepetitionType repetition, Consumer<StructBuilder> filler) {
            content.struct(structName, repetition, filler);
            return this;
        }

        /// Append a `LIST` group whose element is declared by `element`.
        ///
        /// @param listName the list group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the list itself may be null)
        /// @param element declares the list's element and, through [ElementBuilder#fieldId], the
        ///        list's field id
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
        /// @param value declares the map's value, like a list element, and, through
        ///        [ElementBuilder#fieldId], the map's field id
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         or `keyType` is `FIXED_LEN_BYTE_ARRAY`
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                           Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, value);
            return this;
        }

        /// Append a `MAP` group whose key's logical type, type length or field id is declared by
        /// `key`, a `STRING` key being the common case.
        ///
        /// @param mapName the map group name
        /// @param repetition `REQUIRED` or `OPTIONAL` (whether the map itself may be null)
        /// @param keyType the physical type of the required `key`
        /// @param key declares the key's optional attributes, as for a column
        /// @param value declares the map's value, like a list element, and, through
        ///        [ElementBuilder#fieldId], the map's field id
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         the key's type length does not match its type, or its annotation does not apply
        ///         to it
        public Builder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                           Consumer<ColumnBuilder> key, Consumer<ElementBuilder> value) {
            content.map(mapName, repetition, keyType, key, value);
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

    /// Declares the optional attributes of a primitive leaf: a column, a list element, a map
    /// value or a map key. Each attribute is declared at most once.
    public static final class ColumnBuilder {

        private final String columnName;
        private LogicalType logicalType;
        private boolean logicalTypeDeclared;
        private Integer typeLength;
        private Integer fieldId;

        private ColumnBuilder(String columnName) {
            this.columnName = columnName;
        }

        /// Annotate the leaf with a logical type.
        ///
        /// A `FIXED_LEN_BYTE_ARRAY` leaf declared without a [#typeLength] takes the length its
        /// annotation implies: 16 bytes for `UUID`, 12 for `INTERVAL`, 2 for `FLOAT16`, and for
        /// `DECIMAL` the fewest bytes that hold its precision. Any other annotation implies none.
        ///
        /// @param logicalType the annotation, which must be legal for the leaf's physical type
        ///        and type length, or `null` for none
        /// @throws IllegalArgumentException if the logical type is already declared
        public ColumnBuilder logicalType(LogicalType logicalType) {
            requireUndeclared(logicalTypeDeclared, "logical type");
            this.logicalType = logicalType;
            logicalTypeDeclared = true;
            return this;
        }

        /// Give the fixed byte length of a `FIXED_LEN_BYTE_ARRAY` leaf.
        ///
        /// @param typeLength the byte length, positive
        /// @throws IllegalArgumentException if the type length is already declared; a length on
        ///         any other physical type, or one that is not positive, is rejected when the leaf
        ///         is declared
        public ColumnBuilder typeLength(int typeLength) {
            requireUndeclared(this.typeLength != null, "type length");
            this.typeLength = typeLength;
            return this;
        }

        /// Give the leaf a `field_id`, the schema element's id by which table formats such as
        /// Iceberg identify a column.
        ///
        /// @param fieldId the field id
        /// @throws IllegalArgumentException if the field id is already declared
        public ColumnBuilder fieldId(int fieldId) {
            requireUndeclared(this.fieldId != null, "field id");
            this.fieldId = fieldId;
            return this;
        }

        private void requireUndeclared(boolean declared, String attribute) {
            if (declared) {
                throw new IllegalArgumentException("The " + attribute + " of column " + columnName
                        + " is already declared");
            }
        }
    }

    /// Declares the fields of a `struct` group. Nested structs compose by calling
    /// [#struct] again inside the filler.
    public static final class StructBuilder {

        private final String structName;
        private final List<BuilderNode> children = new ArrayList<>();
        private Integer fieldId;

        private StructBuilder(String structName) {
            this.structName = structName;
        }

        /// Give the struct a `field_id`.
        ///
        /// @param fieldId the field id
        /// @throws IllegalArgumentException if the field id is already declared
        public StructBuilder fieldId(int fieldId) {
            if (this.fieldId != null) {
                throw new IllegalArgumentException("The field id of struct " + structName + " is already declared");
            }
            this.fieldId = fieldId;
            return this;
        }

        /// Append a primitive field.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, or the type is
        ///         `FIXED_LEN_BYTE_ARRAY`, which needs a type length or an annotation implying one
        public StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition) {
            return addColumn(columnName, type, repetition, NO_ATTRIBUTES);
        }

        /// Append a primitive field whose logical type, type length or field id is declared by
        /// `column`, as for [Builder#addColumn(String, PhysicalType, RepetitionType, Consumer)].
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, the type length does
        ///         not match the type, or the annotation does not apply to the physical type and
        ///         type length
        public StructBuilder addColumn(String columnName, PhysicalType type, RepetitionType repetition,
                                       Consumer<ColumnBuilder> column) {
            if (repetition == RepetitionType.REPEATED) {
                throw new IllegalArgumentException(
                        "Repeated columns are not yet supported by the writer: " + columnName);
            }
            children.add(leaf(columnName, type, repetition, column));
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
            children.add(buildStruct(structName, repetition, filler));
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
            children.add(buildList(listName, repetition, element));
            return this;
        }

        /// Append a nested `MAP` field.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         or `keyType` is `FIXED_LEN_BYTE_ARRAY`
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 Consumer<ElementBuilder> value) {
            return map(mapName, repetition, keyType, NO_ATTRIBUTES, value);
        }

        /// Append a nested `MAP` field whose key's logical type, type length or field id is
        /// declared by `key`.
        ///
        /// @throws IllegalArgumentException if `repetition` is `REPEATED`, no value is declared,
        ///         the key's type length does not match its type, or its annotation does not apply
        ///         to it
        public StructBuilder map(String mapName, RepetitionType repetition, PhysicalType keyType,
                                 Consumer<ColumnBuilder> key, Consumer<ElementBuilder> value) {
            if (repetition == RepetitionType.REPEATED) {
                throw new IllegalArgumentException(
                        "Repeated groups are not yet supported by the writer: " + mapName);
            }
            children.add(buildMap(mapName, repetition, keyType, key, value));
            return this;
        }
    }

    /// Declares the element of a `LIST`, or the value of a `MAP`: a primitive, a nested
    /// `struct`, a nested `LIST`, or a nested `MAP`. The declared node is named `element`
    /// inside a list and `value` inside a map. [#fieldId] gives the enclosing `LIST` or `MAP`
    /// group its field id; the element's own id is declared where the element is.
    public static final class ElementBuilder {

        private final String groupName;
        private final String childName;
        private BuilderNode element;
        private Integer fieldId;

        private ElementBuilder(String groupName, String childName) {
            this.groupName = groupName;
            this.childName = childName;
        }

        /// Give the enclosing `LIST` or `MAP` group a `field_id`.
        ///
        /// @param fieldId the field id
        /// @throws IllegalArgumentException if the field id is already declared
        public ElementBuilder fieldId(int fieldId) {
            if (this.fieldId != null) {
                throw new IllegalArgumentException("The field id of " + groupName + " is already declared");
            }
            this.fieldId = fieldId;
            return this;
        }

        /// Declare a primitive element.
        ///
        /// @throws IllegalArgumentException if the type is `FIXED_LEN_BYTE_ARRAY`, which needs a
        ///         type length or an annotation implying one
        public void primitive(PhysicalType type, RepetitionType repetition) {
            primitive(type, repetition, NO_ATTRIBUTES);
        }

        /// Declare a primitive element whose logical type, type length or field id is declared by
        /// `column`.
        ///
        /// @throws IllegalArgumentException if the type length does not match the type or the
        ///         annotation does not apply to it
        public void primitive(PhysicalType type, RepetitionType repetition, Consumer<ColumnBuilder> column) {
            set(leaf(childName, type, repetition, column));
        }

        /// Declare a `struct` element.
        ///
        /// @throws IllegalArgumentException if the struct has no fields
        public void struct(RepetitionType repetition, Consumer<StructBuilder> filler) {
            set(buildStruct(childName, repetition, filler));
        }

        /// Declare a nested `LIST` element.
        ///
        /// @throws IllegalArgumentException if no element is declared
        public void list(RepetitionType repetition, Consumer<ElementBuilder> element) {
            set(buildList(childName, repetition, element));
        }

        /// Declare a nested `MAP` element.
        ///
        /// @throws IllegalArgumentException if no value is declared, or `keyType` is
        ///         `FIXED_LEN_BYTE_ARRAY`
        public void map(RepetitionType repetition, PhysicalType keyType, Consumer<ElementBuilder> value) {
            map(repetition, keyType, NO_ATTRIBUTES, value);
        }

        /// Declare a nested `MAP` element whose key's logical type, type length or field id is
        /// declared by `key`.
        ///
        /// @throws IllegalArgumentException if no value is declared, the key's type length does
        ///         not match its type, or its annotation does not apply to it
        public void map(RepetitionType repetition, PhysicalType keyType, Consumer<ColumnBuilder> key,
                        Consumer<ElementBuilder> value) {
            set(buildMap(childName, repetition, keyType, key, value));
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

    private static final Consumer<ColumnBuilder> NO_ATTRIBUTES = column -> {
    };

    private static BuilderStruct buildStruct(String name, RepetitionType repetition, Consumer<StructBuilder> filler) {
        StructBuilder nested = new StructBuilder(name);
        filler.accept(nested);
        if (nested.children.isEmpty()) {
            throw new IllegalArgumentException("Struct must have at least one field: " + name);
        }
        return new BuilderStruct(name, repetition, nested.children, nested.fieldId);
    }

    private static BuilderList buildList(String name, RepetitionType repetition, Consumer<ElementBuilder> element) {
        ElementBuilder builder = new ElementBuilder(name, "element");
        element.accept(builder);
        return new BuilderList(name, repetition, builder.require(name), builder.fieldId);
    }

    /// Builds a `MAP` node named `name` with a required `key` primitive of `keyType` and a
    /// `value` declared through the shared [ElementBuilder]. Shared by every `map` verb.
    ///
    /// The key goes through [#leaf] like any other primitive, so its type length and annotation
    /// are validated where the map is declared.
    private static BuilderMap buildMap(String name, RepetitionType repetition, PhysicalType keyType,
                                       Consumer<ColumnBuilder> key, Consumer<ElementBuilder> value) {
        ElementBuilder builder = new ElementBuilder(name, "value");
        value.accept(builder);
        BuilderLeaf keyLeaf = leaf("key", keyType, RepetitionType.REQUIRED, key);
        return new BuilderMap(name, repetition, keyLeaf, builder.require(name), builder.fieldId);
    }

    private sealed interface BuilderNode {}

    private record BuilderLeaf(String name, PhysicalType type, RepetitionType repetition, Integer typeLength,
                               LogicalType logicalType, Integer fieldId) implements BuilderNode {}

    /// Builds a primitive leaf from the attributes `attributes` declares, validating the type
    /// length — required and positive for a `FIXED_LEN_BYTE_ARRAY`, absent for every other
    /// type — and that the logical type annotation, if any, is legal for that physical type. A
    /// `FIXED_LEN_BYTE_ARRAY` declared without a length takes the one its annotation implies
    /// ([AnnotationKind#impliedFixedWidth]), where it implies one.
    private static BuilderLeaf leaf(String name, PhysicalType type, RepetitionType repetition,
                                    Consumer<ColumnBuilder> attributes) {
        ColumnBuilder column = new ColumnBuilder(name);
        attributes.accept(column);
        if (type != PhysicalType.FIXED_LEN_BYTE_ARRAY && column.typeLength != null) {
            throw new IllegalArgumentException("A type length is only valid for a FIXED_LEN_BYTE_ARRAY column, not "
                    + type + " (" + name + ")");
        }
        Integer length = type == PhysicalType.FIXED_LEN_BYTE_ARRAY && column.typeLength == null
                ? AnnotationKind.impliedFixedWidth(column.logicalType)
                : column.typeLength;
        LogicalTypeValidator.validateLeaf(name, type, repetition, length, column.logicalType);
        return new BuilderLeaf(name, type, repetition, length, column.logicalType, column.fieldId);
    }

    /// Lowers a primitive leaf to its [SchemaElement], deriving both annotation representations
    /// from the single declared logical type.
    private static SchemaElement leafElement(BuilderLeaf leaf) {
        LogicalTypeAnnotations annotations = LogicalTypeAnnotations.of(leaf.type(), leaf.logicalType());
        return new SchemaElement(leaf.name(), leaf.type(), leaf.typeLength(), leaf.repetition(), null,
                annotations.convertedType(), annotations.scale(), annotations.precision(), leaf.fieldId(),
                annotations.union());
    }

    private record BuilderStruct(String name, RepetitionType repetition, List<BuilderNode> children,
                                 Integer fieldId) implements BuilderNode {}

    private record BuilderList(String name, RepetitionType repetition, BuilderNode element, Integer fieldId)
            implements BuilderNode {}

    private record BuilderMap(String name, RepetitionType repetition, BuilderLeaf key, BuilderNode value,
                              Integer fieldId) implements BuilderNode {}

    /// Flattens the builder's field tree into the depth-first [SchemaElement] list
    /// [#fromSchemaElements] consumes, a group element followed by its children. A `LIST`
    /// expands to the canonical 3-level shape (the annotated group, a synthetic `repeated
    /// group list`, then the element); a `MAP` expands to the canonical 2-level shape (the
    /// annotated group, a synthetic `repeated group key_value`, then the required `key` and
    /// the value). The synthetic groups carry no field id.
    private static void flatten(List<BuilderNode> nodes, List<SchemaElement> out) {
        for (BuilderNode node : nodes) {
            switch (node) {
                case BuilderLeaf leaf -> out.add(leafElement(leaf));
                case BuilderStruct group -> {
                    out.add(new SchemaElement(group.name(), null, null, group.repetition(), group.children().size(),
                            null, null, null, group.fieldId(), null));
                    flatten(group.children(), out);
                }
                case BuilderList list -> {
                    out.add(new SchemaElement(list.name(), null, null, list.repetition(), 1,
                            ConvertedType.LIST, null, null, list.fieldId(), LogicalType.list()));
                    out.add(SchemaElement.group("list", RepetitionType.REPEATED, 1));
                    flatten(List.of(list.element()), out);
                }
                case BuilderMap map -> {
                    out.add(new SchemaElement(map.name(), null, null, map.repetition(), 1,
                            ConvertedType.MAP, null, null, map.fieldId(), LogicalType.map()));
                    out.add(SchemaElement.group("key_value", RepetitionType.REPEATED, 2));
                    out.add(leafElement(map.key()));
                    flatten(List.of(map.value()), out);
                }
            }
        }
    }
}
