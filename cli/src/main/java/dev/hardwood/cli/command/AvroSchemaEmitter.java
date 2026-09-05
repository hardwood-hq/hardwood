/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dev.hardwood.cli.internal.JsonStrings;
import dev.hardwood.internal.schema.SchemaNames;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Renders a Parquet [FileSchema] as an Avro record schema. Lists and maps keep the
/// recursive structure of the source; fixed-width columns become named `fixed` types;
/// and every named type — record or fixed — renders through the conversion-wide
/// [AvroTypeNames] context, so definitions and references agree on one unique full
/// name. Only this package (and the schema command's tests) may depend on it.
final class AvroSchemaEmitter {

    private AvroSchemaEmitter() {
    }


    static String toAvroSchema(FileSchema schema) {
        AvroTypeNames names = AvroTypeNames.forSchema(schema);
        StringBuilder sb = new StringBuilder();
        appendAvroRecord(sb, schema.getRootNode(), 0, names);
        return sb.toString();
    }

    /// Renders a record: the full definition at the type's first use — with its
    /// resolved name and namespace from the conversion-wide context — and a dotted
    /// full-name reference at every later site. The root is always rendered in full.
    private static void appendAvroRecord(StringBuilder sb, SchemaNode.GroupNode group, int indent,
            AvroTypeNames names) {
        String p = "  ".repeat(indent);
        sb.append(p).append("{\n");
        sb.append(p).append("  \"type\": \"record\",\n");
        sb.append(p).append("  \"name\": \"").append(names.localName(group)).append("\",\n");
        String namespace = names.namespace(group);
        if (!namespace.isEmpty()) {
            sb.append(p).append("  \"namespace\": \"").append(namespace).append("\",\n");
        }
        String doc = avroDoc(group.name());
        if (!doc.isEmpty()) {
            sb.append(p).append("  ").append(doc).append("\n");
        }
        sb.append(p).append("  \"fields\": [\n");

        List<SchemaNode> children = group.children();
        Set<String> usedNames = new HashSet<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            SchemaNode child = children.get(i);
            appendAvroField(sb, child, SchemaCommand.disambiguate(SchemaNames.sanitize(child.name()), usedNames), indent + 2, names);
            if (i < children.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }

        sb.append(p).append("  ]\n");
        sb.append(p).append("}");
    }

    private static void appendAvroField(StringBuilder sb, SchemaNode node, String avroName, int indent,
            AvroTypeNames names) {
        boolean optional = node.repetitionType() == RepetitionType.OPTIONAL;
        String p = "  ".repeat(indent);
        sb.append(p).append("{ \"name\": \"").append(avroName).append("\", ");
        String doc = avroDoc(node.name());
        if (!doc.isEmpty()) {
            sb.append(doc).append(" ");
        }
        sb.append("\"type\": ");
        appendAvroType(sb, node, optional, indent, names);
        if (optional) {
            sb.append(", \"default\": null");
        }
        sb.append(" }");
    }

    /// Returns a `doc` attribute carrying the Parquet name whenever that name does not
    /// survive the mapping onto the Avro name grammar, so the rewrite is visible rather
    /// than silent. Compared against the sanitized name only: the capitalization applied
    /// to record names is cosmetic and does not warrant a note.
    private static String avroDoc(String parquetName) {
        if (parquetName.equals(SchemaNames.sanitize(parquetName))) {
            return "";
        }
        return "\"doc\": \"Parquet name: " + JsonStrings.escape(parquetName) + "\",";
    }

    /// Field names resolve through [SchemaCommand.disambiguate]: sanitizing is not
    /// injective, so two Parquet names can map onto the same name, and neither Avro
    /// nor proto allows a record to repeat a field name — hence the numeric suffix.
    private static void appendAvroType(StringBuilder sb, SchemaNode node, boolean optional, int indent,
            AvroTypeNames names) {
        if (optional) {
            sb.append("[\"null\", ");
        }
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> appendAvroPrimitiveType(sb, prim, names);
            case SchemaNode.GroupNode group when group.isList() -> {
                SchemaNode elem = group.getListElement();
                if (elem == null) {
                    throw new IllegalArgumentException("List '" + group.name() + "' has no resolvable element");
                }
                sb.append("{\"type\": \"array\", \"items\": ");
                appendAvroType(sb, elem, elem.repetitionType() == RepetitionType.OPTIONAL, indent + 1, names);
                sb.append("}");
            }
            case SchemaNode.GroupNode group when group.isMap() -> appendAvroMapType(sb, group, indent, names);
            case SchemaNode.GroupNode group -> {
                if (names.needsDefinition(group)) {
                    sb.append("\n");
                    appendAvroRecord(sb, group, indent, names);
                }
                else {
                    sb.append("\"").append(names.fullName(group)).append("\"");
                }
            }
        }
        if (optional) {
            sb.append("]");
        }
    }

    private static String primitiveToAvroType(SchemaNode.PrimitiveNode prim) {
        return switch (prim.type()) {
            case BOOLEAN -> "boolean";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case INT32 -> {
                boolean unsignedInt = prim.logicalType() instanceof LogicalType.IntType it && !it.isSigned() && it.bitWidth() == 32;
                yield unsignedInt ? "long" : "int";
            }
            case INT64 -> "long";
            case BYTE_ARRAY -> prim.logicalType() instanceof LogicalType.StringType
                    || prim.logicalType() instanceof LogicalType.EnumType
                    || prim.logicalType() instanceof LogicalType.JsonType ? "string" : "bytes";
            case FIXED_LEN_BYTE_ARRAY, INT96 -> throw new IllegalArgumentException(
                    "Fixed-width types are rendered as named Avro fixed types, not scalars");
        };
    }

    /// Renders the type of a primitive. `FIXED_LEN_BYTE_ARRAY` and `INT96` preserve
    /// their physical width as a named Avro `fixed`; every other primitive keeps the
    /// plain scalar mapping.
    private static void appendAvroPrimitiveType(StringBuilder sb, SchemaNode.PrimitiveNode prim, AvroTypeNames names) {
        switch (prim.type()) {
            case FIXED_LEN_BYTE_ARRAY -> {
                if (prim.logicalType() instanceof LogicalType.IntervalType) {
                    sb.append(names.canonicalFixed("interval", 12));
                }
                else if (prim.logicalType() instanceof LogicalType.Float16Type) {
                    sb.append(names.canonicalFixed("float16", 2));
                }
                else {
                    sb.append(names.fixedReference(prim));
                }
            }
            case INT96 -> sb.append(names.fixedReference(prim));
            default -> sb.append("\"").append(primitiveToAvroType(prim)).append("\"");
        }
    }

    /// Avro fixes map keys to strings, so a map is only representable when its Parquet
    /// key is a string-compatible primitive; anything else is reported, never silently
    /// narrowed. The value keeps its recursive structure. A key-only map has no value
    /// type at all, which Avro renders as bare `null` values.
    private static void appendAvroMapType(StringBuilder sb, SchemaNode.GroupNode group, int indent,
            AvroTypeNames names) {
        SchemaNode key = group.getMapKey();
        validateAvroMapKey(key, group.name());
        sb.append("{\"type\": \"map\", \"values\": ");
        SchemaNode value = group.getMapValue();
        if (value == null) {
            sb.append("\"null\"");
        } else {
            appendAvroType(sb, value, value.repetitionType() == RepetitionType.OPTIONAL, indent + 1, names);
        }
        sb.append("}");
    }

    /// Avro fixes map keys to strings, so a map is representable only when its Parquet
    /// key is a string-compatible primitive: BYTE_ARRAY annotated STRING, ENUM, or
    /// JSON. Anything else is reported, never silently narrowed.
    static void validateAvroMapKey(SchemaNode key, String mapName) {
        boolean representable = key instanceof SchemaNode.PrimitiveNode keyPrim
                && keyPrim.type() == PhysicalType.BYTE_ARRAY
                && (keyPrim.logicalType() instanceof LogicalType.StringType
                        || keyPrim.logicalType() instanceof LogicalType.EnumType
                        || keyPrim.logicalType() instanceof LogicalType.JsonType);
        if (!representable) {
            String description = key == null ? "missing key" : SchemaCommand.describeKeyType(key);
            throw new IllegalArgumentException("Avro map keys must be STRING, ENUM, or JSON; map '" + mapName
                    + "' has key " + description);
        }
    }
}
