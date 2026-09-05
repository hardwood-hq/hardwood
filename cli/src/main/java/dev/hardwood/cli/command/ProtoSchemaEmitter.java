/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dev.hardwood.cli.internal.JsonStrings;
import dev.hardwood.internal.schema.SchemaNames;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Renders a Parquet [FileSchema] as a Protobuf 3 message definition. Lists and maps
/// keep their recursive structure: plain elements and values map to scalars or message
/// types, while optional and nested-container boundaries are wrapped in synthesized
/// single-field messages, since proto3 cannot place `optional` under `repeated`, a
/// `map` inside a `map` value, or `repeated` inside `repeated`. Message declarations
/// resolve through per-scope registries so every reference names exactly one
/// declaration. Only this package (and the schema command's tests) may depend on it.
final class ProtoSchemaEmitter {
    private ProtoSchemaEmitter() {}

    static String toProtoSchema(FileSchema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("syntax = \"proto3\";\n\n");
        String s = SchemaCommand.capitalize(schema.getRootNode().name());
        String sanitized = SchemaNames.sanitize(s);
        appendProtoMessage(sb, schema.getRootNode(), sanitized, 0);
        return sb.toString();
    }

    /// Renders one message: its fields, then the declarations it queued — ordinary
    /// nested groups and synthesized wrapper messages — in encounter order.
    private static void appendProtoMessage(StringBuilder sb, SchemaNode.GroupNode group, String messageName,
                                           int indent) {
        String p = "  ".repeat(indent);
        sb.append(p).append("message ").append(messageName).append(" {\n");
        ProtoScope scope = new ProtoScope();
        int fieldNum = 1;

        List<SchemaNode> children = group.children();
        List<String> fieldNames = new ArrayList<>(children.size());
        Set<String> usedFieldNames = new HashSet<>(children.size());
        for (SchemaNode child : children) {
            fieldNames.add(SchemaCommand.disambiguate(SchemaNames.sanitize(child.name()), usedFieldNames));
        }
        // protoc keeps a message's field names and nested type names in one symbol
        // space: a declaration named like a sibling field makes the file unparseable
        // ("X is already defined"). The declaration registry therefore starts out
        // seeded with this message's field names.
        scope.usedMessageNames.addAll(fieldNames);

        for (int i = 0; i < children.size(); i++) {
            fieldNum = appendProtoField(sb, children.get(i), fieldNames.get(i), fieldNum, indent + 1, scope);
        }

        for (StringBuilder nested : scope.declarations) {
            sb.append("\n");
            sb.append(nested);
        }

        sb.append(p).append("}\n");
    }

    private static int appendProtoField(StringBuilder sb, SchemaNode node, String protoName,
                                        int fieldNum, int indent, ProtoScope scope) {
        String p = "  ".repeat(indent);
        appendProtoComment(sb, p, node.name());
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                String mod = prim.repetitionType() == RepetitionType.OPTIONAL ? "optional " : "";
                sb.append(p).append(mod).append(primitiveToProtoType(prim))
                        .append(" ").append(protoName).append(" = ").append(fieldNum).append(";\n");
            }
            case SchemaNode.GroupNode group when group.isList() -> {
                SchemaNode elem = group.getListElement();
                if (elem == null) {
                    throw new IllegalArgumentException("List '" + group.name() + "' has no resolvable element");
                }
                String protoType;
                if (needsProtoWrapper(elem)) {
                    protoType = scope.registerMessageName(SchemaNames.sanitize(group.name()) + "Element");
                    scope.declarations.add(wrapperMessage(elem, "element", indent, protoType));
                } else {
                    protoType = directProtoType(elem, indent, scope);
                }
                sb.append(p).append("repeated ").append(protoType)
                        .append(" ").append(protoName).append(" = ").append(fieldNum).append(";\n");
            }
            case SchemaNode.GroupNode group when group.isMap() -> {
                String keyType = protoMapKeyType(group);
                SchemaNode value = group.getMapValue();
                String valueType;
                if (value == null) {
                    // A key-only map represents each present key with an empty value
                    // object rather than inventing a scalar.
                    valueType = scope.registerMessageName(SchemaNames.sanitize(group.name()) + "Value");
                    StringBuilder e = new StringBuilder("  ".repeat(indent))
                            .append("message ").append(valueType).append(" {\n")
                            .repeat("  ", indent).append("}\n");
                    scope.declarations.add(e);
                } else if (needsProtoWrapper(value)) {
                    valueType = scope.registerMessageName(SchemaNames.sanitize(group.name()) + "Value");
                    scope.declarations.add(wrapperMessage(value, "value", indent, valueType));
                } else {
                    valueType = directProtoType(value, indent, scope);
                }
                sb.append(p).append("map<").append(keyType).append(", ").append(valueType).append(">")
                        .append(" ").append(protoName).append(" = ").append(fieldNum).append(";\n");
            }
            case SchemaNode.GroupNode group -> {
                String messageName = directProtoType(group, indent, scope);
                sb.append(p).append(messageName).append(" ").append(protoName).append(" = ").append(fieldNum).append(";\n");
            }
        }
        return fieldNum + 1;
    }

    /// A wrapped position: nullability or a nested container that proto3 cannot place
    /// directly under `repeated` or inside a `map` value.
    private static boolean needsProtoWrapper(SchemaNode node) {
        return node.repetitionType() == RepetitionType.OPTIONAL
                || node instanceof SchemaNode.GroupNode group && (group.isList() || group.isMap());
    }

    /// The scalar or message type used when no wrapper is needed. A plain group is
    /// declared in this scope and referenced by its resolved message name.
    private static String directProtoType(SchemaNode node, int indent, ProtoScope scope) {
        if (node instanceof SchemaNode.PrimitiveNode prim) {
            return primitiveToProtoType(prim);
        }
        SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
        String messageName = scope.registerMessageName(group.name());
        StringBuilder nested = new StringBuilder();
        appendProtoMessage(nested, group, messageName, indent);
        scope.declarations.add(nested);
        return messageName;
    }

    /// Protobuf map keys may only be integral or string scalars; float, double, bytes,
    /// and group keys are rejected rather than narrowed.
    static String protoMapKeyType(SchemaNode.GroupNode group) {
        SchemaNode key = group.getMapKey();
        boolean representable = key instanceof SchemaNode.PrimitiveNode keyPrim
                && switch (keyPrim.type()) {
            case BOOLEAN, INT32, INT64, INT96 -> true;
            case BYTE_ARRAY -> keyPrim.logicalType() instanceof LogicalType.StringType
                    || keyPrim.logicalType() instanceof LogicalType.EnumType
                    || keyPrim.logicalType() instanceof LogicalType.JsonType;
            case FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY -> false;
        };
        if (!representable) {
            throw new IllegalArgumentException("Protobuf map keys must be an integer, bool, or string scalar; map '"
                    + group.name() + "' has key " + SchemaCommand.describeKeyType(key));
        }
        return primitiveToProtoType((SchemaNode.PrimitiveNode) key);
    }

    /// Renders the wrapper message for one nullable or nested container boundary:
    /// exactly one field named `element` (list boundary) or `value` (map boundary) with
    /// number 1, plus — lexically nested — the wrappers any deeper boundary needs.
    /// `wrapperName` is already registered in the enclosing scope so the field that
    /// references the wrapper and this declaration agree on the name.
    private static StringBuilder wrapperMessage(SchemaNode wrapped, String fieldName, int indent,
                                                String wrapperName) {
        String p = "  ".repeat(indent);
        // Deeper boundaries nest lexically inside this wrapper, so they resolve names
        // in the wrapper's own scope, seeded with its single field name.
        ProtoScope inner = new ProtoScope();
        inner.usedMessageNames.add(fieldName);
        StringBuilder body = new StringBuilder();
        body.append(p).append("message ").append(wrapperName).append(" {\n");
        String fp = "  ".repeat(indent + 1);
        switch (wrapped) {
            case SchemaNode.PrimitiveNode prim -> {
                String mod = prim.repetitionType() == RepetitionType.OPTIONAL ? "optional " : "";
                body.append(fp).append(mod).append(primitiveToProtoType(prim))
                        .append(" ").append(fieldName).append(" = 1;\n");
            }
            case SchemaNode.GroupNode group when group.isList() -> {
                SchemaNode elem = group.getListElement();
                if (elem == null) {
                    throw new IllegalArgumentException("List '" + group.name() + "' has no resolvable element");
                }
                if (needsProtoWrapper(elem)) {
                    String nested = inner.registerMessageName(SchemaNames.sanitize(group.name()) + "Element");
                    body.append(fp).append("repeated ").append(nested).append(" ").append(fieldName).append(" = 1;\n");
                    inner.declarations.add(wrapperMessage(elem, "element", indent + 1, nested));
                } else {
                    body.append(fp).append("repeated ").append(directProtoType(elem, indent + 1, inner))
                            .append(" ").append(fieldName).append(" = 1;\n");
                }
            }
            case SchemaNode.GroupNode group when group.isMap() -> {
                String keyType = protoMapKeyType(group);
                SchemaNode value = group.getMapValue();
                String valueType;
                if (value == null) {
                    valueType = inner.registerMessageName(SchemaNames.sanitize(group.name()) + "Value");
                    inner.declarations.add(new StringBuilder("  ".repeat(indent + 1))
                            .append("message ").append(valueType).append(" {\n")
                            .repeat("  ", indent + 1).append("}\n"));
                } else if (needsProtoWrapper(value)) {
                    valueType = inner.registerMessageName(SchemaNames.sanitize(group.name()) + "Value");
                    inner.declarations.add(wrapperMessage(value, "value", indent + 1, valueType));
                } else {
                    valueType = directProtoType(value, indent + 1, inner);
                }
                body.append(fp).append("map<").append(keyType).append(", ").append(valueType).append(">")
                        .append(" ").append(fieldName).append(" = 1;\n");
            }
            case SchemaNode.GroupNode group -> {
                String mod = group.repetitionType() == RepetitionType.OPTIONAL ? "optional " : "";
                String messageName = directProtoType(group, indent + 1, inner);
                body.append(fp).append(mod).append(messageName).append(" ").append(fieldName).append(" = 1;\n");
            }
        }
        for (StringBuilder nested : inner.declarations) {
            body.append("\n");
            body.append(nested);
        }
        body.append(p).append("}\n");
        return body;
    }

    /// Per-message scope for declaration naming: ordinary nested groups and synthesized
    /// wrappers share one registry, so same-scope declarations stay unique with
    /// deterministic `_2`, `_3`, … suffixes in source declaration order. Field names
    /// keep their separate disambiguation set.
    private static final class ProtoScope {
        private final Set<String> usedMessageNames = new HashSet<>();
        private final List<StringBuilder> declarations = new ArrayList<>();

        private String registerMessageName(String base) {
            String candidate = SchemaNames.sanitize(SchemaCommand.capitalize(base));
            return SchemaCommand.disambiguate(candidate, usedMessageNames);
        }
    }

    /// Notes the Parquet name in a line comment whenever it does not survive the mapping
    /// onto the proto identifier grammar — the counterpart to Avro's `doc` attribute.
    /// The name is escaped so that one containing a line break cannot end the comment
    /// early and swallow the field that follows.
    private static void appendProtoComment(StringBuilder sb, String p, String parquetName) {
        if (parquetName.equals(SchemaNames.sanitize(parquetName))) {
            return;
        }
        sb.append(p).append("// Parquet name: ").append(JsonStrings.escape(parquetName)).append("\n");
    }

    private static String primitiveToProtoType(SchemaNode.PrimitiveNode prim) {
        return switch (prim.type()) {
            case BOOLEAN -> "bool";
            case INT32 -> prim.logicalType() instanceof LogicalType.IntType it && !it.isSigned() ? "uint32" : "int32";
            case INT64, INT96 ->
                    prim.logicalType() instanceof LogicalType.IntType it && !it.isSigned() ? "uint64" : "int64";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case BYTE_ARRAY -> prim.logicalType() instanceof LogicalType.StringType
                    || prim.logicalType() instanceof LogicalType.EnumType
                    || prim.logicalType() instanceof LogicalType.JsonType ? "string" : "bytes";
            case FIXED_LEN_BYTE_ARRAY -> prim.logicalType() instanceof LogicalType.UuidType ? "string" : "bytes";
        };
    }
}
