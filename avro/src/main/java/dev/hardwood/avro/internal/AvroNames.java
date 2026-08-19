/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import dev.hardwood.internal.schema.SchemaNames;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

final class AvroNames {

    record TypeName(String name, String namespace) {
        String fullName() {
            return namespace == null ? name : namespace + "." + name;
        }
    }

    private final IdentityHashMap<SchemaNode, String> localNames;
    private final IdentityHashMap<SchemaNode, String> namespaces;
    private final IdentityHashMap<SchemaNode, String> rewrittenFrom;
    private final SchemaNode.GroupNode rootNode;

    private AvroNames(SchemaNode.GroupNode rootNode) {
        this.rootNode = rootNode;
        this.localNames = new IdentityHashMap<>();
        this.namespaces = new IdentityHashMap<>();
        this.rewrittenFrom = new IdentityHashMap<>();
    }

    static AvroNames forSchema(FileSchema fileSchema) {
        SchemaNode.GroupNode rootNode = fileSchema.getRootNode();
        AvroNames names = new AvroNames(rootNode);
        TypeName rootType = names.rootTypeName(fileSchema.getName());
        names.localNames.put(rootNode, rootType.name());
        names.namespaces.put(rootNode, rootType.namespace());
        names.recordRewrite(rootNode, fileSchema.getName(), rootType);
        names.visitGroup(rootNode, rootType.fullName());
        return names;
    }

    private TypeName rootTypeName(String rawRootName) {
        int lastDot = rawRootName.lastIndexOf('.');
        if (lastDot < 0) {
            return new TypeName(SchemaNames.sanitize(rawRootName), null);
        }
        String rawNamespace = rawRootName.substring(0, lastDot);
        String rawLocal = rawRootName.substring(lastDot + 1);
        String[] segments = rawNamespace.split("\\.", -1);
        StringBuilder namespace = new StringBuilder(rawNamespace.length());
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                namespace.append('.');
            }
            namespace.append(SchemaNames.sanitize(segments[i]));
        }
        return new TypeName(SchemaNames.sanitize(rawLocal), namespace.toString());
    }

    TypeName typeName(SchemaNode node) {
        String local = localNames.get(node);
        String namespace = namespaces.get(node);
        if (local == null || namespace == null && node != rootNode && !namespaces.containsKey(node)) {
            throw new IllegalArgumentException("Unknown schema node: " + node.name());
        }
        return new TypeName(local, namespace);
    }

    String fieldName(SchemaNode node) {
        return localNames.get(node);
    }

    String rewrittenFrom(SchemaNode node) {
        return rewrittenFrom.get(node);
    }

    private void visitGroup(SchemaNode.GroupNode group, String fullName) {
        if (group.isVariant()) {
            return;
        }
        if (group.isList()) {
            SchemaNode element = group.getListElement();
            if (element != null) {
                assignSingleton(element);
                visitNode(element, fullName + "." + localNames.get(element));
            }
            return;
        }
        if (group.isMap()) {
            SchemaNode value = group.getMapValue();
            if (value != null) {
                assignSingleton(value);
                visitNode(value, fullName + "." + localNames.get(value));
            }
            return;
        }
        if (!group.isStruct()) {
            return;
        }
        Map<SchemaNode, String> resolved = resolveScope(group.children());
        localNames.putAll(resolved);
        for (SchemaNode child : group.children()) {
            visitNode(child, fullName + "." + localNames.get(child));
        }
    }

    private void visitNode(SchemaNode node, String fullName) {
        TypeName type = new TypeName(localNames.get(node), namespaceOf(fullName));
        namespaces.put(node, type.namespace());
        recordRewrite(node, node.name(), type);
        if (node instanceof SchemaNode.GroupNode group) {
            visitGroup(group, type.fullName());
        }
    }

    private String namespaceOf(String fullName) {
        return fullName.substring(0, fullName.lastIndexOf('.'));
    }

    private void assignSingleton(SchemaNode node) {
        Map<SchemaNode, String> resolved = resolveScope(List.of(node));
        localNames.putAll(resolved);
    }

    private void recordRewrite(SchemaNode node, String raw, TypeName type) {
        if (!type.fullName().equals(raw) && !type.name().equals(raw)) {
            rewrittenFrom.put(node, raw);
        }
    }

    private static Map<SchemaNode, String> resolveScope(List<SchemaNode> siblings) {
        IdentityHashMap<SchemaNode, String> result = new IdentityHashMap<>();
        Map<String, List<SchemaNode>> groups = new TreeMap<>();
        Set<String> used = new HashSet<>();
        for (SchemaNode sibling : siblings) {
            String candidate = SchemaNames.isLegal(sibling.name())
                    ? sibling.name() : SchemaNames.sanitize(sibling.name());
            groups.computeIfAbsent(candidate, ignored -> new ArrayList<>()).add(sibling);
        }
        for (List<SchemaNode> group : groups.values()) {
            Set<String> rawNames = new HashSet<>();
            for (SchemaNode member : group) {
                if (!rawNames.add(member.name())) {
                    throw new IllegalArgumentException(
                            "Duplicate schema name '" + member.name() + "' in value path");
                }
            }
            if (group.size() == 1) {
                String local = SchemaNames.isLegal(group.get(0).name())
                        ? group.get(0).name() : SchemaNames.sanitize(group.get(0).name());
                result.put(group.get(0), local);
                used.add(local);
            }
        }
        for (List<SchemaNode> group : groups.values()) {
            if (group.size() == 1) {
                continue;
            }
            group.sort((left, right) -> left.name().compareTo(right.name()));
            String candidate = SchemaNames.isLegal(group.get(0).name())
                    ? group.get(0).name() : SchemaNames.sanitize(group.get(0).name());
            SchemaNode winner = group.get(0);
            for (SchemaNode member : group) {
                if (SchemaNames.isLegal(member.name())) {
                    winner = member;
                    break;
                }
            }
            result.put(winner, candidate);
            used.add(candidate);
            for (SchemaNode member : group) {
                if (member == winner) {
                    continue;
                }
                int suffix = 2;
                String local;
                do {
                    local = candidate + "_" + suffix++;
                } while (used.contains(local));
                result.put(member, local);
                used.add(local);
            }
        }
        return result;
    }
}
