/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro.internal;

import java.util.ArrayList;
import java.util.Comparator;
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

    private AvroNames() {
        this.localNames = new IdentityHashMap<>();
        this.namespaces = new IdentityHashMap<>();
        this.rewrittenFrom = new IdentityHashMap<>();
    }

    static AvroNames forSchema(FileSchema fileSchema) {
        SchemaNode.GroupNode rootNode = fileSchema.getRootNode();
        AvroNames names = new AvroNames();
        TypeName rootType = names.rootTypeName(fileSchema.getName());
        names.localNames.put(rootNode, rootType.name());
        names.namespaces.put(rootNode, rootType.namespace());
        names.recordRewrite(rootNode, fileSchema.getName(), rootType);
        names.visitGroup(rootNode, rootType.fullName(), rootNode.name());
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
        if (local == null) {
            throw new IllegalArgumentException("Unknown schema node: " + node.name());
        }
        String namespace = namespaces.get(node);
        return new TypeName(local, namespace);
    }

    String fieldName(SchemaNode node) {
        return localNames.get(node);
    }

    String rewrittenFrom(SchemaNode node) {
        return rewrittenFrom.get(node);
    }

    private void visitGroup(SchemaNode.GroupNode group, String fullName, String valuePath) {
        if (group.isVariant()) {
            return;
        }
        if (group.isList()) {
            visitSingleton(group.getListElement(), fullName, valuePath);
            return;
        }
        if (group.isMap()) {
            visitSingleton(group.getMapKey(), fullName, valuePath);
            visitSingleton(group.getMapValue(), fullName, valuePath);
            return;
        }
        if (!group.isStruct()) {
            return;
        }
        localNames.putAll(resolveScope(group.children(), valuePath));
        for (SchemaNode child : group.children()) {
            visitNode(child, fullName + "." + localNames.get(child), valuePath + "." + child.name());
        }
    }

    /// Resolve and visit one value node inside a LIST or MAP.
    /// This node is the list element, map key, or map value.
    /// The synthetic `list` and `key_value` nodes do not add path segments.
    ///
    /// Resolve a map key and value in separate scopes. A map key is not emitted
    /// in the Avro schema, so it must not affect the value's name. Resolve the
    /// key anyway because conversion inspects it and may need its name.
    private void visitSingleton(SchemaNode node, String fullName, String valuePath) {
        if (node == null) {
            return;
        }
        localNames.putAll(resolveScope(List.of(node), valuePath));
        visitNode(node, fullName + "." + localNames.get(node), valuePath + "." + node.name());
    }

    private void visitNode(SchemaNode node, String fullName, String valuePath) {
        TypeName type = new TypeName(localNames.get(node), namespaceOf(fullName));
        namespaces.put(node, type.namespace());
        recordRewrite(node, node.name(), type);
        if (node instanceof SchemaNode.GroupNode group) {
            visitGroup(group, type.fullName(), valuePath);
        }
    }

    private String namespaceOf(String fullName) {
        return fullName.substring(0, fullName.lastIndexOf('.'));
    }

    private void recordRewrite(SchemaNode node, String raw, TypeName type) {
        if (!type.name().equals(raw)) {
            rewrittenFrom.put(node, raw);
        }
    }

    /// Resolve the names of all members in one sibling scope.
    ///
    /// Keep legal raw names. Sanitize other names. If candidates collide, a legal
    /// raw name keeps the bare candidate. If none is legal, the smallest raw name
    /// keeps it. Reserve all bare candidates before adding suffixes.
    private static Map<SchemaNode, String> resolveScope(List<SchemaNode> siblings, String valuePath) {
        Map<String, List<SchemaNode>> groups = new TreeMap<>();
        for (SchemaNode sibling : siblings) {
            groups.computeIfAbsent(candidate(sibling), ignored -> new ArrayList<>()).add(sibling);
        }
        IdentityHashMap<SchemaNode, String> result = new IdentityHashMap<>();
        Set<String> used = new HashSet<>();
        for (Map.Entry<String, List<SchemaNode>> entry : groups.entrySet()) {
            List<SchemaNode> members = entry.getValue();
            rejectDuplicateRawNames(members, valuePath);
            SchemaNode winner = winnerOf(members);
            result.put(winner, entry.getKey());
            used.add(entry.getKey());
        }
        for (Map.Entry<String, List<SchemaNode>> entry : groups.entrySet()) {
            List<SchemaNode> members = entry.getValue();
            if (members.size() == 1) {
                continue;
            }
            members.sort(Comparator.comparing(SchemaNode::name));
            for (SchemaNode member : members) {
                if (result.containsKey(member)) {
                    continue;
                }
                result.put(member, nextFreeSuffix(entry.getKey(), used));
            }
        }
        verifyScope(result, valuePath);
        return result;
    }

    private static String candidate(SchemaNode node) {
        return SchemaNames.isLegal(node.name()) ? node.name() : SchemaNames.sanitize(node.name());
    }

    /// The member that keeps the bare candidate: the sole member whose raw name is
    /// already legal, or, when no member has a legal raw name, the smallest raw name in
    /// natural order. Ordering by raw name rather than by declaration order means
    /// reordering the columns of a file cannot swap two Avro names.
    private static SchemaNode winnerOf(List<SchemaNode> members) {
        SchemaNode smallest = members.getFirst();
        for (SchemaNode member : members) {
            if (SchemaNames.isLegal(member.name())) {
                return member;
            }
            if (member.name().compareTo(smallest.name()) < 0) {
                smallest = member;
            }
        }
        return smallest;
    }

    private static String nextFreeSuffix(String candidate, Set<String> used) {
        for (int suffix = 2; ; suffix++) {
            String local = candidate + "_" + suffix;
            if (used.add(local)) {
                return local;
            }
        }
    }

    private static void rejectDuplicateRawNames(List<SchemaNode> members, String valuePath) {
        Set<String> rawNames = new HashSet<>();
        for (SchemaNode member : members) {
            if (!rawNames.add(member.name())) {
                throw new IllegalArgumentException("Duplicate schema name '" + member.name()
                        + "' in value path '" + valuePath + "'");
            }
        }
    }

    /// Check that every resolved local name is legal and unique.
    /// A failure means the resolver has a bug, not that the input is malformed.
    /// Throw instead of emitting a schema that Avro will reject.
    private static void verifyScope(Map<SchemaNode, String> resolved, String valuePath) {
        Set<String> seen = new HashSet<>();
        for (Map.Entry<SchemaNode, String> entry : resolved.entrySet()) {
            String local = entry.getValue();
            if (!SchemaNames.isLegal(local)) {
                throw new IllegalStateException("Resolved Avro name '" + local + "' for '"
                        + entry.getKey().name() + "' in value path '" + valuePath + "' is not a legal Avro name");
            }
            if (!seen.add(local)) {
                throw new IllegalStateException("Resolved Avro name '" + local + "' for '"
                        + entry.getKey().name() + "' is not unique in value path '" + valuePath + "'");
            }
        }
    }
}
