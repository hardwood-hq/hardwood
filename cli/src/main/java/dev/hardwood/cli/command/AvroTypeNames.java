/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import dev.hardwood.internal.schema.SchemaNames;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Conversion-wide registry of the named Avro types (records and fixed types) the
/// emitter defines. Names are resolved for the whole schema before rendering begins,
/// so every declaration and reference of a named type agrees on its final name.
///
/// Path rules: the root keeps the CLI's existing effective name; a direct struct or
/// fixed child of a record is namespaced by that record's full name; a list or map
/// field contributes its own segment before a named descendant. A struct or fixed
/// child proposes `sanitize(capitalize(node.name()))`, a list or map field proposes
/// `sanitize(node.name())`. Both kinds name one segment under the same record, so
/// they compete in a single pool: a container can never take over the record name of
/// a sibling struct, which would merge their nested types onto one full name.
/// Candidates colliding inside one namespace resolve by the #895 ordering: a legal
/// raw name wins the bare candidate, otherwise the smallest raw name wins, exact
/// duplicates fall back to declaration order, and every loser receives `_2`, `_3`, ….
/// The canonical `Interval` and `Float16` fixed types are reserved globally,
/// unnamespaced, and defined once.
final class AvroTypeNames {
    private final FileSchema schema;
    private final Map<SchemaNode, String> fullNames = new IdentityHashMap<>();
    private final Set<String> emitted = new HashSet<>();

    private AvroTypeNames(FileSchema schema) {
        this.schema = schema;
    }

    static AvroTypeNames forSchema(FileSchema schema) {
        AvroTypeNames names = new AvroTypeNames(schema);
        SchemaNode.GroupNode root = schema.getRootNode();
        String rootName = SchemaNames.sanitize(SchemaCommand.capitalize(schema.getName()));
        names.fullNames.put(root, rootName);
        names.visitRecordChildren(root, rootName);
        return names;
    }

    /// Renders the named `fixed` for `prim`: the definition at the type's first use,
    /// a full-name reference afterwards.
    String fixedReference(SchemaNode.PrimitiveNode prim) {
        String fullName = fullNames.get(prim);
        if (fullName == null) {
            throw new IllegalArgumentException("No resolved name for fixed column " + prim.name());
        }
        if (emitted.add(fullName)) {
            StringBuilder def = new StringBuilder();
            def.append("{\"type\": \"fixed\", \"name\": \"")
                    .append(localName(fullName))
                    .append("\"");
            String namespace = namespaceOf(fullName);
            if (!namespace.isEmpty()) {
                def.append(", \"namespace\": \"")
                        .append(namespace)
                        .append("\"");
            }
            def.append(", \"size\": ")
                    .append(fixedSize(prim))
                    .append("}");
            return def.toString();
        }
        return "\"" + fullName + "\"";
    }

    /// The resolved full name of `group`.
    String fullName(SchemaNode.GroupNode group) {
        String fullName = fullNames.get(group);
        if (fullName == null) {
            throw new IllegalArgumentException("No resolved name for record " + group.name());
        }
        return fullName;
    }

    /// Whether the record's full definition still needs to be emitted: true at the
    /// rendering site that expands it, false at every later site, which references the
    /// full name instead.
    boolean needsDefinition(SchemaNode.GroupNode group) {
        return emitted.add(fullName(group));
    }

    /// The final local (unqualified) name of `group`.
    String localName(SchemaNode.GroupNode group) {
        return localName(fullName(group));
    }

    /// The resolved namespace of `group`, empty for the root.
    String namespace(SchemaNode.GroupNode group) {
        return namespaceOf(fullName(group));
    }

    /// Renders the canonical, unnamespaced fixed type `name`: the definition at its
    /// first use, a bare-name reference afterwards.
    String canonicalFixed(String name, int size) {
        if (emitted.add(name)) {
            // The empty namespace anchors the definition to the root namespace no
            // matter where it is first used: without it, a definition rendered inside
            // a namespaced record silently inherits that namespace, and the bare-name
            // references from sibling scopes stop resolving.
            return "{\"type\": \"fixed\", \"name\": \"" + name + "\", \"namespace\": \"\", \"size\": " + size + "}";
        }
        return "\"" + name + "\"";
    }

    private int fixedSize(SchemaNode.PrimitiveNode prim) {
        if (prim.type() == PhysicalType.INT96) {
            return 12;
        }
        Integer typeLength = schema.getColumn(prim.columnIndex()).typeLength();
        if (typeLength == null) {
            throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY column '" + prim.name()
                    + "' is missing its type length");
        }
        return typeLength;
    }

    private void visitRecordChildren(SchemaNode.GroupNode record, String scope) {
        List<NodeCandidate> candidates = candidatesOf(record.children());
        Map<SchemaNode, String> localNames = resolve(candidates);
        for (NodeCandidate candidate : candidates) {
            String local = join(scope, localNames.get(candidate.node()));
            switch (candidate.node()) {
                case SchemaNode.GroupNode g when g.isList() -> {
                    SchemaNode elem = g.getListElement();
                    if (elem != null) {
                        visitContainer(elem, local);
                    }
                }
                case SchemaNode.GroupNode g when g.isMap() -> {
                    SchemaNode value = g.getMapValue();
                    if (value != null) {
                        visitContainer(value, local);
                    }
                }
                case SchemaNode.GroupNode g -> {
                    fullNames.put(g, local);
                    visitRecordChildren(g, local);
                }
                default -> fullNames.put(candidate.node(), local);
            }
        }
    }

    /// Every child of one record that claims a name in that record's scope: a list or
    /// map claims a namespace segment, a struct or fixed child claims a type name.
    /// Both kinds land in one list, because both become the segment after the record's
    /// full name and therefore collide with each other.
    private static List<NodeCandidate> candidatesOf(List<SchemaNode> children) {
        List<NodeCandidate> candidates = new ArrayList<>(children.size());
        for (SchemaNode child : children) {
            switch (child) {
                case SchemaNode.GroupNode g when g.isList() || g.isMap() ->
                        candidates.add(new NodeCandidate(g, SchemaNames.sanitize(g.name()), g.name()));
                case SchemaNode.GroupNode g -> candidates.add(new NodeCandidate(g, typeCandidate(g), g.name()));
                case SchemaNode.PrimitiveNode p
                        when p.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY || p.type() == PhysicalType.INT96 ->
                        candidates.add(new NodeCandidate(p, typeCandidate(p), p.name()));
                default -> {}
            }
        }
        return candidates;
    }

    /// Visits a list element or map value sitting in `namespace`. Containers pass
    /// through, contributing their own name as the next segment; a struct or fixed
    /// descendant is the sole named type of its namespace.
    private void visitContainer(SchemaNode node, String namespace) {
        switch (node) {
            case SchemaNode.GroupNode g when g.isList() -> {
                SchemaNode elem = g.getListElement();
                if (elem != null) {
                    visitContainer(elem, join(namespace, SchemaNames.sanitize(g.name())));
                }
            }
            case SchemaNode.GroupNode g when g.isMap() -> {
                SchemaNode value = g.getMapValue();
                if (value != null) {
                    visitContainer(value, join(namespace, SchemaNames.sanitize(g.name())));
                }
            }
            case SchemaNode.GroupNode g -> {
                String fullName = join(namespace, typeCandidate(g));
                fullNames.put(g, fullName);
                visitRecordChildren(g, fullName);
            }
            case SchemaNode.PrimitiveNode p
                    when p.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY || p.type() == PhysicalType.INT96 ->
                    fullNames.put(p, join(namespace, typeCandidate(p)));
            default -> {}
        }
    }

    /// Assigns each candidate its final local name within one record scope.
    private static Map<SchemaNode, String> resolve(List<NodeCandidate> candidates) {
        Map<String, List<NodeCandidate>> groups = new TreeMap<>();
        for (NodeCandidate candidate : candidates) {
            groups.computeIfAbsent(candidate.candidate(), ignored -> new ArrayList<>()).add(candidate);
        }
        // Every bare candidate is reserved up front, so a suffix never lands on
        // another sibling's bare name.
        Set<String> used = new HashSet<>(groups.keySet());
        Map<SchemaNode, String> localNames = new IdentityHashMap<>();
        for (List<NodeCandidate> members : groups.values()) {
            NodeCandidate winner = winnerOf(members);
            localNames.put(winner.node(), winner.candidate());
            List<NodeCandidate> losers = new ArrayList<>(members);
            losers.remove(winner);
            // Stable sort: exact duplicate raw names keep declaration order.
            losers.sort(Comparator.comparing(NodeCandidate::raw));
            for (NodeCandidate loser : losers) {
                localNames.put(loser.node(), SchemaCommand.disambiguate(winner.candidate(), used));
            }
        }
        return localNames;
    }

    private static NodeCandidate winnerOf(List<NodeCandidate> members) {
        // A legal raw candidate wins the bare candidate, legal candidates competing
        // with each other by raw name. Without a legal candidate the smallest raw name
        // wins. Members arrive in declaration order, so exact duplicate raw names keep
        // source order.
        NodeCandidate winner = null;
        for (NodeCandidate member : members) {
            if (!SchemaNames.isLegal(member.raw())) {
                continue;
            }
            if (winner == null || member.raw().compareTo(winner.raw()) < 0) {
                winner = member;
            }
        }
        if (winner != null) {
            return winner;
        }
        for (NodeCandidate member : members) {
            if (winner == null || member.raw().compareTo(winner.raw()) < 0) {
                winner = member;
            }
        }
        return winner;
    }

    private static String typeCandidate(SchemaNode node) {
        return SchemaNames.sanitize(SchemaCommand.capitalize(node.name()));
    }

    private static String join(String namespace, String segment) {
        return namespace.isEmpty() ? segment : namespace + "." + segment;
    }

    private static String localName(String fullName) {
        return fullName.substring(fullName.lastIndexOf('.') + 1);
    }

    private static String namespaceOf(String fullName) {
        int lastDot = fullName.lastIndexOf('.');
        return lastDot < 0 ? "" : fullName.substring(0, lastDot);
    }

    private record NodeCandidate(SchemaNode node, String candidate, String raw) {}
}
