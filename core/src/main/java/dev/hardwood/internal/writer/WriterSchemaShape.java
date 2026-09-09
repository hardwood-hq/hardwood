/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Which schema shapes the writer can turn into bytes.
///
/// Producibility is a property of the schema alone, so it is settled once — before the
/// destination is opened and before either write view exists — rather than by whichever of
/// the two met the shape first. [RecordShredder] keeps guards of its own, unreachable through
/// the public API once this has run; [RowPlan]'s remaining guards are reached, and are about
/// addressing rather than producing — see below.
///
/// **A schema declares at least one column.** A schema with no leaf has no column chunk to
/// write: the file would be a header and a footer with nothing in between, and the per-column
/// decisions below — the encodings, the share of the buffer each chunk gets — would be made for
/// no column. The format does not forbid such a file and Arrow writes one, so Hardwood reads it
/// — as a relation of no rows — and declines only to produce it. [FileSchema.Builder] already
/// refuses to build such a schema, so the check here is for one built the other way: the
/// childless root [FileSchema#fromSchemaElements] reconstructs from that file's footer.
///
/// The two rules that follow are about repetition.
///
/// **A `REPEATED` field is producible only as the entry of a `LIST` or `MAP` group.** The
/// shredder derives a repetition layer from the annotated group, and [ColumnBatch] addresses
/// that layer's entry offsets through `list(...)` / `map(...)` under the group's path. A
/// `REPEATED` field whose parent carries neither annotation has no layer and no verb: nothing
/// could supply its entry offsets, and shredding it emits one entry per record — every value
/// its own one-element list. Every spelling of the entry is accepted, the three-level
/// `LIST { repeated group list { element } }` and both legacy two-level forms,
/// `LIST { repeated element }` and `LIST { repeated group element { … } }`, because the
/// annotated group supplies the layer either way.
///
/// **An annotated group carries exactly one repetition, in its entry.** A `LIST` or `MAP`
/// group is itself `REQUIRED` or `OPTIONAL` and holds one `REPEATED` field, which a `MAP`
/// additionally requires to be a group, since its entry is a key/value pair. The shredder
/// derives one layer per annotation and the reader derives the repetition levels from the
/// schema, so an annotation over a different arrangement produces a file whose levels and
/// schema disagree — a second repetition with no layer behind it, or a layer over a field
/// that repeats not at all.
///
/// A nullable struct enclosing a `LIST` or `MAP` is producible, so it is not a rule here. A
/// `STRUCT` layer never remaps its item scope, so [RecordShredder]'s leaf-to-root walk levels
/// the repeated field beneath one exactly as it levels one at the record root, and
/// [RowStructNode] / [RowRepeatedNode] cascade the scope the same way. What the shape adds is a
/// rule about the *batch* rather than the schema — a repeated field beneath an absent struct
/// carries no entries, so its offsets must not span any — which [RecordShredder] enforces per
/// batch, where the masks that decide it exist.
///
/// Rules about *addressing* a shape rather than producing it belong to [RowPlan] alone, since
/// the columnar API addresses by index and dotted path and is unharmed by them: two sibling
/// fields sharing a name, and the legacy two-level entries, which the row builders navigate a
/// list through an element node to reach.
public final class WriterSchemaShape {

    private WriterSchemaShape() {
    }

    /// Rejects a schema the writer cannot produce.
    ///
    /// @param schema the schema to write
    /// @throws IllegalArgumentException if the schema declares no columns
    /// @throws UnsupportedOperationException if the schema has a shape the writer cannot produce
    public static void validate(FileSchema schema) {
        if (schema.getColumnCount() == 0) {
            throw new IllegalArgumentException("Schema " + schema.getName()
                    + " has no columns; the writer requires at least one column");
        }
        walk(schema.getRootNode(), "", false);
    }

    /// Walks one group's fields.
    ///
    /// `parentIsAnnotatedGroup` is whether this group is the `LIST` or `MAP` whose entry the
    /// fields being walked are, which is the one place repetition is legal.
    private static void walk(SchemaNode.GroupNode group, String path, boolean parentIsAnnotatedGroup) {
        for (SchemaNode child : group.children()) {
            String childPath = childPath(path, child.name());
            switch (child) {
                case SchemaNode.PrimitiveNode leaf -> requireWritableLeaf(leaf, childPath, parentIsAnnotatedGroup);
                case SchemaNode.GroupNode nested -> {
                    requireWritableGroup(nested, childPath, parentIsAnnotatedGroup);
                    boolean annotated = nested.isList() || nested.isMap();
                    walk(nested, childPath, annotated);
                }
            }
        }
    }

    /// Rejects a leaf the writer cannot produce.
    ///
    /// @param leaf the leaf
    /// @param path the leaf's dotted path
    /// @param parentIsAnnotatedGroup whether the leaf's parent is a `LIST` or `MAP` group
    /// @throws UnsupportedOperationException if the leaf is `REPEATED` outside a `LIST` or `MAP`
    public static void requireWritableLeaf(SchemaNode.PrimitiveNode leaf, String path,
                                           boolean parentIsAnnotatedGroup) {
        if (leaf.repetitionType() == RepetitionType.REPEATED && !parentIsAnnotatedGroup) {
            throw new UnsupportedOperationException("Field " + path + " is a REPEATED leaf outside a LIST or"
                    + " MAP group; the annotation is what gives a repeated field a layer to be addressed"
                    + " through, so the writer cannot produce it");
        }
    }

    /// Rejects a group the writer cannot produce.
    ///
    /// @param group the group
    /// @param path the group's dotted path
    /// @param parentIsAnnotatedGroup whether the group's parent is a `LIST` or `MAP` group, which
    ///        makes this group that annotation's entry scaffolding
    /// @throws UnsupportedOperationException if the group is `REPEATED` outside a `LIST` or `MAP`,
    ///         or is a `LIST` or `MAP` whose own repetition or entry the annotation cannot account for
    public static void requireWritableGroup(SchemaNode.GroupNode group, String path,
                                            boolean parentIsAnnotatedGroup) {
        if (group.isList() || group.isMap()) {
            requireWritableAnnotation(group, path);
            return;
        }
        if (group.repetitionType() == RepetitionType.REPEATED && !parentIsAnnotatedGroup) {
            throw new UnsupportedOperationException("Group " + path + " is REPEATED but carries no LIST or MAP"
                    + " annotation, and is not the entry group of one; the writer cannot produce it");
        }
    }

    /// Rejects a `LIST` or `MAP` group whose repetition the annotation does not account for: the
    /// annotated group repeating on its own behalf, or holding anything other than the single
    /// `REPEATED` entry the shredder derives its one layer from.
    private static void requireWritableAnnotation(SchemaNode.GroupNode group, String path) {
        String annotation = group.isMap() ? "MAP" : "LIST";
        if (group.repetitionType() == RepetitionType.REPEATED) {
            throw new UnsupportedOperationException("Group " + path + " is annotated " + annotation
                    + " and is itself REPEATED; the annotation accounts for the repetition of its entry only, so"
                    + " nothing supplies the group's own entry offsets");
        }
        if (group.children().size() != 1) {
            throw new UnsupportedOperationException("Group " + path + " is annotated " + annotation + " but holds "
                    + group.children().size() + " fields where the layout requires exactly one, its REPEATED entry");
        }
        SchemaNode entry = group.children().get(0);
        if (entry.repetitionType() != RepetitionType.REPEATED) {
            throw new UnsupportedOperationException("Group " + path + " is annotated " + annotation + " but its entry "
                    + entry.name() + " is " + entry.repetitionType() + " rather than REPEATED; the annotation then"
                    + " carries no repetition and the group's entry offsets have nowhere to go");
        }
        if (group.isMap() && !(entry instanceof SchemaNode.GroupNode)) {
            throw new UnsupportedOperationException("Group " + path + " is annotated MAP but its entry "
                    + entry.name() + " is a leaf where the layout requires a repeated group of key and value");
        }
    }

    /// Appends a field name to its parent's dotted path.
    ///
    /// @param path the parent's path, empty at the root
    /// @param name the field name
    /// @return the field's path
    public static String childPath(String path, String name) {
        return path.isEmpty() ? name : path + "." + name;
    }
}
