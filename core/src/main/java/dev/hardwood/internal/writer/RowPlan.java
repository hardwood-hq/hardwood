/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.PrecisionLossPolicy;
import dev.hardwood.writer.StructBuilder;

/// The schema as the row-oriented layer sees it: a tree of [RowNode]s that doubles as the
/// builders handed to a caller, and the staging one batch of records accumulates into.
///
/// Built once per writer and reused for the whole file. Staging is checkpointed per record
/// and rolled back if the record fails, so a rejected value leaves the batch exactly as it
/// was.
///
/// The plan keeps flat arrays of every node that stages something, so checkpointing, rolling
/// back, filling and resetting are loops over arrays rather than walks of the tree.
public final class RowPlan {

    /// Collects the tree's nodes by role while the plan is built.
    record Nodes(List<RowLeafNode> leaves, List<RowStructNode> structs, List<RowRepeatedNode> repeated) {

        Nodes() {
            this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
    }

    private final RowStructNode root;
    private final LeafStage[] stages;
    private final int[] stageMarks;
    private final LeafStage[] variableWidthStages;

    private final RowStructNode[] structs;
    private final NullMaskStage[] structNulls;
    private final int[] structMarks;

    private final RowRepeatedNode[] repeated;
    private final int[] repeatedMarks;
    private final int[] repeatedNullMarks;

    private final RowLeafNode[] leaves;

    private RowPlan(RowStructNode root, Nodes nodes) {
        this.root = root;
        this.leaves = nodes.leaves().toArray(new RowLeafNode[0]);
        this.stages = new LeafStage[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            stages[i] = leaves[i].stage();
        }
        this.stageMarks = new int[stages.length];
        List<LeafStage> variableWidth = new ArrayList<>();
        for (LeafStage stage : stages) {
            if (stage instanceof LeafStage.BinaryStage) {
                variableWidth.add(stage);
            }
        }
        this.variableWidthStages = variableWidth.toArray(new LeafStage[0]);

        this.structs = nodes.structs().toArray(new RowStructNode[0]);
        this.structNulls = new NullMaskStage[structs.length];
        for (int i = 0; i < structs.length; i++) {
            structNulls[i] = structs[i].nulls();
        }
        this.structMarks = new int[structs.length];

        this.repeated = nodes.repeated().toArray(new RowRepeatedNode[0]);
        this.repeatedMarks = new int[repeated.length];
        this.repeatedNullMarks = new int[repeated.length];
    }

    /// Builds the plan for a schema, rejecting the shapes the write path cannot produce.
    ///
    /// @param schema the schema to write
    /// @param precisionLossPolicy what a leaf does with a value finer than it can hold
    /// @return the plan
    /// @throws UnsupportedOperationException if the schema has a shape the writer cannot
    ///         produce
    public static RowPlan build(FileSchema schema, PrecisionLossPolicy precisionLossPolicy) {
        RowStructNode root = buildStruct(schema.getRootNode(), "", schema, false, precisionLossPolicy);
        Nodes nodes = new Nodes();
        root.collect(nodes);
        return new RowPlan(root, nodes);
    }

    /// Stages one record, resolving the fields the filler left unset and undoing everything
    /// it staged if it fails.
    ///
    /// @param filler populates the record
    public void writeRecord(Consumer<StructBuilder> filler) {
        // Before the checkpoint, not after: a record started from inside another record's
        // filler would otherwise overwrite the marks the outer record has to roll back to,
        // leaving its values staged when it fails.
        if (root.isActive()) {
            throw root.reenteredScope();
        }
        checkpoint();
        try {
            root.beginScope();
            filler.accept(root);
            root.endScope();
        }
        catch (RuntimeException e) {
            rollback();
            throw e;
        }
    }

    /// Hands every staged column, struct mask and list offset to the batch.
    public void fill(ColumnBatch batch) {
        for (RowLeafNode leaf : leaves) {
            leaf.fill(batch);
        }
        for (RowStructNode struct : structs) {
            struct.fill(batch);
        }
        for (RowRepeatedNode node : repeated) {
            node.fill(batch);
        }
    }

    /// Returns the staging to empty, ready for the next batch.
    public void reset() {
        for (LeafStage stage : stages) {
            stage.reset();
        }
        for (NullMaskStage nulls : structNulls) {
            if (nulls != null) {
                nulls.reset();
            }
        }
        for (RowRepeatedNode node : repeated) {
            node.reset();
            if (node.nulls() != null) {
                node.nulls().reset();
            }
        }
    }

    /// The staged payload of the variable-width columns, which bounds how much a batch holds
    /// beyond its record count.
    public long variableWidthBytes() {
        long bytes = 0;
        for (LeafStage stage : variableWidthStages) {
            bytes += stage.variableWidthBytes();
        }
        return bytes;
    }

    private void checkpoint() {
        for (int i = 0; i < stages.length; i++) {
            stageMarks[i] = stages[i].count;
        }
        for (int i = 0; i < structs.length; i++) {
            structMarks[i] = structNulls[i] == null ? 0 : structNulls[i].count();
        }
        for (int i = 0; i < repeated.length; i++) {
            repeatedMarks[i] = repeated[i].instanceCount();
            NullMaskStage nulls = repeated[i].nulls();
            repeatedNullMarks[i] = nulls == null ? 0 : nulls.count();
        }
    }

    private void rollback() {
        for (int i = 0; i < stages.length; i++) {
            stages[i].truncate(stageMarks[i]);
        }
        for (int i = 0; i < structs.length; i++) {
            if (structNulls[i] != null) {
                structNulls[i].truncate(structMarks[i]);
            }
            structs[i].clearActive();
        }
        for (int i = 0; i < repeated.length; i++) {
            repeated[i].truncate(repeatedMarks[i]);
            NullMaskStage nulls = repeated[i].nulls();
            if (nulls != null) {
                nulls.truncate(repeatedNullMarks[i]);
            }
            repeated[i].clearActive();
        }
    }

    // ==================== Plan construction ====================

    /// Builds a struct node — the record's root, a nested struct, or a map's key/value entry.
    private static RowStructNode buildStruct(SchemaNode.GroupNode group, String path, FileSchema schema,
                                             boolean nullable, PrecisionLossPolicy policy) {
        List<SchemaNode> children = group.children();
        RowNode[] nodes = new RowNode[children.size()];
        String[] fieldNames = new String[children.size()];
        for (int i = 0; i < children.size(); i++) {
            SchemaNode child = children.get(i);
            nodes[i] = buildNode(child, childPath(path, child.name()), schema, policy);
            fieldNames[i] = child.name();
        }
        // The node builds its own name-to-index map from these, and rejects a duplicate name
        // as it does so.
        return new RowStructNode(path, nodes, fieldNames, nullable);
    }

    private static RowNode buildNode(SchemaNode node, String path, FileSchema schema,
                                     PrecisionLossPolicy policy) {
        return switch (node) {
            case SchemaNode.PrimitiveNode leaf -> {
                // A REPEATED leaf reaching a struct's field or a three-level list's element is
                // unproducible and ParquetFileWriter.create has already refused it; the guard
                // repeats it so a plan built on an unvalidated schema fails the same way rather
                // than addressing a shape the shredder cannot honour. The one REPEATED leaf that
                // is producible — a legacy two-level list's entry — is refused by
                // repeatedChild(...) before reaching here.
                WriterSchemaShape.requireWritableLeaf(leaf, path, false);
                yield new RowLeafNode(path, schema.getColumn(leaf.columnIndex()), policy);
            }
            case SchemaNode.GroupNode group -> buildGroup(group, path, schema, policy);
        };
    }

    private static RowNode buildGroup(SchemaNode.GroupNode group, String path, FileSchema schema,
                                      PrecisionLossPolicy policy) {
        boolean optional = group.repetitionType() == RepetitionType.OPTIONAL;
        // Producibility — a group's own repetition, and an annotated group's entry — is settled
        // by ParquetFileWriter.create before this plan is built. Asking the same helper again
        // keeps a plan built on an unvalidated schema from addressing a shape the shredder
        // cannot honour, and keeps one defect to one wording. A LIST's or MAP's own scaffolding
        // never reaches here: the branch below navigates through it.
        WriterSchemaShape.requireWritableGroup(group, path, false);
        if (group.isList() || group.isMap()) {
            SchemaNode.GroupNode repeated = repeatedChild(group, path);
            if (group.isMap()) {
                // A map's entries are a repeated struct of `key` and `value`, populated
                // through the same builder a nested struct uses.
                RowStructNode entry = buildStruct(repeated, childPath(path, repeated.name()),
                        schema, false, policy);
                return new RowMapNode(path, optional, entry);
            }
            String repeatedPath = childPath(path, repeated.name());
            SchemaNode element = listElement(repeated, repeatedPath);
            String elementPath = childPath(repeatedPath, element.name());
            return new RowListNode(path, optional, buildNode(element, elementPath, schema, policy));
        }
        // Any other group — a plain struct, or one carrying an annotation the write path does
        // not interpret, such as VARIANT — is written field by field like a struct.
        return buildStruct(group, path, schema, optional, policy);
    }

    /// Returns the annotated group's repeated entry group. That the entry exists and is
    /// `REPEATED` is settled before the plan is built; what is left is the row layer's own
    /// limit — the builders navigate a list through an element node below the entry, which a
    /// legacy two-level list, whose entry *is* the element, does not have.
    private static SchemaNode.GroupNode repeatedChild(SchemaNode.GroupNode group, String path) {
        SchemaNode child = onlyChild(group, path);
        if (!(child instanceof SchemaNode.GroupNode repeated)) {
            throw new UnsupportedOperationException("Group " + path + " is annotated LIST or MAP and its entry "
                    + child.name() + " is a leaf (a legacy two-level list); the row API reaches a list's values"
                    + " through an element node below the entry, so it writes three-level LIST groups only."
                    + " The columnar API writes this schema");
        }
        return repeated;
    }

    private static SchemaNode onlyChild(SchemaNode.GroupNode group, String path) {
        if (group.children().size() != 1) {
            throw new UnsupportedOperationException("Group " + path + " holds "
                    + group.children().size() + " fields where the LIST or MAP layout it is part of "
                    + "requires exactly one");
        }
        return group.children().get(0);
    }

    /// Returns the element node below a three-level list's repeated entry. An entry holding
    /// several fields is the list's element itself — the legacy two-level list of structs, which
    /// the columnar API writes and the row builders, which reach a list's values through a
    /// single element node below the entry, cannot.
    private static SchemaNode listElement(SchemaNode.GroupNode repeated, String path) {
        if (repeated.children().size() != 1) {
            throw new UnsupportedOperationException("Group " + path + " holds " + repeated.children().size()
                    + " fields and is therefore the list's element itself (a legacy two-level list of structs);"
                    + " the row API reaches a list's values through a single element node below the entry, so it"
                    + " writes three-level LIST groups only. The columnar API writes this schema");
        }
        return repeated.children().get(0);
    }

    /// Spells a field's path the way [WriterSchemaShape] does, so a path naming a field in a
    /// rejection reads the same whichever of the two raised it.
    private static String childPath(String path, String name) {
        return WriterSchemaShape.childPath(path, name);
    }
}
