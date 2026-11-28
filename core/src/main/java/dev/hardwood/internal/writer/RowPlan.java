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
/// was. A stage outside every repeated field holds one entry per record, so the number of
/// records staged is its checkpoint; only the stages beneath a repeated field, whose entry
/// count varies by record, record a mark of their own.
///
/// The plan keeps flat arrays of every node that stages something, so checkpointing, rolling
/// back, filling and resetting are loops over arrays rather than walks of the tree.
public final class RowPlan {

    /// What every node of the tree is built with: the schema its leaves resolve their columns
    /// in, the policy its leaves apply, and the payload count its variable-width leaves share.
    private record Build(FileSchema schema, PrecisionLossPolicy policy, StagedPayload payload) {
    }

    /// Collects the tree's nodes by role while the plan is built.
    record Nodes(List<RowLeafNode> leaves, List<RowStructNode> structs, List<RowRepeatedNode> repeated) {

        Nodes() {
            this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
    }

    private final RowStructNode root;
    private final LeafStage[] stages;
    /// The payload staged across every variable-width leaf, kept by the leaves as they stage.
    private final StagedPayload payload;

    private final RowStructNode[] structs;
    private final NullMaskStage[] structNulls;

    private final RowRepeatedNode[] repeated;

    private final RowLeafNode[] leaves;

    /// The stages of the leaves outside every repeated field, which hold an entry per record.
    private final LeafStage[] perRecordStages;
    /// The null masks of the `OPTIONAL` structs outside every repeated field.
    private final NullMaskStage[] perRecordStructNulls;
    /// The repeated fields nested in no other, whose offsets and null mask hold an entry per
    /// record.
    private final RowRepeatedNode[] outerRepeated;

    /// The stages of the leaves beneath a repeated field, which hold an entry per element.
    private final LeafStage[] perElementStages;
    private final int[] perElementStageMarks;
    /// The null masks of the `OPTIONAL` structs beneath a repeated field.
    private final NullMaskStage[] perElementStructNulls;
    private final int[] perElementStructMarks;
    /// The repeated fields nested in another, whose offsets and null mask hold an entry per
    /// element of the enclosing one.
    private final RowRepeatedNode[] nestedRepeated;
    private final int[] nestedRepeatedMarks;
    private final int[] nestedRepeatedNullMarks;

    /// Records staged since the last [#reset()]: the checkpoint of every per-record stage.
    private int records;

    private RowPlan(RowStructNode root, Nodes nodes, StagedPayload payload) {
        this.root = root;
        this.payload = payload;
        this.repeated = nodes.repeated().toArray(new RowRepeatedNode[0]);
        this.leaves = nodes.leaves().toArray(new RowLeafNode[0]);
        this.stages = new LeafStage[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            stages[i] = leaves[i].stage();
        }

        this.structs = nodes.structs().toArray(new RowStructNode[0]);
        this.structNulls = new NullMaskStage[structs.length];
        for (int i = 0; i < structs.length; i++) {
            structNulls[i] = structs[i].nulls();
        }

        List<LeafStage> perRecord = new ArrayList<>();
        List<LeafStage> perElement = new ArrayList<>();
        for (int i = 0; i < leaves.length; i++) {
            (beneathRepeated(leaves[i], null) ? perElement : perRecord).add(stages[i]);
        }
        this.perRecordStages = perRecord.toArray(new LeafStage[0]);
        this.perElementStages = perElement.toArray(new LeafStage[0]);
        this.perElementStageMarks = new int[perElementStages.length];

        List<NullMaskStage> perRecordNulls = new ArrayList<>();
        List<NullMaskStage> perElementNulls = new ArrayList<>();
        for (int i = 0; i < structs.length; i++) {
            if (structNulls[i] != null) {
                (beneathRepeated(structs[i], null) ? perElementNulls : perRecordNulls).add(structNulls[i]);
            }
        }
        this.perRecordStructNulls = perRecordNulls.toArray(new NullMaskStage[0]);
        this.perElementStructNulls = perElementNulls.toArray(new NullMaskStage[0]);
        this.perElementStructMarks = new int[perElementStructNulls.length];

        List<RowRepeatedNode> outer = new ArrayList<>();
        List<RowRepeatedNode> nested = new ArrayList<>();
        for (RowRepeatedNode node : repeated) {
            (beneathRepeated(node, node) ? nested : outer).add(node);
        }
        this.outerRepeated = outer.toArray(new RowRepeatedNode[0]);
        this.nestedRepeated = nested.toArray(new RowRepeatedNode[0]);
        this.nestedRepeatedMarks = new int[nestedRepeated.length];
        this.nestedRepeatedNullMarks = new int[nestedRepeated.length];
    }

    /// Whether `node` lies beneath one of the plan's repeated fields other than `self`, judged
    /// by schema path.
    private boolean beneathRepeated(RowNode node, RowRepeatedNode self) {
        for (RowRepeatedNode candidate : repeated) {
            if (candidate != self && node.path.startsWith(candidate.path + ".")) {
                return true;
            }
        }
        return false;
    }

    /// Builds the plan for a schema, rejecting the shapes the write path cannot produce.
    ///
    /// @param schema the schema to write
    /// @param precisionLossPolicy what a leaf does with a value finer than it can hold
    /// @return the plan
    /// @throws UnsupportedOperationException if the schema has a shape the writer cannot
    ///         produce
    public static RowPlan build(FileSchema schema, PrecisionLossPolicy precisionLossPolicy) {
        Build build = new Build(schema, precisionLossPolicy, new StagedPayload());
        RowStructNode root = buildStruct(schema.getRootNode(), "", build, false);
        Nodes nodes = new Nodes();
        root.collect(nodes);
        return new RowPlan(root, nodes, build.payload());
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
            try {
                rollback();
            }
            catch (IllegalStateException inconsistent) {
                inconsistent.addSuppressed(e);
                throw inconsistent;
            }
            throw e;
        }
        records++;
    }

    /// The records staged since the last [#reset()].
    public int stagedRecords() {
        return records;
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
        records = 0;
        payload.bytes = 0;
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

    /// Whether a staged array that holds an entry per element of a `LIST` or `MAP`, rather than
    /// one per record, has reached half of [RowGroupBuffer#MAX_SLICE_RECORDS] entries: a leaf
    /// beneath a repeated field, or the offsets and null mask of a repeated field nested in
    /// another. Those arrays can outgrow the record count, and a record can carry a count past
    /// the threshold; the half left over keeps them within the most a slice holds unless a
    /// single record has more elements than that. Always `false` for a schema without repeated
    /// fields.
    public boolean perElementStagesFull() {
        int threshold = RowGroupBuffer.MAX_SLICE_RECORDS / 2;
        for (LeafStage stage : perElementStages) {
            if (stage.count >= threshold) {
                return true;
            }
        }
        for (RowRepeatedNode node : nestedRepeated) {
            if (node.instanceCount() >= threshold) {
                return true;
            }
        }
        return false;
    }

    /// The staged payload of the variable-width columns, which bounds how much a batch holds
    /// beyond its record count.
    public long variableWidthBytes() {
        return payload.bytes;
    }

    /// Marks the stages beneath a repeated field. Every other stage holds [#records] entries
    /// at this point and needs no mark. A loop over empty arrays for a schema without repeated
    /// fields.
    private void checkpoint() {
        for (int i = 0; i < perElementStages.length; i++) {
            perElementStageMarks[i] = perElementStages[i].count;
        }
        for (int i = 0; i < perElementStructNulls.length; i++) {
            perElementStructMarks[i] = perElementStructNulls[i].count();
        }
        for (int i = 0; i < nestedRepeated.length; i++) {
            nestedRepeatedMarks[i] = nestedRepeated[i].instanceCount();
            NullMaskStage nulls = nestedRepeated[i].nulls();
            nestedRepeatedNullMarks[i] = nulls == null ? 0 : nulls.count();
        }
    }

    private void rollback() {
        for (LeafStage stage : perRecordStages) {
            stage.truncate(records);
        }
        for (int i = 0; i < perElementStages.length; i++) {
            perElementStages[i].truncate(perElementStageMarks[i]);
        }
        for (NullMaskStage nulls : perRecordStructNulls) {
            nulls.truncate(records);
        }
        for (int i = 0; i < perElementStructNulls.length; i++) {
            perElementStructNulls[i].truncate(perElementStructMarks[i]);
        }
        for (RowStructNode struct : structs) {
            struct.clearActive();
        }
        for (RowRepeatedNode node : outerRepeated) {
            truncate(node, records, records);
        }
        for (int i = 0; i < nestedRepeated.length; i++) {
            truncate(nestedRepeated[i], nestedRepeatedMarks[i], nestedRepeatedNullMarks[i]);
        }
    }

    private static void truncate(RowRepeatedNode node, int mark, int nullMark) {
        node.truncate(mark);
        NullMaskStage nulls = node.nulls();
        if (nulls != null) {
            nulls.truncate(nullMark);
        }
        node.clearActive();
    }

    // ==================== Plan construction ====================

    /// Builds a struct node — the record's root, a nested struct, or a map's key/value entry.
    private static RowStructNode buildStruct(SchemaNode.GroupNode group, String path, Build build,
                                             boolean nullable) {
        List<SchemaNode> children = group.children();
        RowNode[] nodes = new RowNode[children.size()];
        String[] fieldNames = new String[children.size()];
        for (int i = 0; i < children.size(); i++) {
            SchemaNode child = children.get(i);
            nodes[i] = buildNode(child, childPath(path, child.name()), build);
            fieldNames[i] = child.name();
        }
        // The node builds its own name-to-index map from these, and rejects a duplicate name
        // as it does so.
        return new RowStructNode(path, nodes, fieldNames, nullable, group.isFile(), group.isVariant());
    }

    private static RowNode buildNode(SchemaNode node, String path, Build build) {
        return switch (node) {
            case SchemaNode.PrimitiveNode leaf -> {
                // A REPEATED leaf reaching a struct's field or a three-level list's element is
                // unproducible and ParquetFileWriter.create has already refused it; the guard
                // repeats it so a plan built on an unvalidated schema fails the same way rather
                // than addressing a shape the shredder cannot honour. The one REPEATED leaf that
                // is producible — a legacy two-level list's entry — is refused by
                // repeatedChild(...) before reaching here.
                WriterSchemaShape.requireWritableLeaf(leaf, path, false);
                yield new RowLeafNode(path, build.schema().getColumn(leaf.columnIndex()), build.policy(),
                        build.payload());
            }
            case SchemaNode.GroupNode group -> buildGroup(group, path, build);
        };
    }

    private static RowNode buildGroup(SchemaNode.GroupNode group, String path, Build build) {
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
                        build, false);
                return new RowMapNode(path, optional, entry);
            }
            String repeatedPath = childPath(path, repeated.name());
            SchemaNode element = listElement(repeated, repeatedPath);
            String elementPath = childPath(repeatedPath, element.name());
            return new RowListNode(path, optional, buildNode(element, elementPath, build));
        }
        // Any other group is written field by field like a struct: a plain struct, a FILE group,
        // which only setFile fills, or one carrying an annotation the write path does not
        // interpret, such as VARIANT.
        return buildStruct(group, path, build, optional);
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
            throw new IllegalArgumentException("Group " + path + " holds "
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
