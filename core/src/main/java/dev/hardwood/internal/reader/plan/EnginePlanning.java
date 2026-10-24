/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.Sections;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.plan.RowGroupIterator.WorkItem;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// The planning of a read for a driver outside this package: the work list of a single file and
/// each work item's stages, which [Planner] decides and the driver runs one at a time, in
/// work-list order. Every byte planning reads comes from the driver's [Sections].
public final class EnginePlanning {

    private final FileMetadataCache files;
    private final Planner planner;
    private final List<Planning> plannings = new ArrayList<>();

    public EnginePlanning(FileMetadataCache files, Sections sections, HardwoodContextImpl context,
                          FileSchema referenceSchema, List<RowGroup> firstFileRowGroups, ReadProjection projection,
                          int maxSpan, long indexWindowBytes, WorkingSet workingSet) {
        this.files = files;
        SchemaCompatibility compatibility = new SchemaCompatibility(referenceSchema, projection.decoded(), null);
        compatibility.validateReferenceColumns(files.inputFiles().getFirst().name());
        this.planner = new Planner(sections, referenceSchema, firstFileRowGroups, projection, null, compatibility,
                new RowGroupPruning(context, null, true),
                new FetchPlanBuilder(projection, null, true, context, workingSet, maxSpan),
                0, 0, 0, indexWindowBytes);
    }

    /// Lists the work items of the read's one file, whose footer the reader has loaded, and
    /// returns how many there are.
    public int list() throws IOException {
        Planner.Listed listed = planner.planRowGroups(0, files.getFile(0), 0);
        if (listed.event() != null) {
            listed.event().commit();
        }
        plannings.addAll(listed.plannings());
        return plannings.size();
    }

    public void setBatchPlan(BatchPlan batchPlan) {
        planner.setBatchPlan(batchPlan);
    }

    /// Takes work item `index` through its stages as far as the bytes the read holds allow:
    /// `null` once its plans are made, else what the next stage lacks. Run again once it has
    /// arrived; never on two threads at once.
    public Need plan(int index) throws IOException {
        return planner.plans(plannings.get(index));
    }

    public WorkItem workItem(int index) {
        return plannings.get(index).workItem;
    }

    /// The fetch plan of each decoded column of work item `index`, once planned.
    public FetchPlan[] plans(int index) {
        return plannings.get(index).plans.columns();
    }

    /// The batches of work item `index`, once planned.
    public BatchPlan.RowGroupBatches batches(int index) {
        return plannings.get(index).plans.batches();
    }

    /// Releases what work item `index`'s planning holds, once every column has passed it.
    public void release(int index) {
        Planning planning = plannings.set(index, null);
        planning.reads.release();
        planning.workItem.indexWindow().release(index);
    }

    /// Releases what every work item not released holds, as the read closes.
    public void discard() {
        for (int i = 0; i < plannings.size(); i++) {
            Planning planning = plannings.set(i, null);
            if (planning != null) {
                planning.workItem.indexWindow().releaseAll();
                planning.reads.discard();
            }
        }
    }
}
