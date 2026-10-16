/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.BatchPlan.RowGroupBatches;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies the batch boundaries [BatchPlan] decides for a read, row group by row group, by
/// replaying the planned segments the way a column worker assembles them.
class BatchPlanTest {

    @Test
    void batchesCloseAtCapacityAcrossRowGroupsOfOneFile() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);

        RowGroupBatches first = plan.plan(0, 0, "a", false, 10);
        RowGroupBatches second = plan.plan(1, 0, "a", false, 3);

        assertThat(first.segmentCount()).isEqualTo(3);
        assertThat(first.lastOpen()).isTrue();
        assertThat(second.closesOpenBatch()).isFalse();
        assertThat(replay(first, second)).containsExactly("a:4", "a:4", "a:4", "a:1");
    }

    @Test
    void aBatchClosesBeforeTheFirstRowOfAnotherFile() {
        BatchPlan plan = new BatchPlan(10, false, false, 0);

        RowGroupBatches first = plan.plan(0, 0, "a", false, 3);
        RowGroupBatches second = plan.plan(1, 1, "b", false, 3);

        assertThat(second.closesOpenBatch()).isTrue();
        assertThat(replay(first, second)).containsExactly("a:3", "b:3");
    }

    /// The file is told apart by its index in the read, so a path listed twice in a row still
    /// starts a batch.
    @Test
    void theSameFileNameListedTwiceStillClosesTheBatch() {
        BatchPlan plan = new BatchPlan(10, false, false, 0);

        assertThat(replay(plan.plan(0, 0, "a", false, 3), plan.plan(1, 1, "a", false, 3)))
                .containsExactly("a:3", "a:3");
    }

    @Test
    void readsEndingBatchesAtRowGroupBoundariesCloseEveryBatchAtItsRowGroupsEnd() {
        BatchPlan plan = new BatchPlan(10, true, false, 0);

        RowGroupBatches first = plan.plan(0, 0, "a", false, 3);
        RowGroupBatches second = plan.plan(1, 0, "a", false, 12);

        assertThat(first.lastOpen()).isFalse();
        assertThat(second.closesOpenBatch()).isFalse();
        assertThat(replay(first, second)).containsExactly("a:3", "a:10", "a:2");
    }

    @Test
    void aFilteredReadKeepsBatchesHomogeneousInTheAlwaysMatchFlag() {
        BatchPlan plan = new BatchPlan(10, false, true, 0);

        RowGroupBatches proven = plan.plan(0, 0, "a", true, 3);
        RowGroupBatches provenToo = plan.plan(1, 0, "a", true, 2);
        RowGroupBatches undecided = plan.plan(2, 0, "a", false, 4);
        RowGroupBatches provenAgain = plan.plan(3, 0, "a", true, 1);

        assertThat(provenToo.closesOpenBatch()).isFalse();
        assertThat(undecided.closesOpenBatch()).isTrue();
        assertThat(provenAgain.closesOpenBatch()).isTrue();
        assertThat(replay(proven, provenToo, undecided, provenAgain))
                .containsExactly("a:5:proven", "a:4", "a:1:proven");
    }

    @Test
    void anUnfilteredReadIgnoresTheAlwaysMatchFlag() {
        BatchPlan plan = new BatchPlan(10, false, false, 0);

        assertThat(replay(plan.plan(0, 0, "a", true, 3), plan.plan(1, 0, "a", false, 4),
                plan.plan(2, 0, "a", true, 3)))
                .containsExactly("a:10");
    }

    /// The rows a row group keeps under its page masks are what the plan places, and a row group
    /// that keeps none takes no part: it neither closes the open batch nor drops the cap.
    @Test
    void maskedRowGroupsContributeTheirKeptRows() {
        BatchPlan plan = new BatchPlan(10, false, true, 4);

        RowGroupBatches proven = plan.plan(0, 0, "a", true, 2);
        RowGroupBatches emptied = plan.plan(1, 0, "a", false, 0);
        RowGroupBatches provenAgain = plan.plan(2, 0, "a", true, 5);

        assertThat(emptied.segmentCount()).isZero();
        assertThat(emptied.closesOpenBatch()).isFalse();
        assertThat(provenAgain.closesOpenBatch()).isFalse();
        assertThat(provenAgain.endsRead()).isTrue();
        assertThat(provenAgain.plannedRows()).isEqualTo(2);
        assertThat(replay(proven, emptied, provenAgain)).containsExactly("a:4:proven");
    }

    @Test
    void theRowCapEndsTheRead() {
        BatchPlan plan = new BatchPlan(4, false, false, 7);

        RowGroupBatches first = plan.plan(0, 0, "a", false, 5);
        RowGroupBatches second = plan.plan(1, 0, "a", false, 5);
        RowGroupBatches third = plan.plan(2, 0, "a", false, 5);

        assertThat(first.endsRead()).isFalse();
        assertThat(second.endsRead()).isTrue();
        assertThat(second.plannedRows()).isEqualTo(2);
        assertThat(second.keptRows()).isEqualTo(5);
        assertThat(third.workItemIndex()).as("no column reaches a row group past the cap").isEqualTo(-1);
        assertThat(replay(first, second)).containsExactly("a:4", "a:3");
    }

    /// Under a filter the cap counts matching rows, so it holds only while every row group with
    /// rows was proven to match, and the first undecided one drops it for the rest of the read.
    @Test
    void aFilteredReadDropsTheCapAtTheFirstUndecidedRowGroup() {
        BatchPlan plan = new BatchPlan(10, false, true, 5);

        RowGroupBatches proven = plan.plan(0, 0, "a", true, 3);
        RowGroupBatches undecided = plan.plan(1, 0, "a", false, 4);
        RowGroupBatches provenAgain = plan.plan(2, 0, "a", true, 10);

        assertThat(provenAgain.endsRead()).isFalse();
        assertThat(replay(proven, undecided, provenAgain))
                .containsExactly("a:3:proven", "a:4", "a:10:proven");
    }

    @Test
    void theCapEndsABatchThatSpansRowGroups() {
        BatchPlan plan = new BatchPlan(10, false, false, 5);

        RowGroupBatches first = plan.plan(0, 0, "a", false, 3);
        RowGroupBatches second = plan.plan(1, 0, "a", false, 4);

        assertThat(second.closesOpenBatch()).isFalse();
        assertThat(second.endsRead()).isTrue();
        assertThat(replay(first, second)).containsExactly("a:5");
    }

    /// Row groups are planned one at a time, in work-list order, as the read reaches them: a file
    /// planned later continues the plan where the files before it left it.
    @Test
    void rowGroupsArePlannedInWorkListOrder() {
        BatchPlan plan = new BatchPlan(10, false, false, 0);
        plan.plan(0, 0, "a", false, 3);

        assertThatThrownBy(() -> plan.plan(2, 1, "b", false, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan expects work item 1 next, got 2");
        assertThatThrownBy(() -> plan.plan(0, 0, "a", false, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan expects work item 1 next, got 0");
    }

    /// A filter-only column is not read in a proven row group, and stands in for it with a
    /// boundary marker. The plan closes every batch of undecided rows before a proven row group,
    /// so no batch mixes rows such a column holds with rows it does not.
    @Test
    void provenRowGroupsNeverShareABatchWithUndecidedOnes() {
        BatchPlan plan = new BatchPlan(10, false, true, 0);

        RowGroupBatches undecided = plan.plan(0, 0, "a", false, 3);
        RowGroupBatches proven = plan.plan(1, 0, "a", true, 3);
        RowGroupBatches provenToo = plan.plan(2, 0, "a", true, 3);
        RowGroupBatches undecidedAgain = plan.plan(3, 0, "a", false, 3);

        assertThat(proven.closesOpenBatch()).isTrue();
        assertThat(provenToo.closesOpenBatch()).isFalse();
        assertThat(undecidedAgain.closesOpenBatch()).isTrue();
    }

    @Test
    void segmentsDescribeTheirRows() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        plan.plan(0, 0, "a", false, 1);

        RowGroupBatches batches = plan.plan(1, 0, "a", false, 12);

        assertThat(batches.segmentRows(0)).isEqualTo(3);
        assertThat(batches.segmentRows(1)).isEqualTo(4);
        assertThat(batches.segmentRows(2)).isEqualTo(4);
        assertThat(batches.segmentRows(3)).isEqualTo(1);
        assertThat(batches.rowsBefore(3)).isEqualTo(11);
        assertThat(batches.closes(2)).isTrue();
        assertThat(batches.closes(3)).isFalse();
        assertThat(batches.plannedRows()).isEqualTo(12);
    }

    @Test
    void rejectsANonPositiveCapacity() {
        assertThatThrownBy(() -> new BatchPlan(0, false, false, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Batch capacity must be positive, got 0");
    }

    /// The iterator plans a row group's batches with its fetch plans, as the read reaches it, so a
    /// later file is planned only when the read gets there and continues the plan of the files
    /// before it. Two 100-row row groups of a binary column, read twice over, close a batch at
    /// every row group, and the second file's batches are attributed to it.
    @Test
    void theIteratorPlansEachRowGroupAsTheReadReachesIt() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = iterator(context, DICT_CROSS_CHUNK, DICT_CROSS_CHUNK)) {
            iterator.planBatches(64, 0);

            List<String> batches = new ArrayList<>();
            List<RowGroupBatches> rowGroups = new ArrayList<>();
            for (int i = 0; ; i++) {
                RowGroupIterator.WorkItem workItem = iterator.workItemAt(i);
                if (workItem == null) {
                    break;
                }
                iterator.getColumnPlan(workItem, 0);
                RowGroupBatches rowGroup = iterator.getBatches(workItem);
                assertThat(rowGroup.workItemIndex()).isEqualTo(i);
                rowGroups.add(rowGroup);
                iterator.releaseWorkItem(workItem);
            }
            batches.addAll(replay(rowGroups.toArray(new RowGroupBatches[0])));

            String name = DICT_CROSS_CHUNK.getFileName().toString();
            assertThat(batches).containsExactly(name + ":64", name + ":36", name + ":64", name + ":36",
                    name + ":64", name + ":36", name + ":64", name + ":36");
        }
    }

    @Test
    void theBatchesArePlannedOnceBeforeAnyFetchPlan() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = iterator(context, DICT_CROSS_CHUNK)) {
            iterator.planBatches(64, 0);

            assertThatThrownBy(() -> iterator.planBatches(64, 0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("planBatches must be called once, before any column requests its fetch plan");
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = iterator(context, DICT_CROSS_CHUNK)) {
            RowGroupIterator.WorkItem workItem = iterator.workItemAt(0);
            iterator.getColumnPlan(workItem, 0);

            assertThatThrownBy(() -> iterator.planBatches(64, 0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("planBatches must be called once, before any column requests its fetch plan");
            assertThatThrownBy(() -> iterator.getBatches(workItem))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No batch plan: planBatches() was not called for this read");
        }
    }

    /// A worker whose column holds more rows of a row group than the plan takes fails instead of
    /// publishing a batch out of line with its siblings.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aColumnHoldingMoreRowsThanPlannedFails() throws Exception {
        assertThatThrownBy(() -> drainAgainst(new BatchPlan(64, true, false, 0)
                .plan(0, 0, DICT_CROSS_CHUNK.getFileName().toString(), false, 90)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[dict_cross_chunk.parquet: row group 0, column 'label', page 0] "
                        + "Batch plan mismatch: the column holds more than the 90 rows the plan takes from the row group");
    }

    /// A worker whose column holds fewer rows of a row group than the plan takes fails when it
    /// leaves the row group.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aColumnHoldingFewerRowsThanPlannedFails() throws Exception {
        assertThatThrownBy(() -> drainAgainst(new BatchPlan(64, true, false, 0)
                .plan(0, 0, DICT_CROSS_CHUNK.getFileName().toString(), false, 120)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[dict_cross_chunk.parquet: row group 1, column 'label', page 0] "
                        + "Batch plan mismatch: the column holds 100 of the 120 rows the plan takes from the row group");
    }

    /// A filter-only column reads a boundary marker for a proven row group. Where the row cap
    /// ends the read inside that row group, the column's worker ends its stream at the marker,
    /// publishing no batch, and assembles none of the pages that follow.
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aFilterOnlyColumnEndsItsStreamWhereTheCapEndsTheReadInASkippedRowGroup() throws Exception {
        RowGroupBatches capped = new BatchPlan(64, true, true, 50).plan(0, 0, "skipped", true, 100);
        assertThat(capped.endsRead()).isTrue();
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = iterator(context, DICT_CROSS_CHUNK)) {
            iterator.planBatches(64, 0);
            ColumnSchema column = iterator.referenceSchema().getColumn(0);
            PageSource source = new PageSource(iterator, 0) {
                private boolean markerYielded;

                @Override
                public PageInfo next() throws IOException {
                    if (!markerYielded) {
                        markerYielded = true;
                        return PageInfo.BOUNDARY_MARKER;
                    }
                    return super.next();
                }

                @Override
                RowGroupBatches getCurrentBatches() {
                    RowGroupBatches planned = super.getCurrentBatches();
                    return planned != null ? planned : capped;
                }
            };
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), 64);
            try (FlatColumnWorker worker = new FlatColumnWorker(source, exchange, column, 64,
                    context.decompressorFactory(), context.executor(), null)) {
                worker.start();

                assertThat(exchange.poll()).as("no batch past the cap").isNull();
                exchange.checkError();
                assertThat(worker.isFinished()).isTrue();
            }
        }
    }

    private static final Path DICT_CROSS_CHUNK = Path.of("src/test/resources/dict_cross_chunk.parquet");

    /// Drains `dict_cross_chunk.parquet`'s column with the first row group's batches replaced by
    /// `firstRowGroup`.
    private static void drainAgainst(RowGroupBatches firstRowGroup) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             RowGroupIterator iterator = iterator(context, DICT_CROSS_CHUNK)) {
            iterator.planBatches(64, 0);
            ColumnSchema column = iterator.referenceSchema().getColumn(0);
            PageSource source = new PageSource(iterator, 0) {
                @Override
                RowGroupBatches getCurrentBatches() {
                    RowGroupBatches planned = super.getCurrentBatches();
                    return planned != null && planned.workItemIndex() == 0 ? firstRowGroup : planned;
                }
            };
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), 64);
            try (FlatColumnWorker worker = new FlatColumnWorker(source, exchange, column, 64,
                    context.decompressorFactory(), context.executor(), null)) {
                worker.start();
                while (exchange.poll() != null) {
                    // drained
                }
                exchange.checkError();
            }
        }
    }

    private static RowGroupIterator iterator(HardwoodContextImpl context, Path... files) throws Exception {
        List<InputFile> inputs = new ArrayList<>();
        for (Path file : files) {
            InputFile input = InputFile.of(file);
            input.open();
            inputs.add(input);
        }
        RowGroupIterator iterator = new RowGroupIterator(inputs, context, 0);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(files[0]))) {
            FileSchema schema = reader.getFileSchema();
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
        }
        return iterator;
    }

    /// Replays row groups' segments as a column worker assembles them, describing each batch as
    /// `file:rows`, with `:proven` when its always-match flag is set.
    private static List<String> replay(RowGroupBatches... rowGroups) {
        List<String> batches = new ArrayList<>();
        int open = 0;
        RowGroupBatches openIn = null;
        for (RowGroupBatches rowGroup : rowGroups) {
            if (rowGroup.closesOpenBatch() && open > 0) {
                batches.add(describe(openIn, open));
                open = 0;
            }
            for (int i = 0; i < rowGroup.segmentCount(); i++) {
                open += rowGroup.segmentRows(i);
                openIn = rowGroup;
                if (rowGroup.closes(i)) {
                    batches.add(describe(rowGroup, open));
                    open = 0;
                }
            }
        }
        if (open > 0) {
            batches.add(describe(openIn, open));
        }
        return batches;
    }

    private static String describe(RowGroupBatches rowGroup, int rows) {
        return rowGroup.fileName() + ":" + rows + (rowGroup.alwaysMatches() ? ":proven" : "");
    }
}
