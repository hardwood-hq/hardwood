/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.plan.BatchPlan.RowGroupBatches;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies the batch boundaries [BatchPlan] decides for a read, row group by row group, by
/// replaying the planned segments the way a column places them.
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

    /// A read plans a row group's batches with its fetch plans, as it reaches it, so a later file
    /// is planned only when the read gets there and continues the plan of the files before it.
    /// Two 100-row row groups of a binary column, read twice over, close a batch at every row group.
    @Test
    void aReadPlansEachRowGroupAsItReachesIt() throws Exception {
        List<Integer> batches = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.openAll(
                List.of(InputFile.of(DICT_CROSS_CHUNK), InputFile.of(DICT_CROSS_CHUNK)));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all()).batchSize(64).build()) {
            while (columns.nextBatch()) {
                batches.add(columns.getRecordCount());
            }
        }

        assertThat(batches).containsExactly(64, 36, 64, 36, 64, 36, 64, 36);
    }

    private static final Path DICT_CROSS_CHUNK = Path.of("src/test/resources/dict_cross_chunk.parquet");

    /// Replays row groups' segments as a column places them, describing each batch as
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
