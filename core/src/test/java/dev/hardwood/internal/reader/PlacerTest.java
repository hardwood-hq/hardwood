/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Placement of pages into the cells of a batch plan, against a sink that records what the
/// placer decides: pages of a column without repetition from their metadata, and measured pages
/// of a column with repetition.
class PlacerTest {

    private static final String FILE_A = "a.parquet";
    private static final String FILE_B = "b.parquet";

    @Test
    void aFlatPageSplitsIntoOneSegmentPerBatch() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 10);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        assertThat(placer.place(flatPage(rowGroup, 10))).isTrue();

        assertThat(sink.segments).containsExactly(
                "cell 0: records [0, 4) values [0, 4)",
                "cell 1: records [4, 8) values [4, 8)",
                "cell 2: records [8, 10) values [8, 10)");
        // The plan leaves the last batch open for a next row group.
        assertThat(sink.sealed).containsExactly("cell 0: 4 records, 4 values", "cell 1: 4 records, 4 values");

        placer.end();

        assertThat(sink.sealed).containsExactly("cell 0: 4 records, 4 values", "cell 1: 4 records, 4 values",
                "cell 2: 2 records, 2 values");
        assertThat(sink.endRead).isTrue();
    }

    @Test
    void aBatchContinuesOnTheNextPage() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 6);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        placer.place(flatPage(rowGroup, 3));
        placer.place(flatPage(rowGroup, 3));

        assertThat(sink.segments).containsExactly(
                "cell 0: records [0, 3) values [0, 3)",
                "cell 0: records [0, 1) values [0, 1)",
                "cell 1: records [1, 3) values [1, 3)");
        assertThat(sink.opened).containsExactly("cell 0: planned 4, from row 0", "cell 1: planned unknown, from row 4");
    }

    @Test
    void aCellOpensAtTheReadsRowOfItsFirstRecord() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches first = plan.plan(0, 0, FILE_A, false, 4);
        BatchPlan.RowGroupBatches second = plan.plan(1, 0, FILE_A, false, 6);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        placer.place(flatPage(first, 4));
        assertThat(placer.nextRow()).isEqualTo(4);
        placer.place(flatPage(second, 6));

        assertThat(sink.opened).containsExactly("cell 0: planned 4, from row 0", "cell 1: planned 4, from row 4",
                "cell 2: planned unknown, from row 8");
        assertThat(placer.nextRow()).isEqualTo(10);
    }

    @Test
    void theRowCapEndsTheReadInsideAPage() {
        BatchPlan plan = new BatchPlan(4, false, false, 6);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 10);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        assertThat(placer.place(flatPage(rowGroup, 10))).isFalse();

        assertThat(sink.sealed).containsExactly("cell 0: 4 records, 4 values", "cell 1: 2 records, 2 values");
        assertThat(sink.endRead).isTrue();
        assertThat(placer.place(flatPage(rowGroup, 10))).as("nothing is placed past the end").isFalse();
        assertThat(sink.segments).hasSize(2);
    }

    /// A page beginning inside a record continues the last record placed, in the cell that holds
    /// it, and that cell is sealed only at the next record start.
    @Test
    void leadingValuesContinueTheLastRecordsBatch() {
        BatchPlan plan = new BatchPlan(2, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 4);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 2);

        placer.place(nestedPage(rowGroup), measure(0, 1, 0, 1, 0), true);

        assertThat(sink.sealed).containsExactly("cell 0: 2 records, 4 values");

        placer.place(nestedPage(rowGroup), measure(1, 1, 0, 1), true);

        assertThat(sink.segments).containsExactly(
                "cell 0: records [0, 2) values [0, 4)",
                "cell 1: records [2, 3) values [4, 5)",
                "cell 1: records [0, 0) values [0, 2)",
                "cell 1: records [0, 1) values [2, 4)");
        assertThat(sink.sealed).as("the last record may continue on a later page").hasSize(1);

        placer.end();

        assertThat(sink.sealed).containsExactly("cell 0: 2 records, 4 values", "cell 1: 2 records, 5 values");
    }

    @Test
    void aChunkBeginningInsideARecordFails() {
        BatchPlan plan = new BatchPlan(2, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 1);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 2);

        assertThatThrownBy(() -> placer.place(nestedPage(rowGroup), measure(1, 0), true))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: first repetition level must be 0 but was 1");
    }

    /// The check runs per column chunk: a later row group's chunk beginning inside a record fails
    /// too, rather than continuing the previous chunk's last record.
    @Test
    void everyChunkMustBeginWithARecord() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches first = plan.plan(0, 0, FILE_A, false, 1);
        BatchPlan.RowGroupBatches second = plan.plan(1, 0, FILE_A, false, 1);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 4);
        placer.place(nestedPage(first), measure(0, 1), true);

        assertThatThrownBy(() -> placer.place(nestedPage(second), measure(2, 0), true))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: first repetition level must be 0 but was 2");
    }

    /// The batch the row cap ends waits for its last record, which continues on the next page.
    @Test
    void theRowCapWaitsForTheLastRecordToComplete() {
        BatchPlan plan = new BatchPlan(4, false, false, 1);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 3);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        assertThat(placer.place(nestedPage(rowGroup), measure(0, 1), true)).isTrue();
        assertThat(sink.endRead).isFalse();

        assertThat(placer.place(nestedPage(rowGroup), measure(1, 0, 1), true)).isFalse();

        assertThat(sink.sealed).containsExactly("cell 0: 1 records, 3 values");
        assertThat(sink.endRead).isTrue();
    }

    @Test
    void aFixedSizeListPageStartsARecordEveryKValues() {
        BatchPlan plan = new BatchPlan(2, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 3);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 2);
        PageDecoder.InPlaceNestedPage page = new PageDecoder.InPlaceNestedPage(9, null, null, 3, null, null, null);

        placer.place(task(rowGroup, PageInfo.UNKNOWN_RECORDS),
                PageMeasure.of(page, PageInfo.UNKNOWN_RECORDS, new PageDecoder.LevelScratch()), true);
        placer.end();

        assertThat(sink.segments).containsExactly(
                "cell 0: records [0, 2) values [0, 6)",
                "cell 1: records [2, 3) values [6, 9)");
    }

    /// A measured page of a column without repetition is placed one record per value, and its
    /// batches are sealed as soon as their last planned record is placed.
    @Test
    void aMeasuredPageWithoutRepetitionSealsAtItsBatchesLastRecord() {
        BatchPlan plan = new BatchPlan(2, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 3);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 2);
        PageDecoder.InPlaceNestedPage page = new PageDecoder.InPlaceNestedPage(3, null, null, 0, null, null, null);

        placer.place(task(rowGroup, 3), PageMeasure.of(page, 3, new PageDecoder.LevelScratch()), false);

        assertThat(sink.sealed).containsExactly("cell 0: 2 records, 2 values");
        assertThat(sink.segments).containsExactly(
                "cell 0: records [0, 2) values [0, 2)",
                "cell 1: records [2, 3) values [2, 3)");
    }

    @Test
    void aBatchOpenAcrossFilesClosesBeforeTheNextFile() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches first = plan.plan(0, 0, FILE_A, false, 3);
        BatchPlan.RowGroupBatches second = plan.plan(1, 1, FILE_B, false, 3);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        placer.place(flatPage(first, 3));
        assertThat(sink.sealed).isEmpty();
        placer.place(flatPage(second, 3));

        assertThat(sink.sealed).containsExactly("cell 0: 3 records, 3 values");
        assertThat(sink.cells.get(0).fileName).isEqualTo(FILE_A);
        assertThat(sink.cells.get(1).fileName).isEqualTo(FILE_B);
    }

    @Test
    void aSkippedRowGroupPlacesNothingAndClosesTheOpenBatch() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches first = plan.plan(0, 0, FILE_A, false, 3);
        BatchPlan.RowGroupBatches skipped = plan.plan(1, 1, FILE_B, false, 2);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);

        placer.place(flatPage(first, 3));
        assertThat(placer.place(marker(skipped))).isTrue();

        assertThat(sink.sealed).containsExactly("cell 0: 3 records, 3 values");
        assertThat(sink.opened).hasSize(1);
        assertThat(placer.nextRow()).as("the skipped row group's rows count").isEqualTo(5);
    }

    @Test
    void aBatchMayNotSpanARowGroupTheColumnIsNotReadIn() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches first = plan.plan(0, 0, FILE_A, false, 3);
        BatchPlan.RowGroupBatches skipped = plan.plan(1, 0, FILE_A, false, 2);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 4);
        placer.place(flatPage(first, 3));

        assertThatThrownBy(() -> placer.place(marker(skipped)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan mismatch: a batch spans a row group the column is not read in and one it is");
    }

    @Test
    void aColumnHoldingMoreRowsThanPlannedFails() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 4);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 4);

        assertThatThrownBy(() -> placer.place(flatPage(rowGroup, 5)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan mismatch: the column holds more than the 4 rows the plan takes from the row group");
    }

    @Test
    void aColumnHoldingFewerRowsThanPlannedFails() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 6);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 4);
        placer.place(flatPage(rowGroup, 5));

        assertThatThrownBy(placer::end)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan mismatch: the column holds 5 of the 6 rows the plan takes from the row group");
    }

    @Test
    void aRowGroupOfAnotherCapacityFails() {
        BatchPlan plan = new BatchPlan(8, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 6);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 4);

        assertThatThrownBy(() -> placer.place(flatPage(rowGroup, 6)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan mismatch: the plan's batch capacity is 8, the column's 4");
    }

    @Test
    void aRowGroupPastTheRowCapFails() {
        BatchPlan plan = new BatchPlan(4, false, false, 2);
        plan.plan(0, 0, FILE_A, false, 2);
        BatchPlan.RowGroupBatches past = plan.plan(1, 0, FILE_A, false, 2);
        Placer<TestCell> placer = new Placer<>(new RecordingSink(), 4);

        assertThatThrownBy(() -> placer.place(nestedPage(past), measure(0), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch plan mismatch: the column reached a row group past the row cap that ended the read");
    }

    @Test
    void aFailureBetweenWorkItemsSealsThePartialBatch() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 3);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);
        placer.place(flatPage(rowGroup, 3));

        placer.sealOpenCell();

        assertThat(sink.sealed).containsExactly("cell 0: 3 records, 3 values");
        assertThat(sink.endRead).isFalse();
    }

    @Test
    void aFailureInsideAWorkItemAbandonsThePartialBatch() {
        BatchPlan plan = new BatchPlan(4, false, false, 0);
        BatchPlan.RowGroupBatches rowGroup = plan.plan(0, 0, FILE_A, false, 6);
        RecordingSink sink = new RecordingSink();
        Placer<TestCell> placer = new Placer<>(sink, 4);
        placer.place(flatPage(rowGroup, 6));

        TestCell abandoned = placer.abandonOpenCell();

        assertThat(abandoned.name).isEqualTo("cell 1");
        assertThat(sink.sealed).containsExactly("cell 0: 4 records, 4 values");
        assertThat(placer.place(flatPage(rowGroup, 1))).as("nothing is placed after it").isFalse();
        assertThat(placer.abandonOpenCell()).isNull();
    }

    // ==================== Helpers ====================

    /// A page of a column without repetition, its `records` records given by its metadata.
    private static PageTask flatPage(BatchPlan.RowGroupBatches batches, int records) {
        return task(batches, records);
    }

    /// A page of a column with repetition, placed through its measure.
    private static PageTask nestedPage(BatchPlan.RowGroupBatches batches) {
        return task(batches, PageInfo.UNKNOWN_RECORDS);
    }

    /// The measure of a page of a column with repetition whose levels are `repetitionLevels`.
    private static PageMeasure measure(int... repetitionLevels) {
        PageDecoder.InPlaceNestedPage page = new PageDecoder.InPlaceNestedPage(repetitionLevels.length, null,
                repetitionLevels, 0, null, null, null);
        return PageMeasure.of(page, PageInfo.UNKNOWN_RECORDS, new PageDecoder.LevelScratch());
    }

    private static PageTask task(BatchPlan.RowGroupBatches batches, int records) {
        PageInfo info = new PageInfo(null, null, null, null, PageRowMask.ALL, records);
        return new PageTask(info, batches.fileName(), batches.workItemIndex(), 0, batches, null, 0);
    }

    private static PageTask marker(BatchPlan.RowGroupBatches batches) {
        return new PageTask(PageInfo.BOUNDARY_MARKER, batches.fileName(), batches.workItemIndex(), 0, batches, null, 0);
    }

    /// A cell as the placer builds it, named by the order it was opened in.
    private static final class TestCell {

        final String name;
        final String fileName;
        int records;
        int values;

        TestCell(String name, String fileName) {
            this.name = name;
            this.fileName = fileName;
        }
    }

    /// Records what the placer decides.
    private static final class RecordingSink implements Placer.Sink<TestCell> {

        final List<TestCell> cells = new ArrayList<>();
        final List<String> opened = new ArrayList<>();
        final List<String> segments = new ArrayList<>();
        final List<String> sealed = new ArrayList<>();
        boolean endRead;

        @Override
        public TestCell open(String fileName, boolean alwaysMatches, int plannedRecords, int capacity, long firstRow) {
            TestCell cell = new TestCell("cell " + cells.size(), fileName);
            cells.add(cell);
            opened.add(cell.name + ": planned " + (plannedRecords == Cell.UNKNOWN_RECORDS ? "unknown" : plannedRecords)
                    + ", from row " + firstRow);
            return cell;
        }

        @Override
        public void add(TestCell cell, PageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd) {
            segments.add(cell.name + ": records [" + recordStart + ", " + recordEnd + ") values [" + valueStart + ", "
                    + valueEnd + ")");
            cell.records += recordEnd - recordStart;
            cell.values += valueEnd - valueStart;
        }

        @Override
        public void sealed(TestCell cell) {
            sealed.add(cell.name + ": " + cell.records + " records, " + cell.values + " values");
        }

        @Override
        public void endRead() {
            endRead = true;
        }
    }
}
