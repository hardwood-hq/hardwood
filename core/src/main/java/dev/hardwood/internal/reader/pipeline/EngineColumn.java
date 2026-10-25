/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.List;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.plan.BatchPlan;

/// One decoded column of a read the read engine drives: it takes the pages its walk hands out,
/// has them decoded and its sealed cells finished on jobs, and publishes its completed cells in
/// plan order to its [dev.hardwood.internal.reader.batch.BatchExchange]. A flat column
/// ([EngineFlatColumn]) places a page as it is handed out; a column decoded through levels
/// ([EngineNestedColumn]) places it once its job has decoded and measured it.
///
/// Every method runs under the engine's lock except [#decode] and [#finishSealed], which run on
/// jobs and touch only the page or the cells they are given. A job's result is applied under the
/// lock ([#decoded], [#completed]).
public sealed interface EngineColumn permits EngineFlatColumn, EngineNestedColumn {

    /// A page handed to the engine to decode: the read's row of its first record, or a lower bound
    /// of it where no metadata gives the records before it.
    final class Page {

        final PageTask task;

        Page(PageTask task) {
            this.task = task;
        }

        public long firstRow() {
            return task.firstRow;
        }

        /// The buffers the page's task takes, which the read admits before it starts
        /// ([dev.hardwood.internal.reader.memory.WorkingSet#admit]).
        public long taskBytes() {
            return task.info == null ? 0 : task.info.taskBytes();
        }
    }

    /// Takes a page of work item `batches`; returns the page to decode, `null` when it has none.
    Page place(PageInfo info, BatchPlan.RowGroupBatches batches, String fileName, int rowGroupIndex, int pageIndex);

    /// The pages taken and not yet released: not decoded for a flat column, not placed for a
    /// column decoded through levels. The engine bounds them per column.
    int pagesHeld();

    /// Ends the column's pages: every cell is sealed once the pages taken before are placed, and
    /// the read's end, or `failure` where it is not `null`, is queued behind them.
    void end(Throwable failure);

    /// The read's row of the next record the column takes, a lower bound where no metadata gives
    /// it.
    long nextRow();

    /// The read's row of the first record not published.
    long firstUnpublishedRow();

    /// Decodes `page`, on a job; returns the result for [#decoded].
    ///
    /// @throws Exception the failure, placed at the page's file, row group and ordinal
    Object decode(Page page, DecodeBuffers buffers) throws Exception;

    /// Applies a [#decode] result.
    ///
    /// @throws Exception a placement's failure, placed at the page it was met at
    void decoded(Object result) throws Exception;

    /// The cells sealed and left to finish, for a job ([#finishSealed]).
    List<Object> takeSealed();

    /// Finishes the cells [#takeSealed] took, on a job; returns them for [#completed].
    ///
    /// @throws Exception the failure, placed at the page it was met at
    List<Object> finishSealed(List<Object> cells, DecodeBuffers buffers) throws Exception;

    /// Marks the cells a job completed, and publishes what that lets the column publish.
    void completed(List<Object> cells);

    /// `e`, placed at the column and `fileName`, `rowGroup` and `page` it was met at.
    Exception placed(Exception e, String fileName, int rowGroup, int page);

    /// Publishes the completed cells at the head of the plan order, as far as the exchange takes
    /// them; also run when the consumer makes room.
    void publish();

    /// Whether a completed cell waits for the consumer to make room in the exchange.
    boolean waitingForRoom();

    /// Whether the column has published its last batch and finished.
    boolean ended();

    /// Stops the column as its read closes: nothing more is published, and a consumer waiting for
    /// a batch finds the end.
    void stop();

    /// Ends the column with `failure`.
    void fail(Throwable failure);

    /// The column's state, for a stall report.
    String describe();
}
