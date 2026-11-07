/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.io.IOException;
import java.util.List;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.schema.ColumnSchema;

/// One decoded column of a read ([dev.hardwood.internal.reader.engine.RowGroupIterator]): it takes
/// the pages its walk hands out, has them decoded and its sealed cells finished on jobs, and publishes its completed cells in
/// plan order to its [dev.hardwood.internal.reader.batch.BatchExchange]. A flat column
/// ([FlatColumn]) places a page as it is handed out; a column decoded through levels
/// ([NestedColumn]) places it once its job has decoded and measured it.
///
/// Every method runs under the read's lock except [#decode] and [#finishSealed], which run on
/// jobs and touch only the page or the cells they are given. A job's result is applied under the
/// lock ([#decoded], [#completed]).
public sealed interface DecodedColumn permits FlatColumn, NestedColumn {

    /// A page handed to the read to decode: the read's row of its first record, or a lower bound
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

    /// A placement's failure, placed at its page, and the page to decode that holds the segments
    /// placed before it, `null` for none: its cells before the failure still complete.
    final class PlaceFailure extends Exception {

        private final transient Page page;

        PlaceFailure(Exception failure, Page page) {
            super(failure);
            this.page = page;
        }

        public Exception failure() {
            return (Exception) getCause();
        }

        public Page page() {
            return page;
        }
    }

    /// Takes a page of work item `batches`; returns the page to decode, `null` when it has none.
    ///
    /// @throws PlaceFailure if placing a flat page fails
    Page place(PageInfo info, BatchPlan.RowGroupBatches batches, String fileName, int rowGroupIndex, int pageIndex)
            throws PlaceFailure;

    /// Whether the column's placement has ended, at the read's row cap or, for a column decoded
    /// through levels, at a failure of its placement: it takes no further page, and its batches
    /// end with the cells placed.
    boolean placementEnded();

    /// Whether `page`, taken and not decoded, is still to be decoded: a column decoded through
    /// levels decodes no page once its placement has ended, since it places none.
    boolean decodes(Page page);

    /// The pages taken and not yet released: not decoded for a flat column, not placed for a
    /// column decoded through levels. The read bounds them per column.
    int pagesHeld();

    /// Ends the column's pages once the pages taken before are placed: at the read's end, every
    /// cell is sealed and the end queued behind them; at `failure`, where it is not `null`, a
    /// failure met between two work items, the open cell is sealed, or abandoned where the column
    /// holds fewer rows of its row group than the plan takes, with the shortfall attached to
    /// `failure` as a suppressed "Batch plan mismatch", and `failure` queued behind the cells.
    void end(Throwable failure);

    /// Ends the column with the `failure` its walk met inside a work item, once every page it took
    /// before is decoded: behind the cells before its open one, which is abandoned.
    void walkFailed(Throwable failure);

    /// The read's row of the next record the column takes, a lower bound where no metadata gives
    /// it.
    long nextRow();

    /// The read's row of the first record not published.
    long firstUnpublishedRow();

    /// Decodes `page`, on a job; returns the result for [#decoded].
    ///
    /// @throws Exception the failure, placed at the page's file, row group and ordinal
    Object decode(Page page, DecodeBuffers buffers) throws Exception;

    /// Applies a [#decode] result. A column decoded through levels places its measured pages
    /// here; a placement's failure ends the column behind the cells before its open one, which is
    /// abandoned.
    void decoded(Object result);

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

    /// A failure met reading `column`, typed ([#asReadFailure]) and placed with the file, row group
    /// and page it came from. Where a [dev.hardwood.MetadataSource] supplied the footers
    /// (`footersSupplied`), a read failure also says so
    /// ([ExceptionContext#addSuppliedFooterHint]): the checks a supplied footer passes on open do
    /// not catch every file rewritten since the footer was read.
    static Exception placeFailure(Exception e, ColumnSchema column, boolean footersSupplied,
                                  String fileName, int rowGroup, int page) {
        if (HardwoodContextImpl.isContextClosed(e)) {
            // The context's, not the file's, whichever thread of the read met the refusal first
            return e;
        }
        Exception typed = asReadFailure(e);
        if (fileName == null || fileName.isEmpty()) {
            return typed;
        }
        String columnPath = column.fieldPath().toString();
        if (typed instanceof RuntimeException re) {
            RuntimeException placed = ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, re);
            return footersSupplied ? ExceptionContext.addSuppliedFooterHint(placed) : placed;
        }
        if (typed instanceof IOException ioe) {
            // Stays checked. The read carries a failure across its thread boundary as a
            // `Throwable`, so nothing between here and the reader needs it wrapped, and the
            // reader's own signature can declare it.
            return ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, ioe);
        }
        return typed;
    }

    /// Delegates runtime-failure classification; checked transport failures pass through unchanged.
    static Exception asReadFailure(Exception e) {
        return e instanceof RuntimeException runtime
                ? ExceptionContext.asReadFailure(runtime)
                : e;
    }
}
