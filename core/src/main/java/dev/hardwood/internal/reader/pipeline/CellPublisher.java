/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.ArrayDeque;

import dev.hardwood.internal.reader.batch.BatchExchange;

/// The publish order of a column of a read: its cells in plan order, then the read's end, published
/// into the column's [BatchExchange] as each cell at the head completes. Every method runs under
/// the read's lock ([dev.hardwood.internal.reader.engine.RowGroupIterator]).
///
/// @param <B> the batch type
final class CellPublisher<B> {

    private static final Object END = new Object();

    private final BatchExchange<B> exchange;
    /// The cells in plan order, then [#END] once the read has ended.
    private final ArrayDeque<Object> order = new ArrayDeque<>();
    /// The failure the column ends with behind its cells, `null` for the read's end.
    private Throwable endFailure;
    private boolean ended;
    /// Whether a completed cell waits for room in the exchange.
    private boolean waitingForRoom;

    CellPublisher(BatchExchange<B> exchange) {
        this.exchange = exchange;
    }

    BatchExchange<B> exchange() {
        return exchange;
    }

    /// Appends a cell opened at its batch's first record.
    void add(BatchCell<B> cell) {
        order.addLast(cell);
    }

    /// Sets the failure the column ends with once every cell before it is published, `null` for
    /// the read's end.
    void endWith(Throwable failure) {
        endFailure = failure;
    }

    /// Takes out a cell that will not complete, abandoned at a failure.
    void remove(BatchCell<B> cell) {
        order.remove(cell);
    }

    /// Appends the read's end behind every cell.
    void endRead() {
        order.addLast(END);
    }

    /// The first cell not published, `null` when there is none.
    BatchCell<B> head() {
        return order.peekFirst() instanceof BatchCell<?> cell ? cast(cell) : null;
    }

    @SuppressWarnings("unchecked")
    private BatchCell<B> cast(BatchCell<?> cell) {
        return (BatchCell<B>) cell;
    }

    /// The cells not published, and the end where it is queued.
    int size() {
        return order.size();
    }

    /// Publishes the completed cells at the head of the order, as far as the exchange takes them,
    /// and finishes the exchange at the read's end. Also run when the consumer makes room.
    void publish() {
        while (!order.isEmpty()) {
            Object head = order.peekFirst();
            if (head == END) {
                order.pollFirst();
                ended = true;
                if (endFailure != null) {
                    exchange.signalError(endFailure);
                }
                else {
                    exchange.finish();
                }
                return;
            }
            BatchCell<B> cell = cast((BatchCell<?>) head);
            if (!cell.isCompleted()) {
                return;
            }
            waitingForRoom = !exchange.tryPublish(cell.batch());
            if (waitingForRoom) {
                return;
            }
            order.pollFirst();
            cell.release();
        }
    }

    boolean waitingForRoom() {
        return waitingForRoom;
    }

    boolean ended() {
        return ended;
    }

    /// Stops publishing as the read closes; a consumer waiting for a batch finds the end.
    void stop() {
        if (!ended) {
            ended = true;
            order.clear();
            exchange.finish();
        }
    }

    /// Ends the column with `failure` at once.
    void fail(Throwable failure) {
        ended = true;
        order.clear();
        exchange.signalError(failure);
    }
}
