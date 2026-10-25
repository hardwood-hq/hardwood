/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/// The walking stage: every column's next page, the requests whose bytes have arrived, the pages
/// walked and not yet handed on, and the requests the walks stopped on. Its share of the engine's
/// poll, [#poll], advances each column's walk over the pages whose bytes have arrived, as
/// Hardwood's walks parse page headers from the bytes present; it reads no other stage. What it
/// gives the others, through the poll, is the pages walked ([#takeWalked]) for decoding, and the
/// requests it stopped on ([#stoppedOn]) for fetching.
///
/// Hardwood runs a walk's advance as a job on the pool; here it runs inside the poll, since what
/// it does to the other stages is the same.
final class Walking {

    private final List<PageCursor> cursors;
    private final Set<Request> arrived = new HashSet<>();
    private final List<Page> walked = new ArrayList<>();
    private final Set<Request> stoppedOn = new LinkedHashSet<>();

    Walking(List<List<Page>> layout) {
        this.cursors = layout.stream().map(PageCursor::new).toList();
    }

    // --- its share of the poll ---

    /// Advances every column's walk over the pages planned (below `plannedRows`, held by
    /// `planned`) and wanted (below `demand.want()`) whose bytes have all arrived. A walk whose
    /// next page lacks bytes stops on the first request missing.
    void poll(Demand demand, long plannedRows, List<Request> planned) {
        stoppedOn.clear();
        for (PageCursor cursor : cursors) {
            Page page;
            while ((page = cursor.next()) != null && page.firstRow() < plannedRows
                    && page.firstRow() < demand.want()) {
                Request missing = firstMissing(page, planned);
                if (missing != null) {
                    stoppedOn.add(missing);
                    break;
                }
                cursor.walked(page);
                walked.add(page);
            }
        }
    }

    /// The first request holding bytes of `page` that has not arrived, `null` if every one has.
    private Request firstMissing(Page page, List<Request> planned) {
        for (Request request : planned) {
            if (page.overlaps(request.offset(), request.end()) && !arrived.contains(request)) {
                return request;
            }
        }
        return null;
    }

    /// The pages walked since the last take, for decoding.
    List<Page> takeWalked() {
        List<Page> taken = new ArrayList<>(walked);
        walked.clear();
        return taken;
    }

    /// The requests the walks stopped on in the last poll, for fetching to start at once.
    Set<Request> stoppedOn() {
        return stoppedOn;
    }

    // --- facts ---

    void arrived(Request request) {
        arrived.add(request);
    }

    @Override
    public String toString() {
        return "walking[stopped=" + stoppedOn + "]";
    }
}
