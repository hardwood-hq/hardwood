/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import dev.hardwood.internal.reader.decode.PageInfo;

/// The plan of a column that is not read in a row group: it fetches nothing and yields
/// one [PageInfo#BOUNDARY_MARKER]. The column places no rows for the row
/// group; the marker only makes it close its batch in progress at the row where its
/// siblings close theirs.
///
/// Planned for a column the predicate references and the projection does not, in a row
/// group statistics proved to match the filter in full, whose batches no consumer takes
/// from that column. See `_designs/RECORD_FILTERING.md`.
public final class SkippedColumnFetchPlan implements FetchPlan {

    public static final SkippedColumnFetchPlan INSTANCE = new SkippedColumnFetchPlan();

    private SkippedColumnFetchPlan() {
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public PageWalk walk() {
        return new PageWalk() {
            private boolean yielded;

            @Override
            public PageInfo advance() {
                if (yielded) {
                    return null;
                }
                yielded = true;
                return PageInfo.BOUNDARY_MARKER;
            }

            @Override
            public ByteNeed pending() {
                return null;
            }
        };
    }
}
