/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

/// The bytes one decode adds to, or drops from, the arrays of the batch holders it fills: what it
/// draws on the claim its read admitted it with. Counted at the sites that create or let go of a
/// holder's arrays, each of which also counts the bytes into the holder itself
/// ([FlatBatch#chargedBytes()]), so that the read returns them as the holder leaves it.
///
/// Written by the thread running the decode only, and read once the decode has ended.
public final class AllocationTally {

    private long bytes;

    /// Counts `bytes` added to a holder's arrays, or, negative, dropped from them.
    public void add(long bytes) {
        this.bytes += bytes;
    }

    /// The net bytes counted.
    public long bytes() {
        return bytes;
    }

    /// Counts `bytes` into `tally`, a fill's that has none (`null`) counting nothing.
    public static void add(AllocationTally tally, long bytes) {
        if (tally != null) {
            tally.add(bytes);
        }
    }
}
