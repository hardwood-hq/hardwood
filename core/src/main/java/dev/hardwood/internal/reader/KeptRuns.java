/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Where the values a [PageRowMask] keeps of a page read in place lie in the page: runs of
/// consecutive page positions, in page order. Kept position `i` of the page's trimmed view is
/// the `i`th position over the runs.
final class KeptRuns {

    private final int[] pageStarts;
    private final int[] keptStarts;
    private final int[] lengths;
    private final int count;
    private final boolean keepsLastValue;

    private KeptRuns(int[] pageStarts, int[] keptStarts, int[] lengths, int count, boolean keepsLastValue) {
        this.pageStarts = pageStarts;
        this.keptStarts = keptStarts;
        this.lengths = lengths;
        this.count = count;
        this.keepsLastValue = keepsLastValue;
    }

    /// Runs `[pageStarts[i], pageStarts[i] + lengths[i])`, the first `count` of each array, of a
    /// page of `pageSize` values.
    static KeptRuns of(int[] pageStarts, int[] lengths, int count, int pageSize) {
        int[] keptStarts = new int[count];
        int kept = 0;
        for (int i = 0; i < count; i++) {
            keptStarts[i] = kept;
            kept += lengths[i];
        }
        boolean keepsLastValue = count > 0 && pageStarts[count - 1] + lengths[count - 1] == pageSize;
        return new KeptRuns(pageStarts, keptStarts, lengths, count, keepsLastValue);
    }

    int count() {
        return count;
    }

    /// Whether the last run ends at the page's last value: the mask keeps the page's last record.
    boolean keepsLastValue() {
        return keepsLastValue;
    }

    /// The run holding kept position `kept`.
    int runOf(int kept) {
        int lo = 0;
        int hi = count - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (keptStarts[mid] <= kept) {
                lo = mid;
            }
            else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /// The page position of kept position `kept`, which lies in run `run`.
    int pagePosition(int run, int kept) {
        return pageStarts[run] + kept - keptStarts[run];
    }

    /// The kept positions run `run` has left from kept position `kept` on.
    int remaining(int run, int kept) {
        return keptStarts[run] + lengths[run] - kept;
    }

    /// The plain bytes of kept positions `[from, to)`.
    int bytes(BinaryPageValues values, int from, int to) {
        int bytes = 0;
        int kept = from;
        while (kept < to) {
            int run = runOf(kept);
            int n = Math.min(to - kept, remaining(run, kept));
            int page = pagePosition(run, kept);
            bytes += values.bytes(page, page + n);
            kept += n;
        }
        return bytes;
    }
}
