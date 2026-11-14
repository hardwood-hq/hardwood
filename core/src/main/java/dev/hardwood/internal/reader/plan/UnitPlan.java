/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.Arrays;
import java.util.List;

import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.select.RowRanges;

/// The plan the read makes for a [UnitListing] on a job, once its page index has arrived: the
/// rows its filter keeps, each decoded column's pages and dictionary page, and the ranges its
/// columns read. Built whole and published as one value; the per-column arrays are not written
/// once built.
///
/// @param matching the rows its filter keeps, by its pages' bounds
/// @param pages per decoded column, its planned pages, `null` for a chunk without an OffsetIndex,
///        which a scan finds
/// @param dictionaryStart per decoded column, where its dictionary page starts, `0` for none to read
/// @param firstDataPage per decoded column, where its first data page lies
/// @param preloaded per decoded column, the dictionary the unit's pruning probe decoded, which its
///        pages decode with instead of reading it again, `null` for none
/// @param dictionaryEnd per decoded column, where such a dictionary's page ends, `0` for none
/// @param headers per decoded column with planned pages, their headers, in `pages`' order, parsed
///        on the plan job where the file holds the page's bytes: `null` for a page it does not
///        hold, which its fetch parses, and for one whose header does not parse
/// @param dictionaryHeaders per decoded column, its dictionary page's header, parsed likewise
/// @param ranges the unit's requests, coalesced across its columns
public record UnitPlan(RowRanges matching, List<NeededPage>[] pages, long[] dictionaryStart,
                long[] firstDataPage, Dictionary[] preloaded, long[] dictionaryEnd,
                PageFormatProbe.HeaderAt[][] headers, PageFormatProbe.HeaderAt[] dictionaryHeaders,
                List<FileRange> ranges) {

    /// The plan of a unit of `columns` decoded columns of which no row is read, its rows kept by
    /// `matching`.
    @SuppressWarnings("unchecked")
    public static UnitPlan empty(int columns, RowRanges matching) {
        List<NeededPage>[] pages = new List[columns];
        Arrays.fill(pages, List.of());
        return new UnitPlan(matching, pages, new long[columns], new long[columns], new Dictionary[columns],
                new long[columns], new PageFormatProbe.HeaderAt[columns][], new PageFormatProbe.HeaderAt[columns],
                List.of());
    }
}
