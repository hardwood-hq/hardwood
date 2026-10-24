/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.jfr.RowGroupScannedEvent;
import dev.hardwood.schema.ColumnSchema;

/// Walks the pages of one projected column in one row group without waiting for I/O.
///
/// A walk is a state machine over the bytes present: it locates each page by its header or by the
/// offset index and takes its bytes from the [FetchSequence] section requests that hold them. Where
/// it needs bytes that are not present, [#advance] returns `null` and [#pending] names them; the
/// walk issues no read, keeps the state it reached, a parsed page header included, and the next
/// [#advance] resumes there. A column's fetch step drives the walk: it issues the read of the
/// pending bytes ([ByteNeed#issue]) and yields on it. A walk over a file whose bytes are in memory
/// never stops for bytes.
///
/// Advancing declares [IOException]: a read the walk needs fails it.
public interface PageWalk {

    /// The next page, or `null` when there is none to return: the walk has ended, or the next
    /// page needs bytes that are not present, which [#pending] then returns.
    ///
    /// @throws IOException if a read the walk needs failed
    PageInfo advance() throws IOException;

    /// What the last [#advance] stopped on, or `null` when it returned a page or ended the walk:
    /// bytes not present ([ByteNeed]), or a task of the read ending, before the dictionary page it
    /// reached can be parsed beside the read's other tasks
    /// ([dev.hardwood.internal.reader.memory.WorkingSet#admit]).
    Need pending();

    /// Which page of the column chunk this walk is on: an ordinal,
    /// [ExceptionContext#DICTIONARY_PAGE], or [ExceptionContext#UNKNOWN_PAGE].
    ///
    /// Read by the fetcher when a page is handed on and when one is not — a header that
    /// will not parse fails before any [PageInfo] exists.
    ///
    /// @return the page this walk is on
    default int currentPage() {
        return ExceptionContext.UNKNOWN_PAGE;
    }

    /// Reports a walk of `column` in row group `rowGroupIndex` of `file` that has handed out or
    /// scanned its last page: `pageCount` pages, located by `strategy`.
    static void scanned(String file, int rowGroupIndex, ColumnSchema column, int pageCount, String strategy) {
        RowGroupScannedEvent event = new RowGroupScannedEvent();
        event.file = file;
        event.rowGroupIndex = rowGroupIndex;
        event.column = column.name();
        event.pageCount = pageCount;
        event.scanStrategy = strategy;
        event.commit();
    }

    /// A column the filter left with no pages in this row group.
    static PageWalk empty() {
        return new PageWalk() {
            @Override
            public PageInfo advance() {
                return null;
            }

            @Override
            public ByteNeed pending() {
                return null;
            }
        };
    }
}
