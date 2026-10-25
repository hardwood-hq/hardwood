/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.ArrayList;
import java.util.List;

/// The "file": nothing here is Parquet. A few columns, each cut into pages of its own size, in row
/// groups whose bytes hold one column's chunk after the other; a request's fetch sleeps (I/O), a
/// page's decode spins (CPU) and yields the page's values.
final class SimulatedFile {

    static final long TOTAL_ROWS = 10_000;
    /// per column: page boundaries differ between columns
    static final int[] PAGE_ROWS = { 100, 250, 40 };
    static final int FETCH_MILLIS = 2;
    static final long DECODE_NANOS_PER_ROW = 10_000;
    /// the rows of a row group; no page crosses a row group's end
    static final long ROW_GROUP_ROWS = 2_000;
    /// the CPU a row group's index takes to parse
    static final long PARSE_INDEX_NANOS = 200_000;
    /// a page's bytes per row
    static final int BYTES_PER_ROW = 8;

    private SimulatedFile() {
    }

    static List<List<Page>> layout() {
        return layout(TOTAL_ROWS, ROW_GROUP_ROWS, PAGE_ROWS);
    }

    /// Each column's pages, of `pageRows[c]` rows for column `c`, in row groups of `rowGroupRows`:
    /// within a row group, column 0's chunk first, then column 1's, and so on. A page ends at its
    /// row group's end.
    static List<List<Page>> layout(long totalRows, long rowGroupRows, int... pageRows) {
        List<List<Page>> columns = new ArrayList<>();
        for (int c = 0; c < pageRows.length; c++) {
            columns.add(new ArrayList<>());
        }
        long offset = 0;
        for (long start = 0; start < totalRows; start += rowGroupRows) {
            long end = Math.min(start + rowGroupRows, totalRows);
            for (int c = 0; c < pageRows.length; c++) {
                List<Page> pages = columns.get(c);
                for (long row = start; row < end; row += pageRows[c]) {
                    int rows = Math.toIntExact(Math.min(pageRows[c], end - row));
                    pages.add(new Page(c, pages.size(), row, rows, offset, rows * BYTES_PER_ROW));
                    offset += (long) rows * BYTES_PER_ROW;
                }
            }
        }
        return columns;
    }

    /// A row group's index, which a read fetches and parses before it may fetch the row group's
    /// pages: as Hardwood's planning reads a row group's page index before its walks.
    static void fetchIndex(int rowGroup) throws InterruptedException {
        Thread.sleep(FETCH_MILLIS);
    }

    static void parseIndex(int rowGroup) {
        spin(PARSE_INDEX_NANOS);
    }

    /// The I/O: a request takes `FETCH_MILLIS`, wherever it runs.
    static void fetch(Request request) throws InterruptedException {
        Thread.sleep(FETCH_MILLIS);
    }

    /// The CPU: occupies its thread for `DECODE_NANOS_PER_ROW` a row, and yields the value of row
    /// `r` in column `c`, `r * (c + 1)`.
    static long[] decode(Page page) {
        spin(page.rows() * DECODE_NANOS_PER_ROW);
        long[] values = new long[page.rows()];
        for (int i = 0; i < values.length; i++) {
            values[i] = (page.firstRow() + i) * (page.column() + 1);
        }
        return values;
    }

    /// Occupies the calling thread for `nanos`.
    private static void spin(long nanos) {
        long until = System.nanoTime() + nanos;
        while (System.nanoTime() < until) {
            Thread.onSpinWait();
        }
    }
}
