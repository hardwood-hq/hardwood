/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/// JFR event emitted when pages are filtered by Column Index predicate push-down.
///
/// One event per column chunk whose pages the push-down considered, so a row group
/// read with a filter over two projected columns emits two events. `pagesSkipped`
/// of 0 means every page survived the predicate.
///
/// No event at all means no page was a candidate for push-down in that column chunk:
/// the read carries no predicate, the column chunk has no Column Index to evaluate,
/// or the pages were narrowed by something other than a predicate — `tail(N)` skipping
/// the rows before the tail is not page filtering and is not reported here.
///
/// The event covers Column Index push-down only. A column chunk with no Column Index
/// is read through a sequential plan that drops pages from the statistics in each page
/// header as it walks them; those drops are real page filtering and none of them are
/// reported, because that plan has no page total to divide by without first reading
/// every header it set out to avoid.
@Name("dev.hardwood.PageFilter")
@Label("Page Filter")
@Category({"Hardwood", "Filter"})
@Description("Pages filtered by Column Index predicate push-down")
@StackTrace(false)
public class PageFilterEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

    @Label("Predicate")
    @Description("Predicate structure with literal values elided")
    public String predicate;

    @Label("Row Group Index")
    @Description("Index of the row group within the file")
    public int rowGroupIndex;

    @Label("Column")
    @Description("Name of the column being filtered")
    public String column;

    @Label("Total Pages")
    @Description("Total number of data pages before filtering")
    public int totalPages;

    @Label("Pages Kept")
    @Description("Number of pages kept after filtering")
    public int pagesKept;

    @Label("Pages Skipped")
    @Description("Number of pages skipped by the filter")
    public int pagesSkipped;
}
