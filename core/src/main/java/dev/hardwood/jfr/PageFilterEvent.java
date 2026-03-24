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
@Name("dev.hardwood.PageFilter")
@Label("Page Filter")
@Category({"Hardwood", "Filter"})
@Description("Pages filtered by Column Index predicate push-down")
@StackTrace(false)
public class PageFilterEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

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
