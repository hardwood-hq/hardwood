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

/// JFR event emitted when row groups are filtered by a predicate push-down filter.
@Name("dev.hardwood.RowGroupFilter")
@Label("Row Group Filter")
@Category({"Hardwood", "Filter"})
@Description("Row groups filtered by predicate push-down")
@StackTrace(false)
public class RowGroupFilterEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

    @Label("Total Row Groups")
    @Description("Total number of row groups in the file before filtering")
    public int totalRowGroups;

    @Label("Row Groups Kept")
    @Description("Number of row groups kept after filtering")
    public int rowGroupsKept;

    @Label("Row Groups Skipped")
    @Description("Number of row groups skipped by the filter")
    public int rowGroupsSkipped;
}
