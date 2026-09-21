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

/// JFR event emitted once per file when its row groups are filtered by statistics and bloom
/// filters, before any of them is read.
///
/// A row group its dictionaries drop once the read reaches it is counted as kept here and
/// reported by [RowGroupDictionaryFilterEvent].
@Name("dev.hardwood.RowGroupFilter")
@Label("Row Group Filter")
@Category({"Hardwood", "Filter"})
@Description("Row groups filtered by statistics and bloom filters")
@StackTrace(false)
public class RowGroupFilterEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

    @Label("Total Row Groups")
    @Description("Total number of row groups before predicate push-down")
    public int totalRowGroups;

    @Label("Row Groups Kept")
    @Description("Number of row groups statistics and bloom filters kept, including any their dictionaries drop later")
    public int rowGroupsKept;

    @Label("Row Groups Skipped")
    @Description("Number of row groups skipped by statistics and bloom filters")
    public int rowGroupsSkipped;

    @Label("Fully Matching Row Groups")
    @Description("Kept row groups whose statistics prove every row matches the predicate")
    public int rowGroupsFullyMatching;
}
