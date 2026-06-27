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

/// JFR event emitted when row groups are selected by a byte-range row-group predicate
/// (split-aware reading).
///
/// This is distinct from [RowGroupFilterEvent], which reports row groups dropped by
/// statistics predicate push-down. Byte-range selection keeps a row group when its
/// midpoint falls inside the requested split, and runs before push-down: the row groups
/// reported as kept here are the input to the push-down evaluation.
@Name("dev.hardwood.RowGroupByteRangeFilter")
@Label("Row Group Byte-Range Filter")
@Category({"Hardwood", "Filter"})
@Description("Row groups selected by a byte-range split predicate")
@StackTrace(false)
public class RowGroupByteRangeFilterEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

    @Label("Total Row Groups")
    @Description("Total number of row groups in the file before byte-range selection")
    public int totalRowGroups;

    @Label("Row Groups Kept")
    @Description("Number of row groups whose midpoint falls inside the requested split")
    public int rowGroupsKept;

    @Label("Row Groups Skipped")
    @Description("Number of row groups skipped because their midpoint falls outside the split")
    public int rowGroupsSkipped;
}
