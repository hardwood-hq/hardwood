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

/// JFR event emitted when records are filtered by record-level predicate evaluation.
@Name("dev.hardwood.RecordFilter")
@Label("Record Filter")
@Category({"Hardwood", "Filter"})
@Description("Records filtered by record-level predicate evaluation")
@StackTrace(false)
public class RecordFilterEvent extends Event {

    @Label("Total Records")
    @Description("Total number of records evaluated against the predicate")
    public long totalRecords;

    @Label("Records Kept")
    @Description("Number of records that matched the predicate")
    public long recordsKept;

    @Label("Records Skipped")
    @Description("Number of records skipped by the predicate")
    public long recordsSkipped;
}
