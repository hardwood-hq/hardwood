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

/// JFR event emitted when a row group's bloom filters prove that none of its rows matches the
/// filter, once the read reaches the row group.
///
/// [RowGroupFilterEvent] reports the decisions statistics take for all row groups of a file before
/// it is read, and counts a row group dropped here as kept.
@Name("dev.hardwood.RowGroupBloomFilter")
@Label("Row Group Bloom Filter")
@Category({"Hardwood", "Filter"})
@Description("Row group skipped by bloom filter predicate push-down")
@StackTrace(false)
public class RowGroupBloomFilterEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

    @Label("Row Group Index")
    @Description("Index of the skipped row group within the file")
    public int rowGroupIndex;
}
