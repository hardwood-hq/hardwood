/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.event;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event emitted when all pages in a row group column chunk have been scanned.
 * <p>
 * Scanning reads page headers and parses dictionary pages upfront, producing
 * {@code PageInfo} records for on-demand decoding. A high page count relative
 * to the row group size may indicate small page sizes or high cardinality
 * dictionary encoding.
 * </p>
 */
@Name("dev.hardwood.RowGroupScanned")
@Label("Row Group Scanned")
@Category({"Hardwood", "Decode"})
@Description("Scanning of page boundaries in a row group column chunk")
@StackTrace(false)
public class RowGroupScannedEvent extends Event {

    @Label("File")
    @Description("Path to the Parquet file")
    public String file;

    @Label("Row Group Index")
    @Description("Index of the row group within the file")
    public int rowGroupIndex;

    @Label("Column")
    @Description("Name of the column being scanned")
    public String column;

    @Label("Page Count")
    @Description("Number of data pages found in this row group column chunk")
    public int pageCount;
}
