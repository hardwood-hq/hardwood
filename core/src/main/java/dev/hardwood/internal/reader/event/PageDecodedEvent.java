/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.event;

import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event emitted when a single Parquet data page is decoded.
 * <p>
 * This event covers the full page decode cycle: header parsing, decompression,
 * and value decoding. The compressed and uncompressed sizes can be used to
 * gauge compression ratios and decode cost per page.
 * </p>
 */
@Name("dev.hardwood.PageDecoded")
@Label("Page Decoded")
@Category({"Hardwood", "Decode"})
@Description("Decoding of a single Parquet data page")
@StackTrace(false)
public class PageDecodedEvent extends Event {

    @Label("Column")
    @Description("Name of the column being decoded")
    public String column;

    @Label("Compressed Size")
    @Description("Compressed size of the page data (bytes)")
    @DataAmount
    public int compressedSize;

    @Label("Uncompressed Size")
    @Description("Uncompressed size of the page data (bytes)")
    @DataAmount
    public int uncompressedSize;
}
