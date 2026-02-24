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
 * JFR event emitted when a Parquet file is opened and its metadata is read.
 * <p>
 * This event spans the full open sequence: memory-mapping the file and
 * parsing the Parquet footer metadata. It is emitted for both single-file
 * reads ({@code ParquetFileReader.open()}) and multi-file reads
 * ({@code FileManager.mapAndReadMetadata()}).
 * </p>
 */
@Name("dev.hardwood.FileOpened")
@Label("File Opened")
@Category({"Hardwood", "I/O"})
@Description("Opening a Parquet file and reading its metadata")
@StackTrace(false)
public class FileOpenedEvent extends Event {

    @Label("File")
    @Description("Name of the Parquet file")
    public String file;

    @Label("File Size")
    @Description("Size of the file (bytes)")
    @DataAmount
    public long fileSize;

    @Label("Row Group Count")
    @Description("Number of row groups in the file")
    public int rowGroupCount;

    @Label("Column Count")
    @Description("Number of columns in the file schema")
    public int columnCount;
}
