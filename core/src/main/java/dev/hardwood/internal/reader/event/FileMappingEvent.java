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
 * JFR event emitted when Hardwood memory-maps a region of a Parquet file.
 * <p>
 * This event tracks mmap operations which are not captured by the standard
 * {@code jdk.FileRead} event. Memory-mapped I/O loads data through page faults
 * rather than explicit read() calls.
 * </p>
 */
@Name("dev.hardwood.FileMapping")
@Label("File Mapping")
@Category({"Hardwood", "I/O"})
@Description("Memory-mapping of a file region for reading Parquet data")
@StackTrace(false)
public class FileMappingEvent extends Event {

    @Label("File")
    @Description("Name of the file being mapped")
    public String file;

    @Label("Offset")
    @Description("Starting offset in the file (bytes)")
    public long offset;

    @Label("Size")
    @Description("Size of the mapped region (bytes)")
    @DataAmount
    public long size;
}
