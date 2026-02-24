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
 * JFR event emitted when the prefetch queue cannot supply a decoded page in time.
 * <p>
 * A miss indicates that the decode pipeline cannot keep up with consumption,
 * causing the prefetch depth to increase adaptively.
 * </p>
 */
@Name("dev.hardwood.PrefetchMiss")
@Label("Prefetch Miss")
@Category({"Hardwood", "Pipeline"})
@Description("Prefetch queue miss requiring synchronous decode or blocking wait")
@StackTrace(false)
public class PrefetchMissEvent extends Event {

    @Label("File")
    @Description("Name of the current file being read")
    public String file;

    @Label("Column")
    @Description("Name of the column experiencing the miss")
    public String column;

    @Label("New Depth")
    @Description("Updated prefetch depth after the miss")
    public int newDepth;

    @Label("Queue Empty")
    @Description("True if the prefetch queue was completely empty")
    public boolean queueEmpty;
}
