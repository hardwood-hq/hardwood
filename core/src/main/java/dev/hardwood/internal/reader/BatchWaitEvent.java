/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * JFR event emitted when the consumer thread blocks waiting for a batch
 * to be assembled by the assembly thread.
 * <p>
 * This event indicates that the decode/assembly pipeline cannot keep up
 * with the consumer, causing the consumer to stall.
 * </p>
 */
@Name("dev.hardwood.BatchWait")
@Label("Batch Wait")
@Category({"Hardwood", "Pipeline"})
@Description("Consumer blocked waiting for the assembly pipeline to produce a batch")
@StackTrace(false)
public class BatchWaitEvent extends Event {

    @Label("Column")
    @Description("Name of the column being waited on")
    public String column;

    @Label("Wait Duration (ms)")
    @Description("Time spent waiting for the batch (milliseconds)")
    public long waitDurationMs;
}
