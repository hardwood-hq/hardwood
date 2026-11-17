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

/// JFR event declared for a prefetch queue that cannot supply a decoded page in time.
///
/// Hardwood does not emit this event: a read has no prefetch queue whose depth grows on a
/// miss. A consumer waiting for decoded data is reported by [BatchWaitEvent].
///
/// @deprecated Hardwood never emits this event; it will be removed in the next major release.
///             Use [BatchWaitEvent] to observe the consumer's stalls.
@Deprecated(since = "1.1", forRemoval = true)
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
