/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/// TEMPORARY CI diagnostic for close / end-of-stream latency. Records timestamped lines while the
/// `hardwood.debug.timing` system property is `true`; the tests print them.
public final class DebugTiming {

    public static final String PROPERTY = "hardwood.debug.timing";

    private static final ConcurrentLinkedQueue<String> LINES = new ConcurrentLinkedQueue<>();

    private DebugTiming() {
    }

    public static boolean enabled() {
        return Boolean.getBoolean(PROPERTY);
    }

    public static void log(String line) {
        LINES.add(String.format("%,.3f ms [%s] %s", System.nanoTime() / 1e6, Thread.currentThread(), line));
    }

    public static String ms(long nanos) {
        return String.format("%.3f", nanos / 1e6);
    }

    public static List<String> drain() {
        List<String> out = new ArrayList<>();
        String line;
        while ((line = LINES.poll()) != null) {
            out.add(line);
        }
        return out;
    }
}
