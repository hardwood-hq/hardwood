/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

/// Combines the failures of one operation into the one it raises.
public final class Failures {

    private Failures() {
    }

    /// Adds `later` to `failure` as suppressed, unless it is `failure` itself: with the heap
    /// exhausted the JVM throws one preallocated `OutOfMemoryError` for separate allocations, and
    /// suppressing a throwable beneath itself throws an `IllegalArgumentException` in place of the
    /// failure.
    public static void addSuppressed(Throwable failure, Throwable later) {
        if (later != failure) {
            failure.addSuppressed(later);
        }
    }

    /// Returns `failure` with `later` suppressed beneath it as [#addSuppressed] adds it, or `later`
    /// where there is no `failure`.
    public static Throwable combine(Throwable failure, Throwable later) {
        if (failure == null) {
            return later;
        }
        addSuppressed(failure, later);
        return failure;
    }
}
