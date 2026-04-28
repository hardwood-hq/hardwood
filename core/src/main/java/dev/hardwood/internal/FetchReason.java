/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

/// Per-thread purpose tag attached to `InputFile.readRange` calls so that
/// fetch logs can attribute each network request to the caller (footer
/// body, row-group index region, column data fetch, prefetch, etc.).
///
/// Usage:
///
/// ```java
/// try (FetchReason.Scope ignored = FetchReason.set("rg=1 col=name")) {
///     buf = inputFile.readRange(offset, length);
/// }
/// ```
///
/// `S3InputFile.readRange` reads [#current()] when emitting its fetch log
/// line. Nesting is supported: the inner scope shadows the outer one and
/// restores it on close.
public final class FetchReason {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private FetchReason() {
    }

    /// Returns the active reason for the current thread, or `"unattributed"`
    /// if no scope is on the stack.
    public static String current() {
        String value = CURRENT.get();
        return value != null ? value : "unattributed";
    }

    /// Sets the reason for the current thread until [Scope#close] is called.
    /// Pass the returned [Scope] to a try-with-resources statement.
    public static Scope set(String reason) {
        String previous = CURRENT.get();
        CURRENT.set(reason);
        return new Scope(previous);
    }

    /// Captures the calling thread's current reason and returns a wrapped
    /// `Runnable` that re-establishes it on its executing thread. Use at
    /// any thread-handoff site (e.g. `CompletableFuture.runAsync`) where the
    /// calling thread's reason should follow the work item to the worker
    /// thread.
    ///
    /// If no reason is currently set, returns the original runnable unchanged
    /// — the worker thread keeps whatever reason (if any) it already has.
    public static Runnable bind(Runnable runnable) {
        String captured = CURRENT.get();
        if (captured == null) {
            return runnable;
        }
        return () -> {
            try (Scope ignored = set(captured)) {
                runnable.run();
            }
        };
    }

    public static final class Scope implements AutoCloseable {

        private final String previous;

        private Scope(String previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous == null) {
                CURRENT.remove();
            }
            else {
                CURRENT.set(previous);
            }
        }
    }
}
