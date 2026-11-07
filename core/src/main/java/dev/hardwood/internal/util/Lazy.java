/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.util;

import java.util.Objects;
import java.util.function.Supplier;

/// A value computed on first [#get()] and returned by every later one.
///
/// Holds a class's logger, so that a class which never logs never obtains one and never triggers
/// the JDK's `System.LoggerFinder` lookup:
///
/// ```java
/// private static final Lazy<System.Logger> LOG =
///         Lazy.of(() -> System.getLogger(FileMetadataCache.class.getName()));
///
/// LOG.get().log(Level.DEBUG, () -> "...");
/// ```
///
/// Log on the logger [#get()] returns, not through a wrapper, so that a backend which finds the
/// caller by walking the stack reports the logging class.
public final class Lazy<T> {

    private final Supplier<? extends T> supplier;
    private volatile T value;

    private Lazy(Supplier<? extends T> supplier) {
        this.supplier = supplier;
    }

    /// @param supplier computes the value on first [#get()]; must not return `null`
    public static <T> Lazy<T> of(Supplier<? extends T> supplier) {
        return new Lazy<>(Objects.requireNonNull(supplier, "supplier"));
    }

    /// The value, computed by the supplier on the first call. Concurrent first calls compute it
    /// once.
    ///
    /// @throws NullPointerException if the supplier returns `null`
    public T get() {
        T result = value;
        if (result != null) {
            return result;
        }
        synchronized (this) {
            if (value == null) {
                value = Objects.requireNonNull(supplier.get(), "supplier returned null");
            }
            return value;
        }
    }
}
