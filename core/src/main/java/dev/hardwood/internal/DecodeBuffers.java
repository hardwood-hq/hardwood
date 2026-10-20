/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/// The buffers a decode task of a `HardwoodContext`'s pool reuses from the tasks before it: one
/// value per [Slot], which the code that owns the slot sizes and replaces.
///
/// A [DecodeExecutor] holds the sets and hands one to each task it runs on the pool, so decode
/// code, which takes a set as a parameter, runs on no other thread. A set is used by one task at a
/// time; a decode keeps no reference to its values past its call.
public final class DecodeBuffers {

    private static final AtomicInteger SLOTS = new AtomicInteger();

    private Object[] values = new Object[0];

    /// Created by the [DecodeExecutor] that holds them, and by tests.
    DecodeBuffers() {
    }

    /// One kind of buffer, declared once by the code that uses it.
    ///
    /// @param <T> the buffer's type
    public static final class Slot<T> {

        private final int index;

        private Slot(int index) {
            this.index = index;
        }
    }

    /// A new kind of buffer, one per set that a task sets it in.
    public static <T> Slot<T> slot() {
        return new Slot<>(SLOTS.getAndIncrement());
    }

    /// This set's buffer of `slot`'s kind, `null` until one is set.
    @SuppressWarnings("unchecked")
    public <T> T get(Slot<T> slot) {
        // Only set(Slot<T>, T) writes the element, so it holds a T
        return slot.index < values.length ? (T) values[slot.index] : null;
    }

    /// Replaces this set's buffer of `slot`'s kind.
    public <T> void set(Slot<T> slot, T value) {
        if (slot.index >= values.length) {
            values = Arrays.copyOf(values, Math.max(slot.index + 1, SLOTS.get()));
        }
        values[slot.index] = value;
    }
}
