/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

/// A bound on work in flight: the engine takes a slot as it emits the work and gives it back as
/// the work's result is applied.
final class Slots {

    private final int capacity;
    private int used;

    Slots(int capacity) {
        this.capacity = capacity;
    }

    boolean free() {
        return used < capacity;
    }

    void take() {
        if (!free()) {
            throw new IllegalStateException("no slot free of " + capacity);
        }
        used++;
    }

    void give() {
        if (used == 0) {
            throw new IllegalStateException("no slot taken");
        }
        used--;
    }

    @Override
    public String toString() {
        return used + "/" + capacity;
    }
}
