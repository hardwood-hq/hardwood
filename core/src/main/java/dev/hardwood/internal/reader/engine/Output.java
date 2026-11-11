/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;

/// Something a service waits for that ends once, with a value or a failure: a range's bytes
/// ([RangeFetch]), which pages, scans and the planner read, ended by the fetch's callbacks; a chunk's
/// dictionary's values ([ChunkDictionary]), which its data pages read, ended by the dictionary
/// page's decode; or a flat page's placement ([TrackedPage#placement]), ended by its column's
/// placement. It exists from when a service decides it needs it, so anything can wait for it from
/// then on ([#whenEnded]). A range a file holds in memory, or a dictionary the probe decoded, has
/// one ended from the start ([#of]). Used under the read's lock.
final class Output<T> {

    /// `null` until something waits
    private List<Runnable> waiters;
    private T value;
    private Throwable failure;
    private boolean ended;

    /// An output ended with `value` from the start.
    static <T> Output<T> of(T value) {
        Output<T> output = new Output<>();
        output.arrived(value);
        return output;
    }

    /// Whether it has ended, with a value or a failure.
    boolean ended() {
        return ended;
    }

    /// Whether it has ended with a value.
    boolean hasValue() {
        return ended && failure == null;
    }

    /// Its value, `null` until it has ended with one and once it is let go ([#drop]).
    T value() {
        return value;
    }

    /// Its failure, `null` unless it has ended with one.
    Throwable failure() {
        return failure;
    }

    /// `waiter` runs once it has ended, under the read's lock, at once where it has; it reads the
    /// value or the failure from the outputs it holds.
    void whenEnded(Runnable waiter) {
        if (ended) {
            waiter.run();
        }
        else {
            if (waiters == null) {
                waiters = new ArrayList<>(1);
            }
            waiters.add(waiter);
        }
    }

    /// It ends with `value`, waking its waiters.
    void arrived(T value) {
        end();
        this.value = value;
        wake();
    }

    /// It ends with `failure`, waking its waiters.
    void failed(Throwable failure) {
        end();
        this.failure = failure;
        wake();
    }

    /// Lets go of its value, which nothing reads any more.
    void drop() {
        value = null;
    }

    private void end() {
        if (ended) {
            throw new IllegalStateException("An output ended twice");
        }
        ended = true;
    }

    private void wake() {
        if (waiters == null) {
            return;
        }
        List<Runnable> woken = waiters;
        waiters = null;
        for (Runnable waiter : woken) {
            waiter.run();
        }
    }
}
