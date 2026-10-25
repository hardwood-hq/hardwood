/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro.runtime;

import java.util.List;

/// A state machine a [Driver] runs: facts in, commands out. It holds state and decides; it runs
/// nothing, and knows nothing of threads, locks or time. The driver calls it under one lock only.
///
/// @param <F> what can happen to the machine
/// @param <C> what the machine can ask for
public interface Machine<F, C> {

    /// Records `fact`, the result of a command [#poll] returned. Records only: what follows from
    /// it is decided by the next poll.
    void post(F fact);

    /// Decides what happens next, from the whole state, and returns it. Called after every post and
    /// every change a caller makes ([Driver#await]); returns nothing while nothing can start.
    List<C> poll();
}
