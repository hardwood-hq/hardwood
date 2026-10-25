/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.step;

/// What a walk or a planning stage needs and is not ready, which
/// [dev.hardwood.internal.reader.fetch.PageWalk#pending] returns: bytes not yet read
/// ([dev.hardwood.internal.reader.fetch.Section]), or a task of the read ending before a
/// dictionary page is parsed beside its other tasks
/// ([dev.hardwood.internal.reader.memory.WorkingSet#admit]). The read does not wait for it: it
/// registers its resume ([#whenMet]) and returns, and the resume runs the next step.
public interface Need {

    /// Runs `resume` once the need is met, on the thread that meets it, or at once on the caller's
    /// when it already is. A failure meets the need too: the next step finds it.
    void whenMet(Runnable resume);
}
