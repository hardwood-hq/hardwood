/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// The failure of a rollback that would grow a staging array instead of shrinking it.
///
/// A staging array outside every LIST or MAP rolls back to the plan's record count, which holds
/// only while every committed record staged one entry in it. A record whose filler caught a
/// rejection of its own leaves the rejected field unstaged, and rolling such an array back would
/// publish a stale slot as data.
final class StagingRollback {

    private StagingRollback() {
    }

    static IllegalStateException shortOf(int mark, int count) {
        return new IllegalStateException("Cannot roll back to " + mark + " staged entries, only " + count
                + " are staged: a committed record left a field unstaged, as a filler that catches a rejection"
                + " of one of its own setters does");
    }
}
