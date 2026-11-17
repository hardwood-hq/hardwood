/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

/// A decode ahead of need learned that its page reconstructs more values than its claim and the
/// room beside the read's reserves hold: it gives up before allocating them, and its page starts
/// again with what it learned ([JobScheduler.Draw]). Never reaches the reader.
final class NoRoomException extends RuntimeException {

    private final long bytes;

    NoRoomException(long bytes) {
        super(null, null, false, false);
        this.bytes = bytes;
    }

    /// The bytes the decode would have drawn for its reconstructed values.
    long bytes() {
        return bytes;
    }
}
