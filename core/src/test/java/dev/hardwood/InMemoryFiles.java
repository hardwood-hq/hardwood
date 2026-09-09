/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;

/// Helpers for tests that hold a file written to [OutputFile#inMemory()] as a `byte[]`: to
/// corrupt, splice or compare it, or to hand it to an API that takes an array.
public final class InMemoryFiles {

    private InMemoryFiles() {
    }

    /// Returns a copy of the finished file.
    public static byte[] toByteArray(InMemoryOutputFile out) {
        ByteBuffer buffer = out.buffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
